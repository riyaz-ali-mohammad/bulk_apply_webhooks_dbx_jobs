"""Unit tests for inventory_jobs.py.

The script duplicates `is_bundle_job` / `fetch_bundle_metadata` / `job_matches`
from bulk_apply_webhooks.py on purpose (per CLAUDE.md, the three scripts are
intentionally standalone). These tests cover the duplicated logic and the
inventory-specific bits: classification, summary aggregation, and CSV schema.
"""

import csv
import io
import json
import logging
from unittest.mock import MagicMock

import pytest
from databricks.sdk.service.jobs import (
    BaseJob,
    JobDeployment,
    JobDeploymentKind,
    JobSettings,
)

import inventory_jobs as inv


def _make_job(
    *,
    job_id=100,
    name="test-job",
    creator="user@example.com",
    tags=None,
    bundle=False,
    metadata_file_path=None,
):
    settings = JobSettings(name=name, tags=tags or {})
    if bundle:
        settings.deployment = JobDeployment(
            kind=JobDeploymentKind.BUNDLE,
            metadata_file_path=metadata_file_path,
        )
    return BaseJob(job_id=job_id, creator_user_name=creator, settings=settings)


class TestIsBundleJob:
    def test_direct_job(self):
        is_bundle, meta = inv.is_bundle_job(_make_job())
        assert (is_bundle, meta) == (False, None)

    def test_bundle_job(self):
        job = _make_job(bundle=True, metadata_file_path="/Workspace/x/state/metadata.json")
        is_bundle, meta = inv.is_bundle_job(job)
        assert is_bundle is True
        assert meta == "/Workspace/x/state/metadata.json"

    def test_job_without_settings(self):
        assert inv.is_bundle_job(BaseJob(job_id=1)) == (False, None)


class TestJobMatches:
    def test_no_filters_match(self):
        assert inv.job_matches(_make_job(), inv.Filters()) is True

    def test_owner_match(self):
        f = inv.Filters(owners=["alice@example.com"])
        assert inv.job_matches(_make_job(creator="alice@example.com"), f) is True
        assert inv.job_matches(_make_job(creator="bob@example.com"), f) is False

    def test_tag_presence_only(self):
        f = inv.Filters(tag_key="team")
        assert inv.job_matches(_make_job(tags={"team": "platform"}), f) is True
        assert inv.job_matches(_make_job(tags={"other": "x"}), f) is False

    def test_tag_key_value(self):
        f = inv.Filters(tag_key="team", tag_value="platform")
        assert inv.job_matches(_make_job(tags={"team": "platform"}), f) is True
        assert inv.job_matches(_make_job(tags={"team": "other"}), f) is False


class TestFetchBundleMetadata:
    def _mock_workspace_download(self, w, payload):
        cm = MagicMock()
        cm.__enter__.return_value = io.BytesIO(json.dumps(payload).encode())
        cm.__exit__.return_value = None
        w.workspace.download.return_value = cm

    def test_parses_config_wrapped_payload(self):
        w = MagicMock()
        self._mock_workspace_download(w, {
            "config": {
                "bundle": {
                    "name": "my-bundle",
                    "target": "prod",
                    "git": {"origin_url": "https://g", "branch": "main", "commit": "abc"},
                },
                "workspace": {"root_path": "/Workspace/x", "file_path": "/Workspace/x/f"},
            }
        })
        result = inv.fetch_bundle_metadata(w, "/path", {})
        assert result.bundle_name == "my-bundle"
        assert result.target == "prod"
        assert result.git_origin == "https://g"

    def test_cache_hit_skips_download(self):
        w = MagicMock()
        cache = {"/p": inv.BundleMetadata(bundle_name="cached")}
        result = inv.fetch_bundle_metadata(w, "/p", cache)
        assert result.bundle_name == "cached"
        w.workspace.download.assert_not_called()

    def test_none_path_returns_none(self):
        assert inv.fetch_bundle_metadata(MagicMock(), None, {}) is None

    def test_download_failure_returns_none_and_caches(self):
        w = MagicMock()
        w.workspace.download.side_effect = Exception("ACL denied")
        cache = {}
        assert inv.fetch_bundle_metadata(w, "/p", cache) is None
        assert cache["/p"] is None


class TestWriteInventory:
    def test_csv_schema_and_blank_bundle_cells_for_direct(self, tmp_path):
        records = [
            inv.JobRecord(
                job_id=1, name="direct-job", creator="a@x.com",
                deployment_kind="DIRECT",
            ),
            inv.JobRecord(
                job_id=2, name="bundle-job", creator="b@x.com",
                deployment_kind="BUNDLE",
                metadata_file_path="/meta",
                metadata=inv.BundleMetadata(
                    bundle_name="b1", target="prod",
                    git_origin="https://g", git_branch="main", git_commit="abc",
                    workspace_root="/w", workspace_file_path="/w/f",
                ),
            ),
        ]
        path = tmp_path / "inv.csv"
        inv.write_inventory(str(path), records)
        rows = list(csv.reader(path.read_text().splitlines()))
        assert rows[0] == inv.CSV_COLUMNS
        direct_row = dict(zip(rows[0], rows[1]))
        bundle_row = dict(zip(rows[0], rows[2]))
        assert direct_row["deployment_kind"] == "DIRECT"
        assert direct_row["bundle_name"] == ""
        assert direct_row["git_origin"] == ""
        assert direct_row["metadata_file_path"] == ""
        assert bundle_row["deployment_kind"] == "BUNDLE"
        assert bundle_row["bundle_name"] == "b1"
        assert bundle_row["target"] == "prod"
        assert bundle_row["metadata_file_path"] == "/meta"

    def test_no_records_no_file_created(self, tmp_path):
        path = tmp_path / "nothing.csv"
        inv.write_inventory(str(path), [])
        assert not path.exists()

    def test_blank_path_is_noop(self):
        inv.write_inventory("", [inv.JobRecord(1, "x", None, "DIRECT")])


class TestMainEndToEnd:
    """Drive `main()` with a mocked WorkspaceClient. Verifies the loop counts
    correctly, classifies, and writes a CSV with both kinds of rows."""

    def _mock_metadata_download(self, w, payload):
        cm = MagicMock()
        cm.__enter__.return_value = io.BytesIO(json.dumps(payload).encode())
        cm.__exit__.return_value = None
        w.workspace.download.return_value = cm

    def test_classifies_and_writes_csv(self, tmp_path, monkeypatch, capsys):
        jobs = [
            _make_job(job_id=1, name="d1", creator="alice@x.com"),
            _make_job(job_id=2, name="d2", creator="alice@x.com"),
            _make_job(job_id=3, name="b1", creator="bob@x.com",
                      bundle=True, metadata_file_path="/m/b1.json"),
            _make_job(job_id=4, name="b2", creator="bob@x.com",
                      bundle=True, metadata_file_path="/m/b1.json"),
        ]
        w = MagicMock()
        w.jobs.list.return_value = iter(jobs)
        w.config.host = "https://test.cloud.databricks.com"
        self._mock_metadata_download(w, {
            "config": {"bundle": {"name": "the-bundle", "target": "prod"}}
        })

        monkeypatch.setattr(inv, "build_client", lambda profile: w)
        out_path = tmp_path / "inv.csv"
        monkeypatch.setattr(
            "sys.argv",
            ["inventory_jobs.py", "--output", str(out_path), "--enrich-bundles"],
        )

        assert inv.main() == 0

        rows = list(csv.reader(out_path.read_text().splitlines()))
        assert rows[0] == inv.CSV_COLUMNS
        kinds = [row[3] for row in rows[1:]]
        assert kinds.count("DIRECT") == 2
        assert kinds.count("BUNDLE") == 2

        out = capsys.readouterr().out
        assert "total:           4" in out
        assert "DAB-deployed:    2" in out
        assert "directly-deployed: 2" in out
        assert "the-bundle" in out  # top-N bundle breakdown
        assert "alice@x.com" in out  # top-N creators

    def test_no_enrich_skips_workspace_download(self, tmp_path, monkeypatch, capsys):
        w = MagicMock()
        w.jobs.list.return_value = iter([
            _make_job(job_id=10, bundle=True, metadata_file_path="/m/x.json"),
        ])
        w.config.host = "https://test"
        monkeypatch.setattr(inv, "build_client", lambda profile: w)
        monkeypatch.setattr(
            "sys.argv",
            ["inventory_jobs.py", "--output", str(tmp_path / "inv.csv")],
        )
        assert inv.main() == 0
        w.workspace.download.assert_not_called()
        out = capsys.readouterr().out
        assert "Bundle name breakdown requires --enrich-bundles" in out

    def test_filters_applied(self, tmp_path, monkeypatch, capsys):
        jobs = [
            _make_job(job_id=1, creator="alice@x.com"),
            _make_job(job_id=2, creator="bob@x.com"),
            _make_job(job_id=3, creator="carol@x.com"),
        ]
        w = MagicMock()
        w.jobs.list.return_value = iter(jobs)
        w.config.host = "https://test"
        monkeypatch.setattr(inv, "build_client", lambda profile: w)
        monkeypatch.setattr(
            "sys.argv",
            [
                "inventory_jobs.py",
                "--output", str(tmp_path / "inv.csv"),
                "--owner", "alice@x.com",
                "--owner", "bob@x.com",
            ],
        )
        assert inv.main() == 0
        out = capsys.readouterr().out
        assert "total:           2" in out  # carol filtered out

    def test_disable_csv_with_empty_output(self, tmp_path, monkeypatch):
        w = MagicMock()
        w.jobs.list.return_value = iter([_make_job(job_id=1)])
        w.config.host = "https://test"
        monkeypatch.setattr(inv, "build_client", lambda profile: w)
        out_path = tmp_path / "should-not-exist.csv"
        monkeypatch.setattr(
            "sys.argv",
            ["inventory_jobs.py", "--output", ""],
        )
        # Change cwd so a stray default-path write would still be visible.
        monkeypatch.chdir(tmp_path)
        assert inv.main() == 0
        assert not out_path.exists()


class TestRunCallable:
    """Lock the run(**kwargs) contract that notebook widgets map to. If a
    notebook calls inv.run(...) with the documented kwargs, the script must
    behave the same as the equivalent CLI invocation."""

    def test_run_kwargs_match_cli_behaviour(self, tmp_path, monkeypatch, capsys):
        jobs = [
            _make_job(job_id=1, creator="alice@x.com"),
            _make_job(job_id=2, creator="bob@x.com"),
        ]
        w = MagicMock()
        w.jobs.list.return_value = iter(jobs)
        w.config.host = "https://test"
        monkeypatch.setattr(inv, "build_client", lambda profile: w)

        out_path = tmp_path / "inv.csv"
        rc = inv.run(
            profile=None,
            tag=None,
            owner=["alice@x.com"],
            output=str(out_path),
            enrich_bundles=False,
            top_n=0,
            progress_every=0,
            verbose=False,
        )
        assert rc == 0
        rows = list(csv.reader(out_path.read_text().splitlines()))
        assert len(rows) == 2  # header + alice only
        out = capsys.readouterr().out
        assert "total:           1" in out


class TestRunNotebookKwargs:
    """The notebook-only kwargs (`client`, `scan_limit`, `name_filter`) are the
    contract the multi-workspace dispatcher in notebooks/_auth.py depends on.
    These tests prove the script honors them; the notebook layer is then a
    thin wrapper around this behaviour."""

    def test_client_kwarg_bypasses_build_client(self, tmp_path, monkeypatch, capsys):
        """When `client` is passed, run() must NOT call build_client(). This
        is how the multi-workspace dispatcher injects a per-workspace SP-authed
        client; if build_client got called instead, every iteration would
        re-resolve the wrong identity."""
        w = MagicMock()
        w.jobs.list.return_value = iter([_make_job(job_id=1)])
        w.config.host = "https://injected"

        def _should_not_be_called(profile):
            raise AssertionError("build_client must not be invoked when client= is provided")
        monkeypatch.setattr(inv, "build_client", _should_not_be_called)

        rc = inv.run(client=w, output="", top_n=0, progress_every=0)
        assert rc == 0

    def test_scan_limit_stops_walk_before_full_iteration(self, monkeypatch):
        """scan_limit caps stats.scanned regardless of matches. Distinct from
        the CLI's --limit (a mutation cap, not applicable to read-only inventory).
        Use a generator to prove the loop short-circuits before consuming all items."""
        consumed = []
        def gen():
            for i in range(10):
                consumed.append(i)
                yield _make_job(job_id=i, creator="nobody@x.com")
        w = MagicMock()
        w.jobs.list.return_value = gen()
        w.config.host = "https://test"
        monkeypatch.setattr(inv, "build_client", lambda profile: w)

        inv.run(owner=["alice@x.com"], scan_limit=3, output="", top_n=0, progress_every=0)
        assert len(consumed) == 3, f"scan_limit=3 should stop at 3 jobs consumed, got {len(consumed)}"

    def test_name_filter_passed_to_jobs_list(self, monkeypatch):
        """name_filter must be forwarded to w.jobs.list(name=...) — that's
        the only server-side filter the Jobs API supports."""
        w = MagicMock()
        w.jobs.list.return_value = iter([])
        w.config.host = "https://test"
        monkeypatch.setattr(inv, "build_client", lambda profile: w)

        inv.run(name_filter="etl-prefix", output="", top_n=0, progress_every=0)
        # Verify list() was called with name= kwarg
        w.jobs.list.assert_called_with(expand_tasks=False, name="etl-prefix")

    def test_no_name_filter_omits_name_kwarg(self, monkeypatch):
        """When name_filter is None, the `name` kwarg must NOT be passed to
        jobs.list() — the SDK treats `name=None` differently from absent."""
        w = MagicMock()
        w.jobs.list.return_value = iter([])
        w.config.host = "https://test"
        monkeypatch.setattr(inv, "build_client", lambda profile: w)

        inv.run(output="", top_n=0, progress_every=0)
        w.jobs.list.assert_called_with(expand_tasks=False)
