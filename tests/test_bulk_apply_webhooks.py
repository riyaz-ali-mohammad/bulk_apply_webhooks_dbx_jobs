"""Unit tests for bulk_apply_webhooks.py (add path only).

The rollback / detach path lives in remove_webhooks.py with its own test file
at tests/test_remove_webhooks.py.

Two top-level groupings:
  * Non-DAB jobs — the happy-path bulk attach logic.
  * DAB-deployed jobs — detection, bundle-metadata fetch, CSV inventory.
"""

import csv
import io
import json
from unittest.mock import MagicMock

import pytest
from databricks.sdk.service.jobs import (
    BaseJob,
    JobDeployment,
    JobDeploymentKind,
    JobSettings,
    Webhook,
    WebhookNotifications,
)

import bulk_apply_webhooks as bulk


WEBHOOK_ID = "abc-123"
OTHER_ID = "other-456"


def _make_job(
    *,
    job_id=100,
    name="test-job",
    creator="user@example.com",
    tags=None,
    webhooks=None,
    bundle=False,
    metadata_file_path=None,
):
    """Build a BaseJob. bundle=True makes it look DAB-deployed."""
    settings = JobSettings(
        name=name,
        tags=tags or {},
        webhook_notifications=webhooks,
    )
    if bundle:
        settings.deployment = JobDeployment(
            kind=JobDeploymentKind.BUNDLE,
            metadata_file_path=metadata_file_path,
        )
    return BaseJob(job_id=job_id, creator_user_name=creator, settings=settings)


# --------------------------------------------------------------------------- #
# Shared pure functions
# --------------------------------------------------------------------------- #


class TestParseEvents:
    def test_valid(self):
        assert bulk.parse_events("on_start,on_failure") == ["on_start", "on_failure"]

    def test_strips_whitespace(self):
        assert bulk.parse_events(" on_start , on_failure ") == ["on_start", "on_failure"]

    def test_unknown_event_raises(self):
        with pytest.raises(SystemExit, match="Unknown event"):
            bulk.parse_events("on_start,on_bogus")


class TestDig:
    def test_returns_nested(self):
        assert bulk._dig({"a": {"b": {"c": 42}}}, "a", "b", "c") == 42

    def test_missing_key_returns_none(self):
        assert bulk._dig({"a": {}}, "a", "b") is None

    def test_non_dict_intermediate_returns_none(self):
        assert bulk._dig({"a": "string"}, "a", "b") is None


class TestIsTransient:
    def test_rate_limit_message(self):
        assert bulk.is_transient(Exception("RATE_LIMIT_EXCEEDED")) is True

    def test_429_message(self):
        assert bulk.is_transient(Exception("HTTP 429 Too Many Requests")) is True

    def test_5xx_error_code(self):
        err = Exception("server hiccup")
        err.error_code = "500"
        assert bulk.is_transient(err) is True

    def test_internal_message(self):
        assert bulk.is_transient(Exception("INTERNAL_ERROR")) is True

    def test_permission_denied_is_not_transient(self):
        assert bulk.is_transient(Exception("PERMISSION_DENIED")) is False


class TestJobMatches:
    def test_no_filters_match(self):
        assert bulk.job_matches(_make_job(), bulk.Filters()) is True

    def test_owner_match(self):
        f = bulk.Filters(owners=["alice@example.com"])
        assert bulk.job_matches(_make_job(creator="alice@example.com"), f) is True
        assert bulk.job_matches(_make_job(creator="bob@example.com"), f) is False

    def test_tag_presence_only(self):
        f = bulk.Filters(tag_key="team")
        assert bulk.job_matches(_make_job(tags={"team": "platform"}), f) is True
        assert bulk.job_matches(_make_job(tags={"other": "x"}), f) is False

    def test_tag_key_value(self):
        f = bulk.Filters(tag_key="team", tag_value="platform")
        assert bulk.job_matches(_make_job(tags={"team": "platform"}), f) is True
        assert bulk.job_matches(_make_job(tags={"team": "other"}), f) is False


# --------------------------------------------------------------------------- #
# Non-DAB jobs: attach / process_job
# --------------------------------------------------------------------------- #


class TestAlreadyAttached:
    def test_no_existing_webhooks(self):
        assert bulk.already_attached(None, WEBHOOK_ID, ["on_failure"]) is False

    def test_attached_on_every_target_event(self):
        existing = WebhookNotifications(
            on_failure=[Webhook(id=WEBHOOK_ID)],
            on_success=[Webhook(id=WEBHOOK_ID)],
        )
        assert bulk.already_attached(existing, WEBHOOK_ID, ["on_failure", "on_success"]) is True

    def test_partial_coverage_returns_false(self):
        existing = WebhookNotifications(on_failure=[Webhook(id=WEBHOOK_ID)])
        assert bulk.already_attached(existing, WEBHOOK_ID, ["on_failure", "on_success"]) is False


class TestMergeWebhooks:
    def test_adds_to_empty(self):
        result = bulk.merge_webhooks(None, WEBHOOK_ID, ["on_failure"])
        assert [w.id for w in result.on_failure] == [WEBHOOK_ID]

    def test_preserves_other_destinations_on_same_event(self):
        existing = WebhookNotifications(on_failure=[Webhook(id=OTHER_ID)])
        result = bulk.merge_webhooks(existing, WEBHOOK_ID, ["on_failure"])
        ids = [w.id for w in result.on_failure]
        assert ids == [OTHER_ID, WEBHOOK_ID]

    def test_idempotent_when_already_present(self):
        existing = WebhookNotifications(on_failure=[Webhook(id=WEBHOOK_ID)])
        result = bulk.merge_webhooks(existing, WEBHOOK_ID, ["on_failure"])
        assert [w.id for w in result.on_failure] == [WEBHOOK_ID]

    def test_preserves_unrelated_event_lists(self):
        existing = WebhookNotifications(on_start=[Webhook(id="start-only")])
        result = bulk.merge_webhooks(existing, WEBHOOK_ID, ["on_failure"])
        assert [w.id for w in result.on_start] == ["start-only"]
        assert [w.id for w in result.on_failure] == [WEBHOOK_ID]


class TestProcessJobNonDAB:
    def test_dry_run_does_not_call_api(self):
        w = MagicMock()
        job = _make_job(webhooks=None)
        stats = bulk.Stats()
        bulk.process_job(w, job, WEBHOOK_ID, ["on_failure"], apply=False, max_retries=0, stats=stats)
        assert stats.would_update == 1
        assert stats.updated == 0
        w.jobs.update.assert_not_called()

    def test_apply_calls_jobs_update(self):
        w = MagicMock()
        job = _make_job(webhooks=None)
        stats = bulk.Stats()
        bulk.process_job(w, job, WEBHOOK_ID, ["on_failure"], apply=True, max_retries=0, stats=stats)
        assert stats.updated == 1
        w.jobs.update.assert_called_once()
        kwargs = w.jobs.update.call_args.kwargs
        assert kwargs["job_id"] == job.job_id
        new_wh = kwargs["new_settings"].webhook_notifications
        assert [x.id for x in new_wh.on_failure] == [WEBHOOK_ID]

    def test_already_attached_short_circuits(self):
        w = MagicMock()
        job = _make_job(webhooks=WebhookNotifications(on_failure=[Webhook(id=WEBHOOK_ID)]))
        stats = bulk.Stats()
        bulk.process_job(w, job, WEBHOOK_ID, ["on_failure"], apply=True, max_retries=0, stats=stats)
        assert stats.already_attached == 1
        assert stats.updated == 0
        w.jobs.update.assert_not_called()

    def test_records_error_on_failure(self):
        w = MagicMock()
        w.jobs.update.side_effect = Exception("PERMISSION_DENIED")
        job = _make_job()
        stats = bulk.Stats()
        bulk.process_job(w, job, WEBHOOK_ID, ["on_failure"], apply=True, max_retries=0, stats=stats)
        assert stats.errored == 1
        assert stats.updated == 0


# --------------------------------------------------------------------------- #
# DAB-deployed jobs: detection, metadata, CSV inventory
# --------------------------------------------------------------------------- #


class TestIsBundleJob:
    def test_non_dab_job(self):
        is_bundle, meta = bulk.is_bundle_job(_make_job())
        assert (is_bundle, meta) == (False, None)

    def test_dab_job(self):
        job = _make_job(bundle=True, metadata_file_path="/Workspace/x/state/metadata.json")
        is_bundle, meta = bulk.is_bundle_job(job)
        assert is_bundle is True
        assert meta == "/Workspace/x/state/metadata.json"

    def test_job_without_settings(self):
        assert bulk.is_bundle_job(BaseJob(job_id=1)) == (False, None)


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
                    "git": {
                        "origin_url": "https://github.com/x/y",
                        "branch": "main",
                        "commit": "abc123",
                    },
                },
                "workspace": {"root_path": "/Workspace/x", "file_path": "/Workspace/x/f"},
            }
        })
        result = bulk.fetch_bundle_metadata(w, "/path", {})
        assert result.bundle_name == "my-bundle"
        assert result.target == "prod"
        assert result.git_origin == "https://github.com/x/y"
        assert result.git_branch == "main"
        assert result.git_commit == "abc123"
        assert result.workspace_root == "/Workspace/x"

    def test_parses_pascal_case_git_fields(self):
        w = MagicMock()
        self._mock_workspace_download(w, {
            "config": {"bundle": {"git": {"OriginURL": "u", "Branch": "b", "Commit": "c"}}}
        })
        result = bulk.fetch_bundle_metadata(w, "/path", {})
        assert (result.git_origin, result.git_branch, result.git_commit) == ("u", "b", "c")

    def test_cache_hit_skips_download(self):
        w = MagicMock()
        cache = {"/path": bulk.BundleMetadata(bundle_name="cached")}
        result = bulk.fetch_bundle_metadata(w, "/path", cache)
        assert result.bundle_name == "cached"
        w.workspace.download.assert_not_called()

    def test_none_path_returns_none(self):
        assert bulk.fetch_bundle_metadata(MagicMock(), None, {}) is None

    def test_download_failure_returns_none_and_caches(self):
        w = MagicMock()
        w.workspace.download.side_effect = Exception("ACL denied")
        cache = {}
        assert bulk.fetch_bundle_metadata(w, "/path", cache) is None
        assert cache["/path"] is None


class TestWriteBundleReport:
    def test_writes_expected_columns(self, tmp_path):
        records = [
            bulk.BundleJobRecord(
                job_id=42,
                name="job-a",
                metadata_file_path="/meta/path",
                creator="alice@x.com",
                metadata=bulk.BundleMetadata(
                    bundle_name="b1", target="prod",
                    git_origin="https://g", git_branch="main", git_commit="abc",
                    workspace_root="/w", workspace_file_path="/w/f",
                ),
            ),
        ]
        path = tmp_path / "report.csv"
        bulk.write_bundle_report(str(path), records)
        rows = list(csv.reader(path.read_text().splitlines()))
        header = rows[0]
        assert header[:5] == ["job_id", "name", "creator", "bundle_name", "target"]
        assert "git_origin" in header and "metadata_file_path" in header
        assert rows[1][0] == "42"
        assert "b1" in rows[1] and "prod" in rows[1]

    def test_no_records_no_file_created(self, tmp_path):
        path = tmp_path / "nothing.csv"
        bulk.write_bundle_report(str(path), [])
        assert not path.exists()

    def test_blank_path_is_noop(self):
        bulk.write_bundle_report("", [bulk.BundleJobRecord(1, "x", None, None)])


# --------------------------------------------------------------------------- #
# run(**kwargs) contract (the surface notebook widgets map to)
# --------------------------------------------------------------------------- #


class TestRunCallable:
    """Lock the run(**kwargs) contract that notebook widgets map to."""

    def test_run_requires_webhook_id(self):
        with pytest.raises(SystemExit, match="webhook_id is required"):
            bulk.run(webhook_id=None)

    def test_run_kwargs_drive_add_path(self, monkeypatch):
        w = MagicMock()
        w.jobs.list.return_value = iter([_make_job(job_id=1, creator="alice@x.com")])
        w.config.host = "https://test"
        monkeypatch.setattr(bulk, "build_client", lambda profile: w)
        rc = bulk.run(
            webhook_id=WEBHOOK_ID,
            owner=["alice@x.com"],
            apply=False,
            bundle_report="",
            progress_every=0,
        )
        assert rc == 0


class TestRunNotebookKwargs:
    """Lock the contract for the notebook-only kwargs that the multi-workspace
    dispatcher relies on: `client`, `scan_limit`, `name_filter`."""

    def test_client_kwarg_bypasses_build_client(self, monkeypatch):
        """When `client` is passed, build_client must not be called. This is
        how the multi-workspace dispatcher injects SP-authed clients."""
        w = MagicMock()
        w.jobs.list.return_value = iter([])
        w.config.host = "https://injected"

        def _should_not_be_called(profile):
            raise AssertionError("build_client must not be invoked when client= is provided")
        monkeypatch.setattr(bulk, "build_client", _should_not_be_called)

        rc = bulk.run(client=w, webhook_id=WEBHOOK_ID, bundle_report="", progress_every=0)
        assert rc == 0

    def test_scan_limit_short_circuits_walk(self, monkeypatch):
        """scan_limit caps the walk regardless of matches. The transcript
        feedback (Riyaz @ 44:03, Santosh @ 41:08) calls out that --limit alone
        doesn't shorten the scan when matches are sparse; scan_limit fixes that."""
        consumed = []
        def gen():
            for i in range(20):
                consumed.append(i)
                yield _make_job(job_id=i, creator="nobody@x.com")
        w = MagicMock()
        w.jobs.list.return_value = gen()
        w.config.host = "https://test"
        monkeypatch.setattr(bulk, "build_client", lambda profile: w)

        bulk.run(
            webhook_id=WEBHOOK_ID,
            owner=["alice@x.com"],  # no jobs match
            scan_limit=5,
            bundle_report="",
            progress_every=0,
        )
        assert len(consumed) == 5, f"scan_limit=5 must stop at 5 jobs consumed, got {len(consumed)}"

    def test_name_filter_forwarded_to_jobs_list(self, monkeypatch):
        """name_filter → w.jobs.list(name=...) — the only filter the Jobs API
        supports server-side, so this is the only way to cut the scan size
        before iteration."""
        w = MagicMock()
        w.jobs.list.return_value = iter([])
        w.config.host = "https://test"
        monkeypatch.setattr(bulk, "build_client", lambda profile: w)

        bulk.run(webhook_id=WEBHOOK_ID, name_filter="etl-", bundle_report="", progress_every=0)
        w.jobs.list.assert_called_with(expand_tasks=False, name="etl-")
