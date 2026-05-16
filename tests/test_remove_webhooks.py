"""Unit tests for remove_webhooks.py.

Covers the remove-mode code that used to live in bulk_apply_webhooks.py before
the script was split. Also smoke-tests the helpers duplicated from
bulk_apply_webhooks.py (per the "no shared module" convention in CLAUDE.md) to
catch a regression where the duplicate drifts from the original.
"""

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
    Webhook,
    WebhookNotifications,
)

import remove_webhooks as rmw


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
# Duplicated helpers — smoke tests to catch drift from bulk_apply_webhooks.py
# --------------------------------------------------------------------------- #


class TestDuplicatedHelpers:
    """Light coverage of helpers duplicated from bulk_apply_webhooks.py.
    Full coverage of each lives in test_bulk_apply_webhooks.py; this is the
    canary that the duplicate keeps working as expected."""

    def test_is_transient_429(self):
        assert rmw.is_transient(Exception("HTTP 429")) is True

    def test_is_transient_permission_denied(self):
        assert rmw.is_transient(Exception("PERMISSION_DENIED")) is False

    def test_dig(self):
        assert rmw._dig({"a": {"b": 1}}, "a", "b") == 1
        assert rmw._dig({"a": {}}, "a", "b") is None

    def test_is_bundle_job_detects_BUNDLE_kind(self):
        job = _make_job(bundle=True, metadata_file_path="/m")
        assert rmw.is_bundle_job(job) == (True, "/m")
        assert rmw.is_bundle_job(_make_job()) == (False, None)

    def test_job_matches_owner(self):
        f = rmw.Filters(owners=["alice@x.com"])
        assert rmw.job_matches(_make_job(creator="alice@x.com"), f) is True
        assert rmw.job_matches(_make_job(creator="bob@x.com"), f) is False

    def test_fetch_bundle_metadata_cached(self):
        w = MagicMock()
        cache = {"/p": rmw.BundleMetadata(bundle_name="cached")}
        assert rmw.fetch_bundle_metadata(w, "/p", cache).bundle_name == "cached"
        w.workspace.download.assert_not_called()


# --------------------------------------------------------------------------- #
# Remove-specific pure functions
# --------------------------------------------------------------------------- #


class TestRemoveWebhooks:
    def test_remove_specific_destination_leaves_others(self):
        existing = WebhookNotifications(
            on_failure=[Webhook(id=WEBHOOK_ID), Webhook(id=OTHER_ID)],
        )
        result, count, ids, events = rmw.remove_webhooks(existing, WEBHOOK_ID)
        assert count == 1
        assert [w.id for w in result.on_failure] == [OTHER_ID]
        assert ids == [WEBHOOK_ID]
        assert events == ["on_failure"]

    def test_remove_all_clears_every_event_list(self):
        existing = WebhookNotifications(
            on_failure=[Webhook(id=WEBHOOK_ID)],
            on_success=[Webhook(id=OTHER_ID)],
        )
        result, count, ids, events = rmw.remove_webhooks(existing, None)
        assert count == 2
        assert result.on_failure == []
        assert result.on_success == []
        assert set(ids) == {WEBHOOK_ID, OTHER_ID}
        assert set(events) == {"on_failure", "on_success"}

    def test_no_op_when_target_not_attached(self):
        existing = WebhookNotifications(on_failure=[Webhook(id=OTHER_ID)])
        _, count, ids, events = rmw.remove_webhooks(existing, WEBHOOK_ID)
        assert count == 0
        assert ids == []
        assert events == []

    def test_no_existing_returns_empty(self):
        result, count, ids, events = rmw.remove_webhooks(None, WEBHOOK_ID)
        assert count == 0
        assert isinstance(result, WebhookNotifications)
        assert ids == []
        assert events == []

    def test_ids_removed_dedups_when_same_id_on_multiple_events(self):
        existing = WebhookNotifications(
            on_failure=[Webhook(id=WEBHOOK_ID)],
            on_success=[Webhook(id=WEBHOOK_ID)],
        )
        _, count, ids, events = rmw.remove_webhooks(existing, WEBHOOK_ID)
        assert count == 2
        assert ids == [WEBHOOK_ID]  # deduped across events
        assert set(events) == {"on_failure", "on_success"}


class TestWebhookAttached:
    def test_none_existing_false(self):
        assert rmw.webhook_attached(None, WEBHOOK_ID) is False

    def test_attached_on_one_event(self):
        existing = WebhookNotifications(on_failure=[Webhook(id=WEBHOOK_ID)])
        assert rmw.webhook_attached(existing, WEBHOOK_ID) is True

    def test_attached_on_another_event(self):
        existing = WebhookNotifications(on_success=[Webhook(id=WEBHOOK_ID)])
        assert rmw.webhook_attached(existing, WEBHOOK_ID) is True

    def test_only_other_webhooks_present(self):
        existing = WebhookNotifications(on_failure=[Webhook(id=OTHER_ID)])
        assert rmw.webhook_attached(existing, WEBHOOK_ID) is False


class TestLoadJobIdsFromFile:
    def test_plain_text_one_id_per_line(self, tmp_path):
        path = tmp_path / "ids.txt"
        path.write_text("1\n2\n3\n")
        assert rmw.load_job_ids_from_file(str(path)) == [1, 2, 3]

    def test_csv_with_inventory_header_row(self, tmp_path):
        path = tmp_path / "inv.csv"
        path.write_text(
            "job_id,name,creator,deployment_kind\n"
            "100,alpha,a@x.com,DIRECT\n"
            "200,beta,b@x.com,BUNDLE\n"
        )
        assert rmw.load_job_ids_from_file(str(path)) == [100, 200]

    def test_csv_without_header(self, tmp_path):
        path = tmp_path / "no-header.csv"
        path.write_text("42,alpha\n99,beta\n")
        assert rmw.load_job_ids_from_file(str(path)) == [42, 99]

    def test_blank_lines_and_whitespace_tolerated(self, tmp_path):
        path = tmp_path / "messy.txt"
        path.write_text("\n  1  \n\n2\n  \n3\n")
        assert rmw.load_job_ids_from_file(str(path)) == [1, 2, 3]

    def test_empty_file_raises(self, tmp_path):
        path = tmp_path / "empty.txt"
        path.write_text("")
        with pytest.raises(SystemExit, match="No job IDs found"):
            rmw.load_job_ids_from_file(str(path))

    def test_non_numeric_mid_file_raises(self, tmp_path):
        path = tmp_path / "bad.csv"
        path.write_text("100\nnot-a-number\n")
        with pytest.raises(SystemExit, match="Bad job ID"):
            rmw.load_job_ids_from_file(str(path))


# --------------------------------------------------------------------------- #
# Per-job remove path
# --------------------------------------------------------------------------- #


class TestProcessRemoveJobDAB:
    """Per-job remove against a DAB job: must proceed but emit a WARNING (non-durable)."""

    def test_warns_on_bundle_job_when_removing(self, caplog):
        w = MagicMock()
        bundle_job = _make_job(
            bundle=True,
            metadata_file_path="/meta",
            webhooks=WebhookNotifications(on_failure=[Webhook(id=WEBHOOK_ID)]),
        )
        w.jobs.get.return_value = bundle_job
        stats = rmw.Stats()
        with caplog.at_level(logging.WARNING):
            rmw.process_remove_job(w, 100, WEBHOOK_ID, apply=False, max_retries=0, stats=stats)
        assert any("bundle-managed" in r.message for r in caplog.records)
        assert stats.would_update == 1

    def test_no_warning_for_non_bundle_job(self, caplog):
        w = MagicMock()
        w.jobs.get.return_value = _make_job(
            webhooks=WebhookNotifications(on_failure=[Webhook(id=WEBHOOK_ID)]),
        )
        stats = rmw.Stats()
        with caplog.at_level(logging.WARNING):
            rmw.process_remove_job(w, 100, WEBHOOK_ID, apply=False, max_retries=0, stats=stats)
        assert not any("bundle-managed" in r.message for r in caplog.records)


# --------------------------------------------------------------------------- #
# Workspace-walk rollback path
# --------------------------------------------------------------------------- #


def _walk_args(**overrides):
    """Build a Namespace shaped like parse_args output for workspace-walk remove."""
    defaults = dict(
        webhook_id=WEBHOOK_ID,
        job_id=[],
        job_ids_from=None,
        tag=None,
        owner=[],
        bundle_jobs="skip",
        bundle_report="",
        apply=False,
        profile=None,
        max_retries=0,
        base_sleep=0,
        jitter=0,
        limit=None,
        progress_every=0,
        verbose=False,
    )
    defaults.update(overrides)
    import argparse as _argparse
    return _argparse.Namespace(**defaults)


class TestRemoveWalkMode:
    """Workspace-walk rollback: walk jobs, filter, pre-check `webhook_attached`,
    and call jobs.update only on jobs that currently have the destination."""

    def test_removes_only_matching_jobs(self):
        # Three jobs: one attached, one with a different webhook, one bare.
        jobs = [
            _make_job(job_id=1, name="has-it",
                      webhooks=WebhookNotifications(on_failure=[Webhook(id=WEBHOOK_ID)])),
            _make_job(job_id=2, name="has-other",
                      webhooks=WebhookNotifications(on_failure=[Webhook(id=OTHER_ID)])),
            _make_job(job_id=3, name="bare"),
        ]
        w = MagicMock()
        w.jobs.list.return_value = iter(jobs)
        stats = rmw.Stats()
        rc = rmw.run_remove_walk_mode(_walk_args(apply=True), w, stats)
        assert rc == 0
        # Only job 1 should be updated.
        assert w.jobs.update.call_count == 1
        kwargs = w.jobs.update.call_args.kwargs
        assert kwargs["job_id"] == 1
        new_wh = kwargs["new_settings"].webhook_notifications
        # WEBHOOK_ID stripped from on_failure (sent as []).
        assert new_wh.on_failure == []
        assert stats.updated == 1
        assert stats.already_attached == 2  # the other two short-circuited

    def test_dry_run_does_not_update(self):
        jobs = [
            _make_job(job_id=1, webhooks=WebhookNotifications(on_failure=[Webhook(id=WEBHOOK_ID)])),
        ]
        w = MagicMock()
        w.jobs.list.return_value = iter(jobs)
        stats = rmw.Stats()
        rmw.run_remove_walk_mode(_walk_args(apply=False), w, stats)
        w.jobs.update.assert_not_called()
        assert stats.would_update == 1

    def test_owner_filter_applied(self):
        jobs = [
            _make_job(job_id=1, creator="alice@x.com",
                      webhooks=WebhookNotifications(on_failure=[Webhook(id=WEBHOOK_ID)])),
            _make_job(job_id=2, creator="bob@x.com",
                      webhooks=WebhookNotifications(on_failure=[Webhook(id=WEBHOOK_ID)])),
        ]
        w = MagicMock()
        w.jobs.list.return_value = iter(jobs)
        stats = rmw.Stats()
        rmw.run_remove_walk_mode(_walk_args(apply=True, owner=["alice@x.com"]), w, stats)
        # Only alice's job touched.
        assert w.jobs.update.call_count == 1
        assert w.jobs.update.call_args.kwargs["job_id"] == 1

    def test_bundle_skip_default(self, caplog):
        jobs = [
            _make_job(job_id=1, name="bundle-job",
                      bundle=True, metadata_file_path="/m",
                      webhooks=WebhookNotifications(on_failure=[Webhook(id=WEBHOOK_ID)])),
        ]
        w = MagicMock()
        w.jobs.list.return_value = iter(jobs)
        stats = rmw.Stats()
        with caplog.at_level(logging.INFO):
            rmw.run_remove_walk_mode(_walk_args(apply=True), w, stats)
        w.jobs.update.assert_not_called()
        assert stats.bundle_skipped == 1
        assert any("SKIP bundle-managed" in r.message for r in caplog.records)

    def test_bundle_include_proceeds_with_warning(self, caplog):
        jobs = [
            _make_job(job_id=1, name="bundle-job",
                      bundle=True, metadata_file_path="/m",
                      webhooks=WebhookNotifications(on_failure=[Webhook(id=WEBHOOK_ID)])),
        ]
        w = MagicMock()
        w.jobs.list.return_value = iter(jobs)
        stats = rmw.Stats()
        with caplog.at_level(logging.WARNING):
            rmw.run_remove_walk_mode(
                _walk_args(apply=True, bundle_jobs="include"), w, stats,
            )
        assert w.jobs.update.call_count == 1
        assert any("bundle-managed" in r.message for r in caplog.records)

    def test_limit_short_circuits(self):
        jobs = [
            _make_job(job_id=i, webhooks=WebhookNotifications(on_failure=[Webhook(id=WEBHOOK_ID)]))
            for i in range(1, 6)
        ]
        w = MagicMock()
        w.jobs.list.return_value = iter(jobs)
        stats = rmw.Stats()
        rmw.run_remove_walk_mode(_walk_args(apply=True, limit=2), w, stats)
        assert w.jobs.update.call_count == 2

    def test_parse_args_requires_webhook_id_in_walk_mode(self, monkeypatch):
        monkeypatch.setattr("sys.argv", ["remove_webhooks.py"])
        with pytest.raises(SystemExit):
            rmw.parse_args()

    def test_parse_args_rejects_filters_with_explicit_job_id(self, monkeypatch):
        monkeypatch.setattr(
            "sys.argv",
            [
                "remove_webhooks.py",
                "--webhook-id", WEBHOOK_ID,
                "--job-id", "1",
                "--tag", "team=x",
            ],
        )
        with pytest.raises(SystemExit):
            rmw.parse_args()


class TestResolveRemoveJobIds:
    """Merging --job-id and --job-ids-from preserves order and de-duplicates."""

    def test_merges_explicit_and_file(self, tmp_path):
        path = tmp_path / "ids.txt"
        path.write_text("3\n4\n5\n")
        args = _walk_args(job_id=[1, 2], job_ids_from=str(path))
        assert rmw._resolve_remove_job_ids(args) == [1, 2, 3, 4, 5]

    def test_dedupes_preserving_first_occurrence(self, tmp_path):
        path = tmp_path / "ids.txt"
        path.write_text("2\n3\n")
        args = _walk_args(job_id=[1, 2], job_ids_from=str(path))
        assert rmw._resolve_remove_job_ids(args) == [1, 2, 3]


# --------------------------------------------------------------------------- #
# run(**kwargs) contract (the surface notebook widgets map to)
# --------------------------------------------------------------------------- #


class TestRunCallable:
    """Lock the run(**kwargs) contract. CLI's post-parse cross-flag guards
    must also fire under run() — a notebook caller should get the same
    SystemExit a CLI caller does."""

    def test_run_walk_requires_webhook_id(self):
        with pytest.raises(SystemExit, match="REQUIRES --webhook-id"):
            rmw.run()

    def test_run_rejects_filters_with_explicit_jobs(self):
        with pytest.raises(SystemExit, match="don't combine"):
            rmw.run(webhook_id=WEBHOOK_ID, job_id=[1], tag="team=x")

    def test_run_per_job_kwargs_drive_remove(self, monkeypatch):
        w = MagicMock()
        w.jobs.get.return_value = _make_job(
            job_id=42,
            webhooks=WebhookNotifications(on_failure=[Webhook(id=WEBHOOK_ID)]),
        )
        w.config.host = "https://test"
        monkeypatch.setattr(rmw, "build_client", lambda profile: w)
        rc = rmw.run(
            webhook_id=WEBHOOK_ID,
            job_id=[42],
            apply=False,
            bundle_report="",
            progress_every=0,
        )
        assert rc == 0


class TestRemovalLogDelta:
    """Audit log: one Delta row per successful jobs.update. Apply-only;
    dry-runs produce no rows."""

    def _fake_spark(self):
        """Mock that captures createDataFrame rows and the saveAsTable target."""
        spark = MagicMock()
        captured = {"rows": None, "table": None, "mode": None, "partitions": None}

        def _create_df(rows):
            captured["rows"] = rows
            df = MagicMock()
            writer = MagicMock()
            df.write = writer
            writer.format.return_value = writer
            writer.mode.side_effect = lambda m: (captured.__setitem__("mode", m) or writer)
            writer.option.return_value = writer
            writer.partitionBy.side_effect = lambda *cols: (captured.__setitem__("partitions", cols) or writer)
            writer.saveAsTable.side_effect = lambda t: captured.__setitem__("table", t)
            return df

        spark.createDataFrame.side_effect = _create_df
        return spark, captured

    def test_walk_writes_one_row_per_apply(self, monkeypatch):
        jobs = [
            _make_job(job_id=1, name="direct-job",
                      webhooks=WebhookNotifications(on_failure=[Webhook(id=WEBHOOK_ID)])),
            _make_job(job_id=2, name="bundle-job",
                      bundle=True, metadata_file_path="/meta",
                      webhooks=WebhookNotifications(on_failure=[Webhook(id=WEBHOOK_ID)])),
        ]
        w = MagicMock()
        w.jobs.list.return_value = iter(jobs)
        w.config.host = "https://test"
        spark, captured = self._fake_spark()
        # Stub bundle metadata fetch so the bundle row gets enriched.
        monkeypatch.setattr(
            rmw, "fetch_bundle_metadata",
            lambda w, p, c: rmw.BundleMetadata(bundle_name="my-bundle", target="prod",
                                                git_branch="main"),
        )
        rc = rmw.run(
            client=w,
            webhook_id=WEBHOOK_ID,
            apply=True,
            bundle_jobs="include",
            bundle_report="",
            progress_every=0,
            delta_table="cat.schema.log_webhook_removals",
            spark=spark,
        )
        assert rc == 0
        assert captured["table"] == "cat.schema.log_webhook_removals"
        assert captured["mode"] == "append"
        assert captured["partitions"] == ("workspace_host",)
        rows = captured["rows"]
        assert len(rows) == 2
        by_id = {r["job_id"]: r for r in rows}
        # Direct job: is_bundle=False, no bundle metadata.
        assert by_id[1]["is_bundle"] is False
        assert by_id[1]["bundle_name"] == ""
        assert by_id[1]["webhook_ids_removed"] == [WEBHOOK_ID]
        assert by_id[1]["events_affected"] == ["on_failure"]
        # Bundle job: is_bundle=True, metadata populated.
        assert by_id[2]["is_bundle"] is True
        assert by_id[2]["bundle_name"] == "my-bundle"
        assert by_id[2]["target"] == "prod"
        assert by_id[2]["git_branch"] == "main"
        assert by_id[2]["metadata_file_path"] == "/meta"

    def test_walk_dry_run_skips_delta_write(self, capfd):
        """Dry-run + delta_table set must NOT produce any Spark writes.
        Uses capfd because `rmw.run()` calls basicConfig(force=True) which
        evicts caplog's handler — stderr capture survives it."""
        jobs = [_make_job(job_id=1,
                          webhooks=WebhookNotifications(on_failure=[Webhook(id=WEBHOOK_ID)]))]
        w = MagicMock()
        w.jobs.list.return_value = iter(jobs)
        w.config.host = "https://test"
        spark, captured = self._fake_spark()
        rmw.run(
            client=w,
            webhook_id=WEBHOOK_ID,
            apply=False,
            bundle_report="",
            progress_every=0,
            delta_table="cat.schema.log_webhook_removals",
            spark=spark,
        )
        spark.createDataFrame.assert_not_called()
        assert captured["table"] is None
        assert "Dry-run: skipping write to Delta" in capfd.readouterr().err

    def test_walk_errored_update_excluded_from_log(self, monkeypatch):
        """A failed jobs.update must NOT produce an audit row — the log only
        reflects deletions that actually happened."""
        jobs = [_make_job(job_id=1,
                          webhooks=WebhookNotifications(on_failure=[Webhook(id=WEBHOOK_ID)]))]
        w = MagicMock()
        w.jobs.list.return_value = iter(jobs)
        w.jobs.update.side_effect = Exception("boom")
        w.config.host = "https://test"
        spark, captured = self._fake_spark()
        rmw.run(
            client=w,
            webhook_id=WEBHOOK_ID,
            apply=True,
            bundle_report="",
            progress_every=0,
            delta_table="cat.schema.log_webhook_removals",
            spark=spark,
        )
        # Empty records list -> write_removal_log_delta short-circuits before createDataFrame.
        spark.createDataFrame.assert_not_called()

    def test_per_job_mode_also_writes_log(self):
        w = MagicMock()
        w.jobs.get.return_value = _make_job(
            job_id=42, name="explicit-target",
            webhooks=WebhookNotifications(on_failure=[Webhook(id=WEBHOOK_ID)]),
        )
        w.config.host = "https://test"
        spark, captured = self._fake_spark()
        rc = rmw.run(
            client=w,
            webhook_id=WEBHOOK_ID,
            job_id=[42],
            apply=True,
            bundle_report="",
            progress_every=0,
            delta_table="cat.schema.log_webhook_removals",
            spark=spark,
        )
        assert rc == 0
        assert captured["table"] == "cat.schema.log_webhook_removals"
        assert len(captured["rows"]) == 1
        assert captured["rows"][0]["job_id"] == 42
        assert captured["rows"][0]["is_bundle"] is False


class TestRunNotebookKwargs:
    """Lock the contract for the notebook-only kwargs that the multi-workspace
    dispatcher relies on: `client`, `scan_limit`."""

    def test_client_kwarg_bypasses_build_client(self, monkeypatch):
        """When `client` is passed, build_client must not be called. This is
        how the multi-workspace dispatcher injects SP-authed clients."""
        w = MagicMock()
        w.jobs.list.return_value = iter([])
        w.config.host = "https://injected"

        def _should_not_be_called(profile):
            raise AssertionError("build_client must not be invoked when client= is provided")
        monkeypatch.setattr(rmw, "build_client", _should_not_be_called)

        rc = rmw.run(client=w, webhook_id=WEBHOOK_ID, bundle_report="", progress_every=0)
        assert rc == 0

    def test_scan_limit_short_circuits_walk(self, monkeypatch):
        """scan_limit caps the walk regardless of matches."""
        consumed = []
        def gen():
            for i in range(20):
                consumed.append(i)
                yield _make_job(job_id=i, creator="nobody@x.com")
        w = MagicMock()
        w.jobs.list.return_value = gen()
        w.config.host = "https://test"
        monkeypatch.setattr(rmw, "build_client", lambda profile: w)

        rmw.run(
            webhook_id=WEBHOOK_ID,
            owner=["alice@x.com"],  # no jobs match
            scan_limit=5,
            bundle_report="",
            progress_every=0,
        )
        assert len(consumed) == 5, f"scan_limit=5 must stop at 5 jobs consumed, got {len(consumed)}"
