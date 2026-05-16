"""Unit tests for apply_webhooks_to_direct_jobs.py.

The script attaches a webhook to non-DAB jobs only. DAB-managed (bundle) jobs
are always skipped — there is no `--bundle-jobs` flag, no bundle inventory
output. The rollback / detach path lives in remove_webhooks.py with its own
test file at tests/test_remove_webhooks.py.
"""

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

import apply_webhooks_to_direct_jobs as apply_direct


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
# Pure helpers
# --------------------------------------------------------------------------- #


class TestParseEvents:
    def test_valid(self):
        assert apply_direct.parse_events("on_start,on_failure") == ["on_start", "on_failure"]

    def test_strips_whitespace(self):
        assert apply_direct.parse_events(" on_start , on_failure ") == ["on_start", "on_failure"]

    def test_unknown_event_raises(self):
        with pytest.raises(SystemExit, match="Unknown event"):
            apply_direct.parse_events("on_start,on_bogus")


class TestIsTransient:
    def test_rate_limit_message(self):
        assert apply_direct.is_transient(Exception("RATE_LIMIT_EXCEEDED")) is True

    def test_429_message(self):
        assert apply_direct.is_transient(Exception("HTTP 429 Too Many Requests")) is True

    def test_5xx_error_code(self):
        err = Exception("server hiccup")
        err.error_code = "500"
        assert apply_direct.is_transient(err) is True

    def test_internal_message(self):
        assert apply_direct.is_transient(Exception("INTERNAL_ERROR")) is True

    def test_permission_denied_is_not_transient(self):
        assert apply_direct.is_transient(Exception("PERMISSION_DENIED")) is False


class TestJobMatches:
    def test_no_filters_match(self):
        assert apply_direct.job_matches(_make_job(), apply_direct.Filters()) is True

    def test_owner_match(self):
        f = apply_direct.Filters(owners=["alice@example.com"])
        assert apply_direct.job_matches(_make_job(creator="alice@example.com"), f) is True
        assert apply_direct.job_matches(_make_job(creator="bob@example.com"), f) is False

    def test_tag_presence_only(self):
        f = apply_direct.Filters(tag_key="team")
        assert apply_direct.job_matches(_make_job(tags={"team": "platform"}), f) is True
        assert apply_direct.job_matches(_make_job(tags={"other": "x"}), f) is False

    def test_tag_key_value(self):
        f = apply_direct.Filters(tag_key="team", tag_value="platform")
        assert apply_direct.job_matches(_make_job(tags={"team": "platform"}), f) is True
        assert apply_direct.job_matches(_make_job(tags={"team": "other"}), f) is False


class TestIsBundleJob:
    """is_bundle_job is the detection that drives the always-skip path."""

    def test_non_dab_job(self):
        is_bundle, meta = apply_direct.is_bundle_job(_make_job())
        assert (is_bundle, meta) == (False, None)

    def test_dab_job(self):
        job = _make_job(bundle=True, metadata_file_path="/Workspace/x/state/metadata.json")
        is_bundle, meta = apply_direct.is_bundle_job(job)
        assert is_bundle is True
        assert meta == "/Workspace/x/state/metadata.json"

    def test_job_without_settings(self):
        assert apply_direct.is_bundle_job(BaseJob(job_id=1)) == (False, None)


# --------------------------------------------------------------------------- #
# Non-DAB jobs: attach / process_job
# --------------------------------------------------------------------------- #


class TestAlreadyAttached:
    def test_no_existing_webhooks(self):
        assert apply_direct.already_attached(None, WEBHOOK_ID, ["on_failure"]) is False

    def test_attached_on_every_target_event(self):
        existing = WebhookNotifications(
            on_failure=[Webhook(id=WEBHOOK_ID)],
            on_success=[Webhook(id=WEBHOOK_ID)],
        )
        assert apply_direct.already_attached(existing, WEBHOOK_ID, ["on_failure", "on_success"]) is True

    def test_partial_coverage_returns_false(self):
        existing = WebhookNotifications(on_failure=[Webhook(id=WEBHOOK_ID)])
        assert apply_direct.already_attached(existing, WEBHOOK_ID, ["on_failure", "on_success"]) is False


class TestMergeWebhooks:
    def test_adds_to_empty(self):
        result = apply_direct.merge_webhooks(None, WEBHOOK_ID, ["on_failure"])
        assert [w.id for w in result.on_failure] == [WEBHOOK_ID]

    def test_preserves_other_destinations_on_same_event(self):
        existing = WebhookNotifications(on_failure=[Webhook(id=OTHER_ID)])
        result = apply_direct.merge_webhooks(existing, WEBHOOK_ID, ["on_failure"])
        ids = [w.id for w in result.on_failure]
        assert ids == [OTHER_ID, WEBHOOK_ID]

    def test_idempotent_when_already_present(self):
        existing = WebhookNotifications(on_failure=[Webhook(id=WEBHOOK_ID)])
        result = apply_direct.merge_webhooks(existing, WEBHOOK_ID, ["on_failure"])
        assert [w.id for w in result.on_failure] == [WEBHOOK_ID]

    def test_preserves_unrelated_event_lists(self):
        existing = WebhookNotifications(on_start=[Webhook(id="start-only")])
        result = apply_direct.merge_webhooks(existing, WEBHOOK_ID, ["on_failure"])
        assert [w.id for w in result.on_start] == ["start-only"]
        assert [w.id for w in result.on_failure] == [WEBHOOK_ID]


class TestProcessJobNonDAB:
    def test_dry_run_does_not_call_api(self):
        w = MagicMock()
        job = _make_job(webhooks=None)
        stats = apply_direct.Stats()
        apply_direct.process_job(w, job, WEBHOOK_ID, ["on_failure"], apply=False, max_retries=0, stats=stats)
        assert stats.would_update == 1
        assert stats.updated == 0
        w.jobs.update.assert_not_called()

    def test_apply_calls_jobs_update(self):
        w = MagicMock()
        job = _make_job(webhooks=None)
        stats = apply_direct.Stats()
        apply_direct.process_job(w, job, WEBHOOK_ID, ["on_failure"], apply=True, max_retries=0, stats=stats)
        assert stats.updated == 1
        w.jobs.update.assert_called_once()
        kwargs = w.jobs.update.call_args.kwargs
        assert kwargs["job_id"] == job.job_id
        new_wh = kwargs["new_settings"].webhook_notifications
        assert [x.id for x in new_wh.on_failure] == [WEBHOOK_ID]

    def test_already_attached_short_circuits(self):
        w = MagicMock()
        job = _make_job(webhooks=WebhookNotifications(on_failure=[Webhook(id=WEBHOOK_ID)]))
        stats = apply_direct.Stats()
        apply_direct.process_job(w, job, WEBHOOK_ID, ["on_failure"], apply=True, max_retries=0, stats=stats)
        assert stats.already_attached == 1
        assert stats.updated == 0
        w.jobs.update.assert_not_called()

    def test_records_error_on_failure(self):
        w = MagicMock()
        w.jobs.update.side_effect = Exception("PERMISSION_DENIED")
        job = _make_job()
        stats = apply_direct.Stats()
        apply_direct.process_job(w, job, WEBHOOK_ID, ["on_failure"], apply=True, max_retries=0, stats=stats)
        assert stats.errored == 1
        assert stats.updated == 0


# --------------------------------------------------------------------------- #
# Always-skip bundle jobs (no escape hatch — was --bundle-jobs include/only)
# --------------------------------------------------------------------------- #


class TestBundleJobsAlwaysSkipped:
    """DAB-managed jobs must always be skipped. There is no flag to override."""

    def test_single_bundle_job_is_skipped(self, monkeypatch):
        w = MagicMock()
        w.jobs.list.return_value = iter([
            _make_job(job_id=1, bundle=True, metadata_file_path="/m"),
        ])
        w.config.host = "https://test"
        monkeypatch.setattr(apply_direct, "build_client", lambda profile: w)

        rc = apply_direct.run(webhook_id=WEBHOOK_ID, apply=True, progress_every=0)
        assert rc == 0
        w.jobs.update.assert_not_called()

    def test_mixed_walk_updates_only_direct(self, monkeypatch):
        # 1 bundle + 1 direct → only the direct should be updated.
        w = MagicMock()
        w.jobs.list.return_value = iter([
            _make_job(job_id=1, name="bundle-one", bundle=True, metadata_file_path="/m"),
            _make_job(job_id=2, name="direct-one"),
        ])
        w.config.host = "https://test"
        monkeypatch.setattr(apply_direct, "build_client", lambda profile: w)

        rc = apply_direct.run(webhook_id=WEBHOOK_ID, apply=True, progress_every=0)
        assert rc == 0
        assert w.jobs.update.call_count == 1
        assert w.jobs.update.call_args.kwargs["job_id"] == 2

    def test_limit_counts_only_direct_mutations(self, monkeypatch):
        # limit=1 + (1 bundle + 2 direct) should mutate exactly 1 (the first direct).
        # Bundle skip does not consume the mutation cap.
        w = MagicMock()
        w.jobs.list.return_value = iter([
            _make_job(job_id=1, name="bundle-one", bundle=True, metadata_file_path="/m"),
            _make_job(job_id=2, name="direct-one"),
            _make_job(job_id=3, name="direct-two"),
        ])
        w.config.host = "https://test"
        monkeypatch.setattr(apply_direct, "build_client", lambda profile: w)

        rc = apply_direct.run(webhook_id=WEBHOOK_ID, apply=True, limit=1, progress_every=0)
        assert rc == 0
        # Only direct-one (job_id=2) should have been mutated; loop stops at limit before direct-two.
        assert w.jobs.update.call_count == 1
        assert w.jobs.update.call_args.kwargs["job_id"] == 2


# --------------------------------------------------------------------------- #
# run(**kwargs) contract (the surface notebook widgets map to)
# --------------------------------------------------------------------------- #


class TestRunCallable:
    """Lock the run(**kwargs) contract that notebook widgets map to."""

    def test_run_requires_webhook_id(self):
        with pytest.raises(SystemExit, match="webhook_id is required"):
            apply_direct.run(webhook_id=None)

    def test_run_kwargs_drive_add_path(self, monkeypatch):
        w = MagicMock()
        w.jobs.list.return_value = iter([_make_job(job_id=1, creator="alice@x.com")])
        w.config.host = "https://test"
        monkeypatch.setattr(apply_direct, "build_client", lambda profile: w)
        rc = apply_direct.run(
            webhook_id=WEBHOOK_ID,
            owner=["alice@x.com"],
            apply=False,
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
        monkeypatch.setattr(apply_direct, "build_client", _should_not_be_called)

        rc = apply_direct.run(client=w, webhook_id=WEBHOOK_ID, progress_every=0)
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
        monkeypatch.setattr(apply_direct, "build_client", lambda profile: w)

        apply_direct.run(
            webhook_id=WEBHOOK_ID,
            owner=["alice@x.com"],  # no jobs match
            scan_limit=5,
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
        monkeypatch.setattr(apply_direct, "build_client", lambda profile: w)

        apply_direct.run(webhook_id=WEBHOOK_ID, name_filter="etl-", progress_every=0)
        w.jobs.list.assert_called_with(expand_tasks=False, name="etl-")
