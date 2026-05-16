"""Unit tests for patch_bundle_yaml.py.

Two top-level groupings:
  * Non-DAB-style patching — base `resources.jobs.<name>` blocks, where the
    patcher behaves like a straightforward YAML editor.
  * DAB-specific caveats — per-target overrides and `${var.*}` references,
    where the patcher must NOT write (to avoid `Duplicate webhook ids` at
    `bundle deploy`).

The integration test at the bottom drives `main()` against the
`examples/caveats/` bundle, which deliberately exercises all three caveats.
"""

import io
import shutil
from pathlib import Path

import pytest

import patch_bundle_yaml as patcher


WEBHOOK_ID = "wid-literal-12345"
REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture
def yaml():
    return patcher.make_yaml()


def _load(yaml, src: str):
    return yaml.load(io.StringIO(src))


def _dump(yaml, doc) -> str:
    buf = io.StringIO()
    yaml.dump(doc, buf)
    return buf.getvalue()


# --------------------------------------------------------------------------- #
# Shared pure functions
# --------------------------------------------------------------------------- #


class TestParseEvents:
    def test_valid(self):
        assert patcher.parse_events("on_failure,on_start") == ["on_failure", "on_start"]

    def test_unknown_raises(self):
        with pytest.raises(SystemExit):
            patcher.parse_events("on_bogus")


class TestParseTagFilter:
    def test_key_value(self):
        assert patcher.parse_tag_filter("team=platform") == ("team", "platform")

    def test_key_only(self):
        assert patcher.parse_tag_filter("team") == ("team", None)

    def test_none(self):
        assert patcher.parse_tag_filter(None) is None


class TestJobMatchesYAML:
    def test_name_filter(self, yaml):
        node = _load(yaml, "name: my-job\n")
        assert patcher.job_matches(node, ["my-job"], None) is True
        assert patcher.job_matches(node, ["other"], None) is False

    def test_tag_filter(self, yaml):
        node = _load(yaml, """
name: j
tags:
  team: platform
""")
        assert patcher.job_matches(node, [], ("team", "platform")) is True
        assert patcher.job_matches(node, [], ("team", "other")) is False
        assert patcher.job_matches(node, [], ("missing", None)) is False

    def test_owner_filter_matches_is_owner_user(self, yaml):
        node = _load(yaml, """
name: j
permissions:
  - level: IS_OWNER
    user_name: alice@x.com
""")
        assert patcher.job_matches(node, [], None, ["alice@x.com"]) is True
        assert patcher.job_matches(node, [], None, ["bob@x.com"]) is False

    def test_owner_filter_matches_can_manage_group(self, yaml):
        node = _load(yaml, """
name: j
permissions:
  - level: CAN_MANAGE
    group_name: data-platform
""")
        assert patcher.job_matches(node, [], None, ["data-platform"]) is True
        assert patcher.job_matches(node, [], None, ["analytics"]) is False

    def test_owner_filter_matches_can_manage_service_principal(self, yaml):
        node = _load(yaml, """
name: j
permissions:
  - level: CAN_MANAGE
    service_principal_name: deploy-sp
""")
        assert patcher.job_matches(node, [], None, ["deploy-sp"]) is True

    def test_owner_filter_ignores_can_view_and_can_manage_run(self, yaml):
        """CAN_VIEW / CAN_MANAGE_RUN are runtime perms, not ownership — must NOT match."""
        node = _load(yaml, """
name: j
permissions:
  - level: CAN_VIEW
    user_name: alice@x.com
  - level: CAN_MANAGE_RUN
    user_name: bob@x.com
""")
        assert patcher.job_matches(node, [], None, ["alice@x.com"]) is False
        assert patcher.job_matches(node, [], None, ["bob@x.com"]) is False

    def test_owner_filter_no_permissions_block(self, yaml):
        node = _load(yaml, "name: j\n")
        assert patcher.job_matches(node, [], None, ["alice@x.com"]) is False

    def test_owner_filter_multiple_owners_or_semantics(self, yaml):
        node = _load(yaml, """
name: j
permissions:
  - level: IS_OWNER
    user_name: alice@x.com
""")
        # Any of the listed owners matches → pass.
        assert patcher.job_matches(node, [], None, ["bob@x.com", "alice@x.com"]) is True
        assert patcher.job_matches(node, [], None, ["bob@x.com", "carol@x.com"]) is False

    def test_owner_combines_with_other_filters_via_and(self, yaml):
        """All supplied filters must match (AND across filter types)."""
        node = _load(yaml, """
name: alpha
tags:
  team: platform
permissions:
  - level: IS_OWNER
    user_name: alice@x.com
""")
        assert patcher.job_matches(node, ["alpha"], ("team", "platform"), ["alice@x.com"]) is True
        # name mismatch -> False even though owner+tag match
        assert patcher.job_matches(node, ["beta"], ("team", "platform"), ["alice@x.com"]) is False
        # owner mismatch -> False even though name+tag match
        assert patcher.job_matches(node, ["alpha"], ("team", "platform"), ["bob@x.com"]) is False

    def test_owner_empty_list_is_no_op(self, yaml):
        """Empty/None owner filter should not exclude any job."""
        node = _load(yaml, "name: j\n")
        assert patcher.job_matches(node, [], None, []) is True
        assert patcher.job_matches(node, [], None, None) is True


# --------------------------------------------------------------------------- #
# Non-DAB-style patching: base `resources.jobs.<name>` blocks
# --------------------------------------------------------------------------- #


BASE_ONLY = """
resources:
  jobs:
    base_job:
      name: base
      tasks: []
"""


class TestPatchWebhooksBaseJob:
    def test_adds_block_when_absent(self, yaml):
        doc = _load(yaml, BASE_ONLY)
        _, node = next(iter(patcher.find_job_nodes(doc)))
        changed, skipped = patcher.patch_webhooks(node, WEBHOOK_ID, ["on_failure"])
        assert changed is True
        assert skipped == []
        assert node["webhook_notifications"]["on_failure"][0]["id"] == WEBHOOK_ID

    def test_inserts_block_before_tasks(self, yaml):
        doc = _load(yaml, BASE_ONLY)
        _, node = next(iter(patcher.find_job_nodes(doc)))
        patcher.patch_webhooks(node, WEBHOOK_ID, ["on_failure"])
        keys = list(node.keys())
        assert keys.index("webhook_notifications") < keys.index("tasks")

    def test_appends_to_existing_list(self, yaml):
        doc = _load(yaml, """
resources:
  jobs:
    j:
      name: j
      webhook_notifications:
        on_failure:
          - id: existing-id
      tasks: []
""")
        _, node = next(iter(patcher.find_job_nodes(doc)))
        patcher.patch_webhooks(node, WEBHOOK_ID, ["on_failure"])
        ids = [x["id"] for x in node["webhook_notifications"]["on_failure"]]
        assert ids == ["existing-id", WEBHOOK_ID]

    def test_idempotent(self, yaml):
        src = f"""
resources:
  jobs:
    j:
      name: j
      webhook_notifications:
        on_failure:
          - id: {WEBHOOK_ID}
      tasks: []
"""
        doc = _load(yaml, src)
        _, node = next(iter(patcher.find_job_nodes(doc)))
        changed, _ = patcher.patch_webhooks(node, WEBHOOK_ID, ["on_failure"])
        assert changed is False

    def test_round_trip_preserves_comments(self, yaml):
        doc = _load(yaml, """
resources:
  jobs:
    j:
      name: j
      # important comment that must survive
      tasks: []
""")
        _, node = next(iter(patcher.find_job_nodes(doc)))
        patcher.patch_webhooks(node, WEBHOOK_ID, ["on_failure"])
        assert "# important comment that must survive" in _dump(yaml, doc)


# --------------------------------------------------------------------------- #
# DAB-specific caveats: per-target overrides and ${var.*} references
# --------------------------------------------------------------------------- #


class TestHasVariableRef:
    def test_detects_variable(self):
        assert patcher._has_variable_ref([{"id": "${var.webhook_id}"}]) is True

    def test_literal_only(self):
        assert patcher._has_variable_ref([{"id": "abc-123"}]) is False

    def test_empty(self):
        assert patcher._has_variable_ref([]) is False

    def test_mixed(self):
        assert patcher._has_variable_ref([{"id": "abc"}, {"id": "${var.x}"}]) is True


class TestPatchWebhooksDABCaveats:
    """Skip-with-no-write rules that exist purely to avoid `Duplicate webhook
    ids` rejections at `bundle deploy`."""

    def test_skips_event_list_with_var_ref(self, yaml):
        doc = _load(yaml, """
resources:
  jobs:
    j:
      name: j
      webhook_notifications:
        on_failure:
          - id: ${var.webhook_id}
      tasks: []
""")
        _, node = next(iter(patcher.find_job_nodes(doc)))
        changed, skipped = patcher.patch_webhooks(node, WEBHOOK_ID, ["on_failure"])
        assert changed is False
        assert skipped == ["on_failure"]
        assert [x["id"] for x in node["webhook_notifications"]["on_failure"]] == [
            "${var.webhook_id}"
        ]

    def test_var_ref_only_skips_affected_event(self, yaml):
        doc = _load(yaml, """
resources:
  jobs:
    j:
      name: j
      webhook_notifications:
        on_failure:
          - id: ${var.webhook_id}
      tasks: []
""")
        _, node = next(iter(patcher.find_job_nodes(doc)))
        changed, skipped = patcher.patch_webhooks(node, WEBHOOK_ID, ["on_failure", "on_success"])
        assert changed is True
        assert skipped == ["on_failure"]
        assert node["webhook_notifications"]["on_success"][0]["id"] == WEBHOOK_ID


WITH_TARGET_OVERRIDE = """
resources:
  jobs:
    shared_job:
      name: shared
      tasks: []

targets:
  prod:
    mode: production
    resources:
      jobs:
        shared_job:
          name: shared-prod
          timeout_seconds: 7200
"""


class TestFindJobNodes:
    def test_yields_base_jobs(self, yaml):
        doc = _load(yaml, BASE_ONLY)
        nodes = list(patcher.find_job_nodes(doc))
        assert [loc for loc, _ in nodes] == ["resources.jobs.base_job"]

    def test_yields_target_overrides_with_prefix(self, yaml):
        doc = _load(yaml, WITH_TARGET_OVERRIDE)
        locs = {loc for loc, _ in patcher.find_job_nodes(doc)}
        assert "resources.jobs.shared_job" in locs
        assert "targets.prod.resources.jobs.shared_job" in locs


class TestOverrideDupEvents:
    def test_override_already_has_same_id(self, yaml):
        doc = _load(yaml, f"""
targets:
  prod:
    resources:
      jobs:
        j:
          webhook_notifications:
            on_failure:
              - id: {WEBHOOK_ID}
""")
        target_node = doc["targets"]["prod"]["resources"]["jobs"]["j"]
        assert patcher._override_dup_events(target_node, WEBHOOK_ID, ["on_failure", "on_success"]) == [
            "on_failure"
        ]

    def test_override_has_different_id_no_dup(self, yaml):
        doc = _load(yaml, """
targets:
  prod:
    resources:
      jobs:
        j:
          webhook_notifications:
            on_failure:
              - id: other-id
""")
        target_node = doc["targets"]["prod"]["resources"]["jobs"]["j"]
        assert patcher._override_dup_events(target_node, WEBHOOK_ID, ["on_failure"]) == []

    def test_override_without_webhook_block(self, yaml):
        doc = _load(yaml, """
targets:
  prod:
    resources:
      jobs:
        j:
          name: j
""")
        target_node = doc["targets"]["prod"]["resources"]["jobs"]["j"]
        assert patcher._override_dup_events(target_node, WEBHOOK_ID, ["on_failure"]) == []


# --------------------------------------------------------------------------- #
# Integration: full bundle patch against examples/caveats
# --------------------------------------------------------------------------- #


@pytest.fixture
def caveats_bundle(tmp_path):
    """Copy examples/caveats to a temp dir so the patcher mutates a throwaway tree."""
    src = REPO_ROOT / "examples" / "caveats"
    dst = tmp_path / "caveats"
    shutil.copytree(src, dst, ignore=shutil.ignore_patterns(".databricks"))
    return dst


class TestCaveatsBundleEndToEnd:
    """The caveats bundle is purpose-built to hit every DAB-specific patcher
    rule at once. After --apply we expect: base files patched, ${var.*}
    files untouched, target overrides untouched."""

    def test_apply_writes_to_base_files_only(self, caveats_bundle, monkeypatch):
        monkeypatch.setattr(
            "sys.argv",
            [
                "patch_bundle_yaml.py",
                "--bundle-dir", str(caveats_bundle),
                "--webhook-id", WEBHOOK_ID,
                "--apply",
            ],
        )
        assert patcher.main() == 0

        analytics = (caveats_bundle / "resources" / "analytics.yml").read_text()
        assert WEBHOOK_ID in analytics, "analytics base should be patched"

        etl = (caveats_bundle / "resources" / "etl.yml").read_text()
        assert WEBHOOK_ID in etl, "anchor-shared jobs should each get the block"

        reporting = (caveats_bundle / "resources" / "reporting.yml").read_text()
        assert WEBHOOK_ID not in reporting, "${var.*} event lists must be skipped"
        assert "${var.webhook_id}" in reporting, "existing variable refs must survive"

        root = (caveats_bundle / "databricks.yml").read_text()
        assert WEBHOOK_ID not in root, "target overrides must not be patched"

    def test_dry_run_does_not_write(self, caveats_bundle, monkeypatch, capsys):
        before = {p: p.read_text() for p in (caveats_bundle / "resources").glob("*.yml")}
        monkeypatch.setattr(
            "sys.argv",
            [
                "patch_bundle_yaml.py",
                "--bundle-dir", str(caveats_bundle),
                "--webhook-id", WEBHOOK_ID,
            ],
        )
        assert patcher.main() == 0
        after = {p: p.read_text() for p in (caveats_bundle / "resources").glob("*.yml")}
        assert before == after
        # The unified diff is emitted to stdout in dry-run.
        out = capsys.readouterr().out
        assert WEBHOOK_ID in out

    def test_rerun_is_idempotent(self, caveats_bundle, monkeypatch):
        argv = [
            "patch_bundle_yaml.py",
            "--bundle-dir", str(caveats_bundle),
            "--webhook-id", WEBHOOK_ID,
            "--apply",
        ]
        monkeypatch.setattr("sys.argv", argv)
        assert patcher.main() == 0
        after_first = (caveats_bundle / "resources" / "analytics.yml").read_text()

        assert patcher.main() == 0
        after_second = (caveats_bundle / "resources" / "analytics.yml").read_text()
        assert after_first == after_second


class TestRunCallable:
    """Lock the run(**kwargs) contract that notebook widgets map to. The
    patcher notebook calls patcher.run(bundle_dir=..., webhook_id=..., apply=...)
    after reading widgets — that path must produce the same files-on-disk as
    the equivalent CLI invocation."""

    def test_run_apply_matches_cli(self, caveats_bundle):
        rc = patcher.run(
            bundle_dir=str(caveats_bundle),
            webhook_id=WEBHOOK_ID,
            apply=True,
        )
        assert rc == 0
        analytics = (caveats_bundle / "resources" / "analytics.yml").read_text()
        assert WEBHOOK_ID in analytics

    def test_run_rejects_empty_webhook_id(self, caveats_bundle):
        with pytest.raises(SystemExit, match="webhook_id is required"):
            patcher.run(bundle_dir=str(caveats_bundle), webhook_id="")