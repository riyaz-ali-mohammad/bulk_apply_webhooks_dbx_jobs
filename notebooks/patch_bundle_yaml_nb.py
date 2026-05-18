# Databricks notebook source
# MAGIC %md
# MAGIC # Patch a DAB Bundle's YAML to attach `webhook_notifications`
# MAGIC
# MAGIC The durable counterpart to the bulk-apply notebook for **bundle-managed jobs**.
# MAGIC Reads `databricks.yml` plus every file matched by its `include:` globs, and
# MAGIC merges the supplied webhook ID into each configured event list. Default is
# MAGIC **dry-run** — flip `apply=true` to write files in place.
# MAGIC
# MAGIC ## Workflow (this notebook stops at "patched files + diff")
# MAGIC The bundle repo **must be checked out as a Databricks Git folder** so this
# MAGIC notebook can read/write its YAML files in place. After running:
# MAGIC 1. Inspect the diff printed below.
# MAGIC 2. From the **Repos UI** (or `%sh git -C <bundle_dir> ...`), commit the
# MAGIC    patched YAMLs and push to a feature branch.
# MAGIC 3. Open a PR for bundle-owner review.
# MAGIC 4. **Validate and deploy happen in CI/PR**, not from this notebook.
# MAGIC
# MAGIC ## DAB caveats this notebook already handles
# MAGIC - **Per-target overrides** (`targets.<env>.resources.jobs.<name>`) are
# MAGIC   **never** written. DAB deep-merge concatenates `webhook_notifications` event
# MAGIC   lists at deploy; patching both base + override produces a `Duplicate webhook
# MAGIC   ids` deploy failure. The patcher walks them only to *detect* and WARN.
# MAGIC - **`${var.*}` event entries** are skipped with a WARNING. The patcher writes
# MAGIC   literal IDs and can't resolve variables; if a variable resolves to the same
# MAGIC   destination, the deploy is rejected as a duplicate. Hand-edit if needed.
# MAGIC
# MAGIC **Prerequisites**
# MAGIC - This notebook + sibling `patch_bundle_yaml.py` live in a Git/workspace folder.
# MAGIC - The target bundle repo is checked out as a Databricks Git folder, e.g.
# MAGIC   `/Workspace/Repos/<user>/<repo>/<bundle-subdir>`. The SP running this
# MAGIC   notebook must have **Can Manage** on that Git folder to write files.

# COMMAND ----------

# MAGIC %pip install -q ruamel.yaml
# MAGIC dbutils.library.restartPython()
# MAGIC # ruamel.yaml is the only dep this notebook needs that isn't in the
# MAGIC # Databricks runtime. Do NOT install databricks-sdk here — the runtime
# MAGIC # ships it pinned to a protobuf-compatible version; upgrading via pip
# MAGIC # breaks PySpark.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets

# COMMAND ----------

# Bundle target
dbutils.widgets.text(
    "bundle_dir", "/Workspace/Repos/<user>/<repo>",
    "path to bundle root (contains databricks.yml)",
)

# Operation
dbutils.widgets.text("webhook_name", "",
    "webhook destination display_name to attach (required; resolved to id in the current workspace)")
dbutils.widgets.multiselect(
    "events",
    "on_failure,on_duration_warning_threshold_exceeded",
    ["on_start", "on_success", "on_failure", "on_duration_warning_threshold_exceeded"],
    "events to patch (multi-select)",
)
dbutils.widgets.dropdown("apply", "false", ["false", "true"], "write files in place (vs dry-run diff)")

# Filters
dbutils.widgets.text("job", "", "filter by job `name:` field (comma-separated)")
dbutils.widgets.text("tag", "", "filter by job-resource tag (key=value or key)")
dbutils.widgets.text(
    "owner", "",
    "filter by `permissions:` (comma-separated user/SP/group; matches IS_OWNER or CAN_MANAGE)",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read widget values
# MAGIC All `dbutils.widgets.get(...)` calls happen here so you can see in one
# MAGIC place what's being passed into the run.

# COMMAND ----------

bundle_dir = dbutils.widgets.get("bundle_dir").strip()
webhook_name = dbutils.widgets.get("webhook_name").strip()
events = dbutils.widgets.get("events").strip()
apply_flag = dbutils.widgets.get("apply")
job_raw = dbutils.widgets.get("job").strip()
tag = dbutils.widgets.get("tag").strip()
owner_raw = dbutils.widgets.get("owner").strip()

# COMMAND ----------

print(f"bundle_dir:   {bundle_dir!r}")
print(f"webhook_name: {webhook_name!r}")
print(f"events:       {events!r}")
print(f"apply:        {apply_flag!r}")
print(f"job:          {job_raw!r}")
print(f"tag:          {tag!r}")
print(f"owner:        {owner_raw!r}")

# COMMAND ----------

import os
import sys
import importlib.util

notebook_dir = os.path.dirname(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
repo_root = os.path.abspath(os.path.join("/Workspace" + notebook_dir, ".."))


def _load_script(name):
    """Load a top-level script by absolute path via importlib. Bypasses sys.path
    because the regular import machinery misbehaves in THIS notebook after the
    `dbutils.library.restartPython()` in cell 4 — empirically it raises
    ModuleNotFoundError even with the file present and the path inserted into
    sys.path. Root cause not fully verified; the bypass sidesteps it entirely."""
    path = os.path.join(repo_root, f"{name}.py")
    spec = importlib.util.spec_from_file_location(f"{name}_script", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[f"{name}_script"] = mod
    spec.loader.exec_module(mod)
    return mod


patch_bundle_yaml = _load_script("patch_bundle_yaml")
create_webhook_destination = _load_script("create_webhook_destination")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve `webhook_name` → `webhook_id` in the current workspace
# MAGIC The patcher writes a literal id into `databricks.yml`, so we need a
# MAGIC concrete UUID. Page through `/api/2.0/notification-destinations` in the
# MAGIC workspace this notebook is running in, and match on `display_name`.
# MAGIC Raises if not found.
# MAGIC
# MAGIC Note: this assumes the bundle deploy-target workspace is the same as
# MAGIC where this notebook runs. If your bundle deploys cross-workspace, the
# MAGIC literal id written into YAML must also be valid there — verify the
# MAGIC destination exists with the same `display_name` in every target.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

if not webhook_name:
    raise SystemExit("webhook_name widget is required.")

_w = WorkspaceClient()
_dest = create_webhook_destination.find_existing(_w, webhook_name)
if _dest is None:
    raise Exception(
        f"Webhook destination {webhook_name!r} not found in workspace {_w.config.host}. "
        f"Create it first via notebooks/create_webhook_destination_nb."
    )
webhook_id = _dest["id"]
print(f"Resolved {webhook_name!r} -> {webhook_id} in {_w.config.host}")

# COMMAND ----------

kwargs = dict(
    bundle_dir=bundle_dir,
    webhook_id=webhook_id,
    events=events,
    job=[j.strip() for j in job_raw.split(",") if j.strip()] if job_raw else [],
    tag=tag or None,
    owner=[o.strip() for o in owner_raw.split(",") if o.strip()] if owner_raw else [],
    apply=apply_flag == "true",
    verbose=False,
)
rc = patch_bundle_yaml.run(**kwargs)
if rc != 0:
    raise SystemExit(rc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps (run from a terminal in the bundle's Git folder, or `%sh` below)
# MAGIC
# MAGIC ```bash
# MAGIC cd <bundle_dir>
# MAGIC git checkout -b add-webhook-<short-id>
# MAGIC git add -p
# MAGIC git commit -m "Attach webhook destination <short-id> to all jobs"
# MAGIC git push -u origin add-webhook-<short-id>
# MAGIC # Open a PR for bundle-owner review. CI runs `databricks bundle validate`
# MAGIC # and merge triggers `databricks bundle deploy`.
# MAGIC ```
