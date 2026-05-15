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

dbutils.widgets.text(
    "bundle_dir", "/Workspace/Repos/<user>/<repo>",
    "path to bundle root (contains databricks.yml)",
)
dbutils.widgets.text("webhook_id", "", "webhook destination ID to attach (required)")
dbutils.widgets.text(
    "events",
    "on_failure,on_duration_warning_threshold_exceeded",
    "comma-separated event list",
)
dbutils.widgets.text("job", "", "filter by job `name:` field (comma-separated)")
dbutils.widgets.text("tag", "", "filter by job-resource tag (key=value or key)")
dbutils.widgets.dropdown("apply", "false", ["false", "true"], "write files in place (vs dry-run diff)")

# COMMAND ----------

import os
import sys

notebook_dir = os.path.dirname(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
repo_root = os.path.abspath(os.path.join("/Workspace" + notebook_dir, ".."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

import patch_bundle_yaml

job_raw = dbutils.widgets.get("job").strip()
kwargs = dict(
    bundle_dir=dbutils.widgets.get("bundle_dir").strip(),
    webhook_id=dbutils.widgets.get("webhook_id").strip(),
    events=dbutils.widgets.get("events").strip(),
    job=[j.strip() for j in job_raw.split(",") if j.strip()] if job_raw else [],
    tag=dbutils.widgets.get("tag").strip() or None,
    apply=dbutils.widgets.get("apply") == "true",
    verbose=False,
)
print("Calling patch_bundle_yaml.run with:", kwargs)
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
