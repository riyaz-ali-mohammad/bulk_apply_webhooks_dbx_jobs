# Databricks notebook source
# MAGIC %md
# MAGIC # Bulk-attach (or remove) a Webhook Notification Destination on Jobs
# MAGIC
# MAGIC Walks the Jobs API across one or more target workspaces and attaches (or
# MAGIC detaches, with `remove=true`) a webhook destination on every matching job.
# MAGIC Default is **dry-run** — flip `apply=true` to actually mutate.
# MAGIC
# MAGIC ## Multi-workspace + SP auth
# MAGIC Same model as the inventory notebook: a single Entra-ID SP registered as a
# MAGIC Databricks-account service principal, granted workspace-admin on each
# MAGIC target. The notebook loops over `workspace_urls`, one `WorkspaceClient`
# MAGIC per workspace. Per-workspace failures log a WARNING and the loop continues.
# MAGIC
# MAGIC ## Scan performance
# MAGIC The Jobs API does **not** support server-side filtering on tag or creator.
# MAGIC - `--limit` caps **mutations** (jobs that would actually be updated). When
# MAGIC   matches are sparse, the loop still walks the full workspace looking for
# MAGIC   more matches.
# MAGIC - `--scan-limit` caps the **walk itself** (jobs scanned, regardless of
# MAGIC   matches). Use this for "touch only the first N jobs the workspace
# MAGIC   returns" rollouts.
# MAGIC - `--name-filter` is the **only filter the Jobs API supports server-side**.
# MAGIC   Massive speedup when the support team knows part of the job name.
# MAGIC
# MAGIC ## Modes (set via `remove` widget)
# MAGIC - `remove=false` (default): add mode. Requires `webhook_id`. Walks each
# MAGIC   workspace honoring `tag`/`owner`/`bundle_jobs`/`name_filter`/`scan_limit`.
# MAGIC - `remove=true` with `job_id` / `job_ids_from`: per-job rollback. Detaches
# MAGIC   webhooks from explicit IDs. `webhook_id` optional (omit to clear all).
# MAGIC - `remove=true` without `job_id` / `job_ids_from`: workspace-walk rollback.
# MAGIC   `webhook_id` REQUIRED.
# MAGIC
# MAGIC ## Bundle-managed jobs
# MAGIC `bundle_jobs=skip` (default) leaves DAB-managed jobs untouched; `bundle
# MAGIC deploy` would otherwise silently overwrite the API edit. Use the patcher
# MAGIC notebook for bundle-managed jobs.

# COMMAND ----------

# MAGIC %pip install -q -r ../requirements.txt
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Authentication
dbutils.widgets.text("workspace_urls", "",
    "comma-separated target workspace URLs (empty = current workspace)")
dbutils.widgets.text("secret_scope", "webhook-rollout", "Databricks secret scope")
dbutils.widgets.text("client_id_key", "databricks_client_id", "secret key for SP client_id")
dbutils.widgets.text("client_secret_key", "databricks_client_secret", "secret key for SP client_secret")

# Operation
dbutils.widgets.text("webhook_id", "", "webhook destination ID (required in add mode)")
dbutils.widgets.dropdown("remove", "false", ["false", "true"], "remove mode")
dbutils.widgets.text("job_id", "", "explicit job IDs (comma-separated; remove mode only)")
dbutils.widgets.text("job_ids_from", "", "path to text/CSV of job IDs (remove mode only)")
dbutils.widgets.text("events", "on_failure,on_duration_warning_threshold_exceeded",
    "comma-separated event list (add mode)")
dbutils.widgets.dropdown("apply", "false", ["false", "true"], "actually mutate (vs dry-run)")

# Filters
dbutils.widgets.text("tag", "", "tag filter (key=value or key)")
dbutils.widgets.text("owner", "", "owner filter (comma-separated emails)")
dbutils.widgets.dropdown("bundle_jobs", "skip", ["skip", "include", "only"],
    "policy for DAB-managed jobs")

# Output (Delta inventory of bundle-managed jobs encountered)
dbutils.widgets.text("delta_table", "main.webhook_rollout.bundle_jobs",
    "fully-qualified UC table for bundle-managed jobs encountered (empty = no Delta output)")

# Performance
dbutils.widgets.text("name_filter", "",
    "server-side substring filter on job name (forwarded to jobs.list)")
dbutils.widgets.text("scan_limit", "", "hard cap on jobs scanned (empty = no cap)")
dbutils.widgets.text("limit", "", "cap on jobs to update (empty = no cap)")

# Pacing / retries
dbutils.widgets.text("max_retries", "5", "max retries on 429/5xx")
dbutils.widgets.text("base_sleep", "0.3", "base sleep (s) between updates")
dbutils.widgets.text("jitter", "0.4", "max random jitter (s) per update")
dbutils.widgets.text("progress_every", "500", "log progress every N jobs (0 disables)")
dbutils.widgets.dropdown("verbose", "false", ["false", "true"], "DEBUG logging")

# COMMAND ----------

import os
import sys

notebook_dir = os.path.dirname(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
repo_root = os.path.abspath(os.path.join("/Workspace" + notebook_dir, ".."))
notebooks_dir = os.path.abspath("/Workspace" + notebook_dir)
for p in (repo_root, notebooks_dir):
    if p not in sys.path:
        sys.path.insert(0, p)

import bulk_apply_webhooks
import _auth


def _parse_csv_ints(s: str):
    return [int(x.strip()) for x in s.split(",") if x.strip()]


def _parse_csv_strs(s: str):
    return [x.strip() for x in s.split(",") if x.strip()]


def _optional_int(s: str):
    s = s.strip()
    return int(s) if s else None


shared_kwargs = dict(
    webhook_id=dbutils.widgets.get("webhook_id").strip() or None,
    remove=dbutils.widgets.get("remove") == "true",
    job_id=_parse_csv_ints(dbutils.widgets.get("job_id")),
    job_ids_from=dbutils.widgets.get("job_ids_from").strip() or None,
    events=dbutils.widgets.get("events").strip(),
    tag=dbutils.widgets.get("tag").strip() or None,
    owner=_parse_csv_strs(dbutils.widgets.get("owner")),
    bundle_jobs=dbutils.widgets.get("bundle_jobs"),
    bundle_report="",  # CSV disabled in notebook mode
    apply=dbutils.widgets.get("apply") == "true",
    profile=None,
    max_retries=int(dbutils.widgets.get("max_retries")),
    base_sleep=float(dbutils.widgets.get("base_sleep")),
    jitter=float(dbutils.widgets.get("jitter")),
    limit=_optional_int(dbutils.widgets.get("limit")),
    progress_every=int(dbutils.widgets.get("progress_every")),
    verbose=dbutils.widgets.get("verbose") == "true",
    spark=spark,
    delta_table=dbutils.widgets.get("delta_table").strip() or None,
    scan_limit=_optional_int(dbutils.widgets.get("scan_limit")),
    name_filter=dbutils.widgets.get("name_filter").strip() or None,
)

workspace_urls = _auth.parse_workspace_urls(dbutils.widgets.get("workspace_urls"))
clients = _auth.build_clients(
    workspace_urls=workspace_urls,
    secret_scope=dbutils.widgets.get("secret_scope").strip() or None,
    client_id_key=dbutils.widgets.get("client_id_key").strip(),
    client_secret_key=dbutils.widgets.get("client_secret_key").strip(),
    dbutils=dbutils,
)

mode_label = "REMOVE" if shared_kwargs["remove"] else "ADD"
apply_label = "APPLY" if shared_kwargs["apply"] else "DRY-RUN"
print(f"{mode_label} ({apply_label}) across {len(clients)} workspace(s)")

errors = []
for w in clients:
    print(f"\n=== {w.config.host} ===")
    try:
        rc = bulk_apply_webhooks.run(client=w, workspace_label=w.config.host, **shared_kwargs)
        if rc != 0:
            errors.append((w.config.host, f"run returned {rc}"))
    except Exception as e:
        errors.append((w.config.host, str(e)))
        print(f"ERROR {w.config.host}: {e}")

if errors:
    print(f"\n{len(errors)} workspace(s) failed:")
    for host, err in errors:
        print(f"  {host}: {err}")
    raise SystemExit(1)
print("\nDone.")
