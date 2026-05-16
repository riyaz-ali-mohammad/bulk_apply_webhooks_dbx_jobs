# Databricks notebook source
# MAGIC %md
# MAGIC # Remove (detach) a Webhook Notification Destination from Jobs
# MAGIC
# MAGIC Companion to `apply_webhooks_to_direct_jobs` — the rollback / detach path. Default
# MAGIC is **dry-run** — flip `apply=true` to actually mutate.
# MAGIC
# MAGIC ## Multi-workspace + SP auth
# MAGIC Same model as the other notebooks: a single Entra-ID SP registered as a
# MAGIC Databricks-account service principal, granted workspace-admin on each
# MAGIC target. The notebook loops over `WORKSPACE_URLS`, one `WorkspaceClient`
# MAGIC per workspace. Per-workspace failures log a WARNING and the loop continues.
# MAGIC
# MAGIC ## Modes (selected automatically based on widget values)
# MAGIC - **Per-job rollback**: set `job_id` and/or `job_ids_from`. `webhook_id`
# MAGIC   optional — omit to clear ALL webhook_notifications from the listed jobs.
# MAGIC   Filters (`tag`/`owner`) and `bundle_jobs`/`scan_limit`/`limit` are
# MAGIC   ignored in this mode.
# MAGIC - **Workspace-walk rollback**: leave `job_id` and `job_ids_from` empty.
# MAGIC   `webhook_id` REQUIRED. Walks each workspace honoring
# MAGIC   `tag`/`owner`/`bundle_jobs`/`scan_limit`/`limit` and removes only that
# MAGIC   destination from every matching job that currently has it.
# MAGIC
# MAGIC ## Bundle-managed jobs
# MAGIC - **Per-job mode**: the script proceeds and emits a WARNING. API edits
# MAGIC   to bundle jobs are non-durable — `databricks bundle deploy` will
# MAGIC   re-add the webhook unless the bundle YAML is also patched via the
# MAGIC   patcher notebook.
# MAGIC - **Walk mode**: follows the `bundle_jobs` widget (default `skip`).

# COMMAND ----------

# MAGIC %md
# MAGIC No `%pip install` needed — this notebook only imports `databricks-sdk`,
# MAGIC which is pre-installed in the Databricks runtime. Installing it again
# MAGIC from PyPI would upgrade `protobuf` past the runtime's pinned version
# MAGIC and break PySpark (the Delta write below).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets

# COMMAND ----------

# Authentication
dbutils.widgets.text("secret_scope", "webhook-rollout", "Databricks secret scope")

# Operation
dbutils.widgets.text("webhook_id", "",
    "webhook destination ID (REQUIRED in walk mode; optional in per-job mode)")
dbutils.widgets.text("job_id", "", "explicit job IDs (comma-separated; per-job mode)")
dbutils.widgets.text("job_ids_from", "", "path to text/CSV of job IDs (per-job mode)")
dbutils.widgets.dropdown("apply", "false", ["false", "true"], "actually mutate (vs dry-run)")

# Filters (walk mode only)
dbutils.widgets.text("tag", "", "tag filter (key=value or key) — walk mode only")
dbutils.widgets.text("owner", "", "owner filter (comma-separated emails) — walk mode only")
dbutils.widgets.dropdown("bundle_jobs", "skip", ["skip", "include", "only"],
    "policy for DAB-managed jobs (walk mode only)")

# Performance (walk mode only)
dbutils.widgets.text("scan_limit", "", "hard cap on jobs scanned (empty = no cap)")
dbutils.widgets.text("limit", "", "cap on jobs to update (empty = no cap)")

# Output (Delta audit log)
dbutils.widgets.text("catalog", "main", "UC catalog for the Delta audit log")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Target workspaces
# MAGIC Edit the list below. One entry per target workspace URL. Leave the list
# MAGIC empty (`WORKSPACE_URLS = []`) to fall back to notebook-auto-auth against
# MAGIC the current workspace — handy for one-off testing before secrets are
# MAGIC wired up.

# COMMAND ----------

WORKSPACE_URLS = [
    # "https://adb-1234567890123456.7.azuredatabricks.net",
    # "https://adb-9876543210987654.4.azuredatabricks.net",
]
for u in WORKSPACE_URLS:
    print(u)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read widget values
# MAGIC All `dbutils.widgets.get(...)` calls happen here so you can see in one
# MAGIC place what's being passed into the run.

# COMMAND ----------

secret_scope = dbutils.widgets.get("secret_scope").strip()
webhook_id = dbutils.widgets.get("webhook_id").strip()
job_id = dbutils.widgets.get("job_id").strip()
job_ids_from = dbutils.widgets.get("job_ids_from").strip()
apply_flag = dbutils.widgets.get("apply")
tag = dbutils.widgets.get("tag").strip()
owner_raw = dbutils.widgets.get("owner").strip()
bundle_jobs = dbutils.widgets.get("bundle_jobs")
scan_limit = dbutils.widgets.get("scan_limit").strip()
limit = dbutils.widgets.get("limit").strip()
catalog = dbutils.widgets.get("catalog").strip() or "main"

# COMMAND ----------

print(f"secret_scope:  {secret_scope!r}")
print(f"webhook_id:    {webhook_id!r}")
print(f"job_id:        {job_id!r}")
print(f"job_ids_from:  {job_ids_from!r}")
print(f"apply:         {apply_flag!r}")
print(f"tag:           {tag!r}")
print(f"owner:         {owner_raw!r}")
print(f"bundle_jobs:   {bundle_jobs!r}")
print(f"scan_limit:    {scan_limit!r}")
print(f"limit:         {limit!r}")
print(f"catalog:       {catalog!r}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Output destination
# MAGIC Apply-only audit log of webhook removals — one row per successful
# MAGIC `jobs.update`. Schema / table parts are hardcoded; the catalog comes
# MAGIC from the `catalog` widget. The SP must have `USE CATALOG`, `USE SCHEMA`,
# MAGIC and `CREATE TABLE` on the target schema. Dry-runs (`apply=false`)
# MAGIC produce no rows. Mode is `append` — re-running the rollback adds
# MAGIC history, never clobbers it. Set the cell to `DELTA_TABLE = ""` to
# MAGIC disable the write entirely.

# COMMAND ----------

DELTA_TABLE = f"{catalog}.webhook_rollout.log_webhook_removals"
print(f"DELTA_TABLE: {DELTA_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hardcoded secret keys + pacing knobs
# MAGIC Rarely changed run-to-run — would clutter the widget pane. Edit the
# MAGIC constants if your secret scope uses different key names or you need
# MAGIC different retry/throttle behaviour.

# COMMAND ----------

SP_CLIENT_ID_KEY = "databricks_client_id"
SP_CLIENT_SECRET_KEY = "databricks_client_secret"
MAX_RETRIES = 5
BASE_SLEEP = 0.3
JITTER = 0.4
PROGRESS_EVERY = 500

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preflight: verify the scope has the keys we expect
# MAGIC Skipped when `WORKSPACE_URLS` is empty — in that case the notebook falls
# MAGIC back to notebook-auto-auth and doesn't need the secret scope.

# COMMAND ----------

if WORKSPACE_URLS:
    present = {k.key for k in dbutils.secrets.list(secret_scope)}
    for required in (SP_CLIENT_ID_KEY, SP_CLIENT_SECRET_KEY):
        if required not in present:
            raise SystemExit(
                f"Secret scope {secret_scope!r} is missing key {required!r}. "
                f"Keys present: {sorted(present)}. "
                f"Edit SP_CLIENT_ID_KEY / SP_CLIENT_SECRET_KEY in this notebook "
                f"if your scope uses different names."
            )
    print(f"Secret scope {secret_scope!r} OK — keys present: {sorted(present)}")
else:
    print("WORKSPACE_URLS empty — skipping secret-scope preflight "
          "(notebook-auto-auth will be used).")

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

import remove_webhooks
import _auth


def _parse_csv_ints(s: str):
    return [int(x.strip()) for x in s.split(",") if x.strip()]


def _parse_csv_strs(s: str):
    return [x.strip() for x in s.split(",") if x.strip()]


def _optional_int(s: str):
    s = s.strip()
    return int(s) if s else None


shared_kwargs = dict(
    webhook_id=webhook_id or None,
    job_id=_parse_csv_ints(job_id),
    job_ids_from=job_ids_from or None,
    tag=tag or None,
    owner=_parse_csv_strs(owner_raw),
    bundle_jobs=bundle_jobs,
    bundle_report="",  # CSV disabled in notebook mode
    apply=apply_flag == "true",
    profile=None,
    max_retries=MAX_RETRIES,
    base_sleep=BASE_SLEEP,
    jitter=JITTER,
    limit=_optional_int(limit),
    progress_every=PROGRESS_EVERY,
    verbose=False,
    spark=spark,
    delta_table=DELTA_TABLE or None,
    scan_limit=_optional_int(scan_limit),
)

clients = _auth.build_clients(
    workspace_urls=[u.strip().rstrip("/") for u in WORKSPACE_URLS if u and u.strip()],
    secret_scope=secret_scope or None,
    client_id_key=SP_CLIENT_ID_KEY,
    client_secret_key=SP_CLIENT_SECRET_KEY,
    dbutils=dbutils,
)

apply_label = "APPLY" if shared_kwargs["apply"] else "DRY-RUN"
mode_label = "PER-JOB" if (shared_kwargs["job_id"] or shared_kwargs["job_ids_from"]) else "WALK"
print(f"REMOVE ({mode_label}, {apply_label}) across {len(clients)} workspace(s)")

errors = []
for w in clients:
    print(f"\n=== {w.config.host} ===")
    try:
        rc = remove_webhooks.run(client=w, workspace_label=w.config.host, **shared_kwargs)
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

# COMMAND ----------

# Inspect the removal audit log written above. Resolves the table name from
# DELTA_TABLE so it tracks the catalog widget — no manual edit needed when the
# catalog changes.
display(spark.sql(f"SELECT * FROM {DELTA_TABLE} ORDER BY deleted_at DESC"))
