# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# ///
# MAGIC %md
# MAGIC # Inventory Jobs (read-only, multi-workspace)
# MAGIC
# MAGIC Walks every job across one or more target workspaces and classifies each as
# MAGIC `BUNDLE` (DAB-managed) or `DIRECT`. Writes a per-job inventory to a Delta
# MAGIC table partitioned by `workspace_host`. Use this **before** any rollout to
# MAGIC size up the work.
# MAGIC
# MAGIC ## Auth model
# MAGIC One **global Entra-ID service principal**, registered as a Databricks-account
# MAGIC service principal, with workspace-admin granted on every target workspace.
# MAGIC The SP's OAuth `client_id` and `client_secret` live in a Databricks secret
# MAGIC scope; this notebook reads them via `dbutils.secrets.get(...)` and constructs
# MAGIC one `WorkspaceClient` per target workspace.
# MAGIC
# MAGIC Leave `WORKSPACE_URLS` empty to fall back to notebook-auto-auth against the
# MAGIC current workspace (handy for one-off testing before secrets are wired up).
# MAGIC
# MAGIC ## Output
# MAGIC `SELECT * FROM <delta_table>` returns the latest inventory across all
# MAGIC workspaces that have been scanned. Per-workspace re-runs replace only that
# MAGIC workspace's partition (idempotent via dynamic partition overwrite).
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - This notebook lives in a workspace folder / Git folder containing the
# MAGIC   sibling `inventory_jobs.py` and `_auth.py` at `../`.
# MAGIC - UC catalog/schema/volume in the `DELTA_TABLE` constant must exist and be
# MAGIC   writable by the SP.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets

# COMMAND ----------

# Authentication
dbutils.widgets.text("secret_scope", "webhook-rollout",
    "Databricks secret scope holding SP credentials")

# Filters
dbutils.widgets.text("tag", "", "tag filter (key=value or key)")
dbutils.widgets.text("owner", "", "owner filter (comma-separated creator emails)")
dbutils.widgets.dropdown("enrich_bundles", "false", ["false", "true"],
    "fetch per-bundle metadata (one /Workspace download per unique bundle)")

# Performance
dbutils.widgets.text("scan_limit", "", "hard cap on jobs scanned (empty = no cap)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Target workspaces
# MAGIC Edit the list below. One entry per target workspace URL. Leave the list
# MAGIC empty (`WORKSPACE_URLS = []`) to fall back to notebook-auto-auth against
# MAGIC the current workspace — handy for one-off testing before secrets are
# MAGIC wired up.

# COMMAND ----------

WORKSPACE_URLS = [
    # 'https://e2-demo-field-eng.cloud.databricks.com/',
    # 'https://e2-demo-west.cloud.databricks.com/'
]
for u in WORKSPACE_URLS:
    print(u)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Output destination
# MAGIC Edit the fully-qualified UC table below before running. The SP must have
# MAGIC `USE CATALOG`, `USE SCHEMA`, and `CREATE TABLE` on the target schema.

# COMMAND ----------

DELTA_TABLE = "riz_catalog.webhook_rollout.jobs_inventory"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read widget values
# MAGIC All `dbutils.widgets.get(...)` calls happen here so you can see in one
# MAGIC place what's being passed into the run.

# COMMAND ----------

secret_scope = dbutils.widgets.get("secret_scope").strip()
tag = dbutils.widgets.get("tag").strip()
owner_raw = dbutils.widgets.get("owner").strip()
enrich_bundles = dbutils.widgets.get("enrich_bundles")
scan_limit = dbutils.widgets.get("scan_limit").strip()

# COMMAND ----------

print(f"secret_scope:   {secret_scope!r}")
print(f"tag:            {tag!r}")
print(f"owner:          {owner_raw!r}")
print(f"enrich_bundles: {enrich_bundles!r}")
print(f"scan_limit:     {scan_limit!r}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hardcoded secret keys
# MAGIC Teams use these names by convention. If your scope uses different keys,
# MAGIC edit them here rather than adding more widgets.

# COMMAND ----------

SP_CLIENT_ID_KEY = "databricks_client_id"
SP_CLIENT_SECRET_KEY = "databricks_client_secret"

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

import inventory_jobs
import _auth


def _optional_int(s: str):
    s = s.strip()
    return int(s) if s else None


shared_kwargs = dict(
    profile=None,
    tag=tag or None,
    owner=[o.strip() for o in owner_raw.split(",") if o.strip()] if owner_raw else [],
    output="",  # CSV disabled in notebook mode; Delta is the output
    enrich_bundles=enrich_bundles == "true",
    top_n=10,
    progress_every=500,
    verbose=False,
    spark=spark,
    delta_table=DELTA_TABLE,
    scan_limit=_optional_int(scan_limit),
)

clients = _auth.build_clients(
    workspace_urls=[u.strip().rstrip("/") for u in WORKSPACE_URLS if u and u.strip()],
    secret_scope=secret_scope or None,
    client_id_key=SP_CLIENT_ID_KEY,
    client_secret_key=SP_CLIENT_SECRET_KEY,
    dbutils=dbutils,
)

print(f"Inventorying {len(clients)} workspace(s) → {DELTA_TABLE}")
errors = []
for w in clients:
    print(f"\n=== {w.config.host} ===")
    try:
        rc = inventory_jobs.run(client=w, workspace_label=w.config.host, **shared_kwargs)
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
print(f"\nDone. SELECT * FROM {DELTA_TABLE}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inspect the inventory written above. Edit the table name if you
# MAGIC -- changed the DELTA_TABLE constant.
# MAGIC SELECT * FROM riz_catalog.webhook_rollout.jobs_inventory

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT workspace_host,count(*) FROM riz_catalog.webhook_rollout.jobs_inventory
# MAGIC GROUP BY workspace_host

# COMMAND ----------


