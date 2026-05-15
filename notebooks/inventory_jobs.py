# Databricks notebook source
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
# MAGIC Leave `workspace_urls` empty to fall back to notebook-auto-auth against the
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
# MAGIC - UC catalog/schema/volume in the `delta_table` widget must exist and be
# MAGIC   writable by the SP.

# COMMAND ----------

# MAGIC %pip install -q -r ../requirements.txt
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets

# COMMAND ----------

# Authentication
dbutils.widgets.text("workspace_urls", "",
    "comma-separated target workspace URLs (empty = current workspace)")
dbutils.widgets.text("secret_scope", "webhook-rollout",
    "Databricks secret scope holding SP credentials")
dbutils.widgets.text("client_id_key", "databricks_client_id", "secret key for SP client_id")
dbutils.widgets.text("client_secret_key", "databricks_client_secret", "secret key for SP client_secret")

# Output
dbutils.widgets.text("delta_table", "main.webhook_rollout.jobs_inventory",
    "fully-qualified UC table name for the inventory")

# Filters
dbutils.widgets.text("tag", "", "tag filter (key=value or key)")
dbutils.widgets.text("owner", "", "owner filter (comma-separated creator emails)")
dbutils.widgets.dropdown("enrich_bundles", "false", ["false", "true"],
    "fetch per-bundle metadata (one /Workspace download per unique bundle)")

# Performance
dbutils.widgets.text("name_filter", "",
    "server-side substring filter on job name (forwarded to jobs.list)")
dbutils.widgets.text("scan_limit", "", "hard cap on jobs scanned (empty = no cap)")
dbutils.widgets.text("progress_every", "500", "log progress every N jobs (0 disables)")
dbutils.widgets.text("top_n", "10", "top-N bundles/creators in summary")
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

import inventory_jobs
import _auth


def _optional_int(s: str):
    s = s.strip()
    return int(s) if s else None


owner_raw = dbutils.widgets.get("owner").strip()
shared_kwargs = dict(
    profile=None,
    tag=dbutils.widgets.get("tag").strip() or None,
    owner=[o.strip() for o in owner_raw.split(",") if o.strip()] if owner_raw else [],
    output="",  # CSV disabled in notebook mode; Delta is the output
    enrich_bundles=dbutils.widgets.get("enrich_bundles") == "true",
    top_n=int(dbutils.widgets.get("top_n")),
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

print(f"Inventorying {len(clients)} workspace(s) → {shared_kwargs['delta_table']}")
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
print(f"\nDone. SELECT * FROM {shared_kwargs['delta_table']}")
