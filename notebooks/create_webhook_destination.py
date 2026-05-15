# Databricks notebook source
# MAGIC %md
# MAGIC # Create Webhook Notification Destination (multi-workspace)
# MAGIC
# MAGIC Idempotent on `display_name`: in each target workspace, if a destination
# MAGIC with the supplied name already exists, this notebook reports the existing
# MAGIC ID and skips. Default is **dry-run** (`apply=false`) — flip to `apply=true`
# MAGIC to actually create.
# MAGIC
# MAGIC ## Multi-workspace + SP auth
# MAGIC Loops over `workspace_urls` and creates the same destination (same name +
# MAGIC URL) in each, authenticated as the global Entra-ID SP whose credentials
# MAGIC live in a Databricks secret scope. The SP must have **account-admin** for
# MAGIC notification destinations on each target workspace.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC Notebook lives in a workspace folder / Git folder with siblings
# MAGIC `create_webhook_destination.py` and `_auth.py` at `../`.

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

# Destination
dbutils.widgets.text("url", "", "webhook URL (https://...)")
dbutils.widgets.text("name", "", "destination display name (must be unique per workspace)")
dbutils.widgets.dropdown("apply", "false", ["false", "true"], "actually create (vs dry-run)")
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

import create_webhook_destination
import _auth

shared_kwargs = dict(
    url=dbutils.widgets.get("url").strip(),
    name=dbutils.widgets.get("name").strip(),
    apply=dbutils.widgets.get("apply") == "true",
    profile=None,
    verbose=dbutils.widgets.get("verbose") == "true",
)
if not shared_kwargs["url"] or not shared_kwargs["name"]:
    raise SystemExit("Both `url` and `name` widgets are required.")

workspace_urls = _auth.parse_workspace_urls(dbutils.widgets.get("workspace_urls"))
clients = _auth.build_clients(
    workspace_urls=workspace_urls,
    secret_scope=dbutils.widgets.get("secret_scope").strip() or None,
    client_id_key=dbutils.widgets.get("client_id_key").strip(),
    client_secret_key=dbutils.widgets.get("client_secret_key").strip(),
    dbutils=dbutils,
)

apply_label = "APPLY" if shared_kwargs["apply"] else "DRY-RUN"
print(f"Create destination ({apply_label}) name={shared_kwargs['name']} "
      f"across {len(clients)} workspace(s)")

errors = []
for w in clients:
    print(f"\n=== {w.config.host} ===")
    try:
        rc = create_webhook_destination.run(client=w, **shared_kwargs)
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
