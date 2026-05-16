# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# ///
# MAGIC %md
# MAGIC # Create Webhook Notification Destination (multi-workspace)
# MAGIC
# MAGIC Idempotent on `display_name`: in each target workspace, if a destination
# MAGIC with the supplied name already exists, this notebook reports the existing
# MAGIC ID and skips. Default is **dry-run** (`apply=false`) — flip to `apply=true`
# MAGIC to actually create.
# MAGIC
# MAGIC ## Multi-workspace + SP auth
# MAGIC Loops over `WORKSPACE_URLS` and creates the same destination (same name +
# MAGIC URL) in each, authenticated as the global Entra-ID SP whose credentials
# MAGIC live in a Databricks secret scope. The SP must have **account-admin** for
# MAGIC notification destinations on each target workspace.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC Notebook lives in a workspace folder / Git folder with siblings
# MAGIC `create_webhook_destination.py` and `_auth.py` at `../`.

# COMMAND ----------

# MAGIC %md
# MAGIC No `%pip install` needed — this notebook only imports `databricks-sdk`,
# MAGIC which is pre-installed in the Databricks runtime.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets

# COMMAND ----------

# %pip install -q databricks-sdk
# dbutils.library.restartPython()

# COMMAND ----------

# Authentication
dbutils.widgets.text("secret_scope", "webhook-rollout", "Databricks secret scope")

# Destination
dbutils.widgets.text("url", "", "webhook URL (https://...)")
dbutils.widgets.text("name", "", "destination display name (must be unique per workspace)")
dbutils.widgets.dropdown("apply", "false", ["false", "true"], "actually create (vs dry-run)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Target workspaces
# MAGIC Edit the list below. One entry per target workspace URL. Leave the list
# MAGIC empty (`WORKSPACE_URLS = []`) to fall back to notebook-auto-auth against
# MAGIC the current workspace.

# COMMAND ----------

WORKSPACE_URLS = [
    'https://e2-demo-field-eng.cloud.databricks.com/',
    'https://e2-demo-west.cloud.databricks.com/'
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
url = dbutils.widgets.get("url").strip()
name = dbutils.widgets.get("name").strip()
apply_flag = dbutils.widgets.get("apply")

# COMMAND ----------

print(f"secret_scope: {secret_scope!r}")
print(f"url:          {url!r}")
print(f"name:         {name!r}")
print(f"apply:        {apply_flag!r}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hardcoded secret keys
# MAGIC Team convention. Edit if your scope uses different names.

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

import create_webhook_destination
import _auth

shared_kwargs = dict(
    url=url,
    name=name,
    apply=apply_flag == "true",
    profile=None,
    verbose=False,
)
if not shared_kwargs["url"] or not shared_kwargs["name"]:
    raise SystemExit("Both `url` and `name` widgets are required.")

clients = _auth.build_clients(
    workspace_urls=[u.strip().rstrip("/") for u in WORKSPACE_URLS if u and u.strip()],
    secret_scope=secret_scope or None,
    client_id_key=SP_CLIENT_ID_KEY,
    client_secret_key=SP_CLIENT_SECRET_KEY,
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
