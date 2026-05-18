# Databricks notebook source
# MAGIC %md
# MAGIC # Attach a Webhook Notification Destination to Direct (non-DAB) Jobs
# MAGIC
# MAGIC Walks the Jobs API across one or more target workspaces and attaches a
# MAGIC webhook destination on every matching **non-DAB** job. Default is
# MAGIC **dry-run** — flip `apply=true` to actually mutate.
# MAGIC
# MAGIC DAB-managed (Asset Bundle) jobs are **always skipped** — there is no
# MAGIC widget to override this. `databricks bundle deploy` would silently
# MAGIC overwrite API edits, so they belong to the patcher notebook
# MAGIC (`notebooks/patch_bundle_yaml`). For an inventory of DAB jobs in a
# MAGIC workspace, run `notebooks/inventory_jobs` and filter
# MAGIC `WHERE deployment_kind = 'BUNDLE'`.
# MAGIC
# MAGIC For the rollback / detach path, see the companion notebook
# MAGIC `notebooks/remove_webhooks`.
# MAGIC
# MAGIC ## Multi-workspace + SP auth
# MAGIC Same model as the inventory notebook: a single Entra-ID SP registered as a
# MAGIC Databricks-account service principal, granted workspace-admin on each
# MAGIC target. The notebook loops over `WORKSPACE_URLS`, one `WorkspaceClient`
# MAGIC per workspace. Per-workspace failures log a WARNING and the loop continues.
# MAGIC
# MAGIC ## Scan performance
# MAGIC The Jobs API does **not** support server-side filtering on tag or creator.
# MAGIC - `limit` caps **mutations** (jobs that would actually be updated). When
# MAGIC   matches are sparse, the loop still walks the full workspace looking for
# MAGIC   more matches.
# MAGIC - `scan_limit` caps the **walk itself** (jobs scanned, regardless of
# MAGIC   matches). Use this for "touch only the first N jobs the workspace
# MAGIC   returns" rollouts.

# COMMAND ----------

# MAGIC %md
# MAGIC No `%pip install` needed — this notebook only imports `databricks-sdk`,
# MAGIC which is pre-installed in the Databricks runtime. Installing it again
# MAGIC from PyPI would upgrade `protobuf` past the runtime's pinned version
# MAGIC and break PySpark.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets

# COMMAND ----------

# Authentication
dbutils.widgets.text("secret_scope", "webhook-rollout", "Databricks secret scope")

# Operation
dbutils.widgets.text("webhook_name", "", "webhook destination display_name (required; resolved to id per workspace)")
dbutils.widgets.multiselect(
    "events",
    "on_failure",
    ["on_start", "on_success", "on_failure", "on_duration_warning_threshold_exceeded"],
    "events to attach the webhook on (multi-select; default is on_failure only — add on_duration_warning_threshold_exceeded in the UI if desired)",
)
dbutils.widgets.dropdown("apply", "false", ["false", "true"], "actually mutate (vs dry-run)")

# Filters
dbutils.widgets.text("tag", "", "tag filter (key=value or key)")
dbutils.widgets.text("owner", "", "owner filter (comma-separated emails)")

# Performance
dbutils.widgets.text("scan_limit", "", "hard cap on jobs scanned (empty = no cap)")
dbutils.widgets.text("limit", "", "cap on jobs to update (empty = no cap)")

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
webhook_name = dbutils.widgets.get("webhook_name").strip()
events = dbutils.widgets.get("events").strip()
apply_flag = dbutils.widgets.get("apply")
tag = dbutils.widgets.get("tag").strip()
owner_raw = dbutils.widgets.get("owner").strip()
scan_limit = dbutils.widgets.get("scan_limit").strip()
limit = dbutils.widgets.get("limit").strip()

# COMMAND ----------

print(f"secret_scope:  {secret_scope!r}")
print(f"webhook_name:  {webhook_name!r}")
print(f"events:        {events!r}")
print(f"apply:         {apply_flag!r}")
print(f"tag:           {tag!r}")
print(f"owner:         {owner_raw!r}")
print(f"scan_limit:    {scan_limit!r}")
print(f"limit:         {limit!r}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hardcoded secret keys + pacing knobs
# MAGIC Rarely changed run-to-run — would clutter the widget pane. Edit the
# MAGIC constants if your secret scope uses different key names or you need
# MAGIC different retry/throttle behaviour.

# COMMAND ----------

# Auth mode:
#   "azure-direct" (default) — Azure AD direct auth. Secret scope holds the
#       Azure Application ID, the Azure tenant ID, and the Entra-ID secret.
#   "databricks-oauth" — SP is registered as a Databricks-account service
#       principal; secret scope holds the *Databricks-issued* OAuth
#       client_id / client_secret. SP_TENANT_ID_KEY is unused.
AUTH_MODE = "azure-direct"
SP_CLIENT_ID_KEY = "azure_client_id"
SP_CLIENT_SECRET_KEY = "azure_client_secret"
SP_TENANT_ID_KEY = "azure_tenant_id"  # used only when AUTH_MODE = "azure-direct"
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
    required_keys = [SP_CLIENT_ID_KEY, SP_CLIENT_SECRET_KEY]
    if AUTH_MODE == "azure-direct":
        required_keys.append(SP_TENANT_ID_KEY)
    present = {k.key for k in dbutils.secrets.list(secret_scope)}
    for required in required_keys:
        if required not in present:
            raise SystemExit(
                f"Secret scope {secret_scope!r} is missing key {required!r} "
                f"(AUTH_MODE={AUTH_MODE!r}). Keys present: {sorted(present)}. "
                f"Edit the SP_*_KEY constants in this notebook if your scope "
                f"uses different names."
            )
    print(f"Secret scope {secret_scope!r} OK ({AUTH_MODE}) — keys present: {sorted(present)}")
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

import apply_webhooks_to_direct_jobs
import create_webhook_destination
import _auth


def _parse_csv_strs(s: str):
    return [x.strip() for x in s.split(",") if x.strip()]


def _optional_int(s: str):
    s = s.strip()
    return int(s) if s else None


clients = _auth.build_clients(
    workspace_urls=[u.strip().rstrip("/") for u in WORKSPACE_URLS if u and u.strip()],
    secret_scope=secret_scope or None,
    client_id_key=SP_CLIENT_ID_KEY,
    client_secret_key=SP_CLIENT_SECRET_KEY,
    dbutils=dbutils,
    auth_mode=AUTH_MODE,
    tenant_id_key=SP_TENANT_ID_KEY,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve `webhook_name` → `webhook_id` per workspace
# MAGIC Notification destination IDs are per-workspace — the same display_name
# MAGIC has a different UUID in each workspace. Page through the workspace's
# MAGIC `/api/2.0/notification-destinations` and match on `display_name`. Fail
# MAGIC fast if any target workspace doesn't contain a destination with this
# MAGIC name (create it first via `notebooks/create_webhook_destination_nb`).

# COMMAND ----------

if not webhook_name:
    raise SystemExit("webhook_name widget is required.")

webhook_ids_by_host = {}
missing_hosts = []
for w in clients:
    dest = create_webhook_destination.find_existing(w, webhook_name)
    if dest is None:
        missing_hosts.append(w.config.host)
        print(f"  {w.config.host}: NOT FOUND")
    else:
        webhook_ids_by_host[w.config.host] = dest["id"]
        print(f"  {w.config.host}: {webhook_name!r} -> {dest['id']}")

if missing_hosts:
    raise Exception(
        f"Webhook destination {webhook_name!r} not found in {len(missing_hosts)} "
        f"workspace(s): {missing_hosts}. Create it first via "
        f"notebooks/create_webhook_destination_nb, then re-run this notebook."
    )

# COMMAND ----------

shared_kwargs = dict(
    events=events,
    tag=tag or None,
    owner=_parse_csv_strs(owner_raw),
    apply=apply_flag == "true",
    profile=None,
    max_retries=MAX_RETRIES,
    base_sleep=BASE_SLEEP,
    jitter=JITTER,
    limit=_optional_int(limit),
    progress_every=PROGRESS_EVERY,
    verbose=False,
    scan_limit=_optional_int(scan_limit),
)

apply_label = "APPLY" if shared_kwargs["apply"] else "DRY-RUN"
print(f"ADD ({apply_label}) across {len(clients)} workspace(s)")

errors = []
for w in clients:
    print(f"\n=== {w.config.host} ===")
    try:
        rc = apply_webhooks_to_direct_jobs.run(
            client=w,
            workspace_label=w.config.host,
            webhook_id=webhook_ids_by_host[w.config.host],
            **shared_kwargs,
        )
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
