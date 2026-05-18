# Databricks notebooks — webhook rollout runbook

Hands-on guide for launching the five notebooks in this directory. The
[top-level README](../README.md) covers the CLI variants and the design
rationale; this one is the runbook for the support team.

```
notebooks/
├── _auth.py                          (helper — do not run directly)
├── inventory_jobs_nb.py              read-only: list jobs, classify BUNDLE vs DIRECT, write Delta
├── create_webhook_destination_nb.py  create the webhook destination (idempotent on display_name)
├── apply_webhooks_to_direct_jobs_nb.py  attach the destination to direct-deployed jobs (DAB jobs always skipped)
├── remove_webhooks_nb.py             detach the destination from jobs (rollback / cleanup)
└── patch_bundle_yaml_nb.py           edit DAB YAML in place for bundle-managed jobs
```

The notebook files end in `_nb.py` to avoid name collisions with the top-level
scripts they import. Each notebook imports the matching top-level script by
its short name (e.g. `notebooks/remove_webhooks_nb.py` does `import remove_webhooks`).
This is purely a packaging concern — section headers below use the conceptual
short names (`inventory_jobs`, `remove_webhooks`, etc.).

## One-time setup (admin)

The support team running these notebooks doesn't need to do any of this — it's
a one-time bootstrap that the workspace / platform admin handles.

### 1. Service principal with multi-workspace access

- Register the Entra-ID service principal (SP) as a **Databricks-account
  service principal** in the account console. This step matters even if the
  SP was originally created in Azure — see "If the SP was created in Azure"
  below.
- Grant it **workspace admin** on every target workspace it will scan or mutate.
- For `create_webhook_destination`, the SP needs **account-admin** for
  Notification Destinations (workspace admin alone is not sufficient — that's
  what the API enforces; if you hit a 403 here, check this).

#### If the SP was created in Azure (Entra ID)

The notebooks use **Databricks OAuth M2M** auth via
`WorkspaceClient(host, client_id, client_secret)`. Your Azure-created SP works
unchanged — provided it has been registered as a Databricks-account service
principal. After that registration, the credentials the notebooks consume are:

| Secret scope key             | Source                                                                       |
|------------------------------|------------------------------------------------------------------------------|
| `databricks_client_id`       | Azure SP's **Application (client) ID** (the UUID from Entra ID)              |
| `databricks_client_secret`   | OAuth secret **minted in the Databricks account console** under that SP     |

The Azure-side secret (the one you'd get from Entra ID's "Certificates &
secrets" blade) is **not** what goes here. The Databricks-issued OAuth secret
is — generated under **Account console → Service principals → \<your SP\> →
OAuth secrets → Generate secret**. No code change in `_auth.py` is needed.

There is a second path (Azure AD direct auth via `azure_tenant_id` /
`azure_client_id` / `azure_client_secret`) for the edge case where the SP
**cannot** be registered as a Databricks-account SP. It requires a small edit
to `_auth.py` to swap the kwargs passed to `WorkspaceClient(...)`. Prefer
Path A (the default) unless your platform team explicitly tells you the
account-SP registration is not an option.

### 2. Secret scope holding the SP's OAuth credentials

In the **management workspace** (the one the team will run notebooks from),
create a Databricks secret scope and put two keys in it:

| Key                         | Value                              |
|-----------------------------|------------------------------------|
| `databricks_client_id`      | SP's OAuth client ID (see table above) |
| `databricks_client_secret`  | SP's OAuth client secret (see table above) |

Defaults assumed by every notebook:
- scope name: `webhook-rollout` (widget-editable)
- key names: `databricks_client_id` / `databricks_client_secret` (hardcoded as
  constants near the top of each notebook — edit there if your scope uses
  different names)

Each notebook runs a preflight cell that lists the scope and fails fast with a
clear error if either key is missing.

### 3. UC schema for Delta output (`inventory_jobs`, `remove_webhooks`)

Two of the notebooks write to a Delta table. The SP needs `USE CATALOG`,
`USE SCHEMA`, and `CREATE TABLE` on the target schema. Defaults:

| Notebook              | Default table                              |
|-----------------------|--------------------------------------------|
| `inventory_jobs`      | `main.webhook_rollout.jobs_inventory`      |
| `remove_webhooks`     | `main.webhook_rollout.bundle_jobs` (walk mode only — bundle jobs encountered during rollback) |

Both are set as `DELTA_TABLE = "..."` Python constants near the top of each
notebook — edit them to your catalog/schema before the first run.

`apply_webhooks_to_direct_jobs` produces **no Delta output** — it always
skips DAB jobs and never fetches their metadata.

### 4. Check out this repo as a Databricks Git folder

In the management workspace: **Repos → Add Repo →** point at this repo's URL.
The five notebooks must sit next to the `.py` scripts they import (the
notebooks do `sys.path.insert(0, repo_root)` to import `inventory_jobs.py`
etc. as siblings).

## How a notebook is structured

Every notebook in this directory follows the same shape, so you only need to
learn it once:

1. **Markdown header** — what the notebook does, prerequisites, caveats.
2. **Widget declarations** (`dbutils.widgets.text(...)`) — the dynamic knobs.
   First run creates the widgets in the UI; subsequent runs read whatever you
   set in the widget pane.
3. **`WORKSPACE_URLS = [...]`** — Python list of target workspace hosts. Edit
   in place. Empty list falls back to notebook-auto-auth against the current
   workspace (handy for first-pass testing before the secret scope is wired).
4. **`DELTA_TABLE = "..."`** — fully qualified UC table for output. Edit in
   place.
5. **Read widget values** — every `dbutils.widgets.get(...)` happens in one
   cell so you can scan it.
6. **Print widget values** — echoes what got picked up. Eyeball this before
   running anything destructive.
7. **Secret-key constants + preflight** — verifies the scope holds the two
   keys the dispatcher needs.
8. **Main execution cell** — the multi-workspace loop. Per-workspace failures
   log a WARNING and the loop continues.
9. **`%sql SELECT * FROM <table>`** (inventory + bulk + remove only) — for
   quick output inspection without leaving the notebook.

## End-to-end workflow

```
inventory_jobs                     ← read-only, size up the work
       │
       ▼
create_webhook_destination         ← idempotent; creates the destination
       │
       ▼
apply_webhooks_to_direct_jobs (dry-run)      ← preview what would change
       │
       ▼
apply_webhooks_to_direct_jobs (apply=true)   ← attaches to DIRECT-deployed jobs only
       │
       ▼
patch_bundle_yaml                  ← for BUNDLE-deployed jobs, separate flow

remove_webhooks                    ← off-cycle: rollback / cleanup
```

`apply_webhooks_to_direct_jobs` **always** skips bundle-managed jobs (no flag
to override) because `databricks bundle deploy` would silently overwrite the
API edit. The patcher notebook is the durable path for those.

`remove_webhooks` is the inverse of `apply_webhooks_to_direct_jobs` — same
multi-workspace shape, opposite operation. Unlike the attach notebook, it CAN
operate on bundle jobs via the `bundle_jobs` widget (useful for cleaning up
stale references before re-patching). Run it when a rollout needs to be
reversed.

---

## 1. `inventory_jobs` — read-only

**Use first.** Walks every job across one or more workspaces, classifies each
as `BUNDLE` (DAB-managed) or `DIRECT`, and writes a per-job inventory to
Delta. No mutation, no webhook ID needed.

Widgets:

| Widget           | Purpose                                                            |
|------------------|--------------------------------------------------------------------|
| `secret_scope`   | Secret scope holding the SP credentials                            |
| `tag`            | Tag filter — `key=value` or just `key` (client-side, post-list)    |
| `owner`          | Comma-separated creator emails (client-side filter)                |
| `enrich_bundles` | `true` to fetch per-bundle metadata (one workspace download per unique bundle — slower) |
| `scan_limit`     | Hard cap on jobs scanned per workspace (empty = no cap)            |

To run:
1. Open `notebooks/inventory_jobs_nb` in the Databricks Git folder.
2. Edit `WORKSPACE_URLS` and `DELTA_TABLE` cells.
3. Set widgets in the UI (most can be left at defaults for a first pass).
4. **Run All**.
5. The trailing `%sql` cell shows the inventory. Per-workspace re-runs replace
   only that workspace's partition (idempotent).

## 2. `create_webhook_destination` — idempotent

Creates a generic-webhook Notification Destination in each target workspace.
If a destination with the same `display_name` already exists in a workspace,
it reports the existing ID and skips — safe to re-run.

Widgets:

| Widget         | Purpose                                                |
|----------------|--------------------------------------------------------|
| `secret_scope` | Secret scope holding the SP credentials                |
| `url`          | Webhook URL the destination POSTs to (`https://...`)   |
| `name`         | Display name (must be unique per workspace)            |
| `apply`        | `false` = dry-run (default). `true` = actually create. |

To run:
1. Edit `WORKSPACE_URLS`. (Same list as for `inventory_jobs`.)
2. Set `url` and `name` widgets.
3. Run with `apply=false` first to confirm auth and check for existing
   destinations.
4. Re-run with `apply=true` to create.

This notebook calls the REST endpoint directly (`POST
/api/2.0/notification-destinations`) via `WorkspaceClient.api_client.do(...)`,
not the typed `w.notification_destinations.*` API — the latter is not
present in the older SDK shipped on the Databricks runtime. See the comment
at the top of `../create_webhook_destination.py` for context.

**Outputs**: the destination ID for each workspace, printed to cell output.
You don't need to copy this id into the next notebook — `apply_webhooks_to_direct_jobs_nb`
takes the destination `display_name` and resolves it to the per-workspace id
itself (see step 3 widgets).

## 3. `apply_webhooks_to_direct_jobs` — attach (DIRECT jobs only)

Walks the Jobs API across each workspace and attaches the destination to
matching `DIRECT` jobs. Bundle-managed (DAB) jobs are **always skipped** —
there is no widget to override. For the bundle path, use the
`patch_bundle_yaml` notebook; to find the bundles that need patching, run
`inventory_jobs` and filter `WHERE deployment_kind = 'BUNDLE'`. For the
rollback / detach path, see `remove_webhooks` below.

Widgets:

| Widget          | Purpose                                                       |
|-----------------|---------------------------------------------------------------|
| `secret_scope`  | Secret scope holding the SP credentials                       |
| `webhook_name`  | Destination `display_name` from step 2 (required; resolved to id per workspace) |
| `events`        | Multi-select. Defaults to `on_failure,on_duration_warning_threshold_exceeded` |
| `apply`         | `false` = dry-run. `true` = actually mutate.                  |
| `tag`           | Tag filter (`key=value` or `key`)                             |
| `owner`         | Comma-separated creator emails                                |
| `scan_limit`    | Hard cap on jobs scanned (empty = no cap)                     |
| `limit`         | Hard cap on jobs **mutated** (different from `scan_limit` — see "gotchas") |

A dedicated cell paginates each workspace's
`/api/2.0/notification-destinations` and builds a `host -> id` map. If the
`display_name` isn't present in any target workspace, the notebook raises an
exception listing the missing workspaces — create the destination there first
via `notebooks/create_webhook_destination_nb`.

To run:
1. Edit `WORKSPACE_URLS`.
2. Set `webhook_name` to the destination's display name from step 2.
3. Run with `apply=false` first — produces the "would update N jobs" dry-run
   log. Bundle jobs encountered during the walk are logged with
   `SKIP bundle-managed` and counted under `bundle_skipped` in the final
   stats line.
4. Re-run with `apply=true` to attach.

## 4. `remove_webhooks` — detach (rollback / cleanup)

Inverse of `apply_webhooks_to_direct_jobs`. Two shapes, picked automatically from which
widgets you fill in:

- **Per-job rollback**: set `job_id` and/or `job_ids_from`. `webhook_id`
  optional — omit to clear **all** webhook_notifications from the listed jobs.
  Filters and walk-mode widgets are ignored.
- **Workspace-walk rollback**: leave `job_id` and `job_ids_from` empty.
  `webhook_id` **REQUIRED**. Walks each workspace honoring `tag` / `owner` /
  `bundle_jobs` / `scan_limit` / `limit` and removes only that destination
  from every matching job that currently has it.

Widgets:

| Widget          | Purpose                                                       |
|-----------------|---------------------------------------------------------------|
| `secret_scope`  | Secret scope holding the SP credentials                       |
| `webhook_id`    | Destination ID to detach. REQUIRED in walk mode; optional in per-job mode (omit to clear all). |
| `job_id`        | Comma-separated job IDs (per-job mode)                        |
| `job_ids_from`  | Path to a text/CSV of job IDs (per-job mode). UC Volume path recommended (`/Volumes/<cat>/<schema>/<vol>/file.csv`) — see "Gotchas" |
| `apply`         | `false` = dry-run. `true` = actually mutate.                  |
| `tag`           | Tag filter — walk mode only                                   |
| `owner`         | Comma-separated creator emails — walk mode only               |
| `bundle_jobs`   | `skip` (default) / `include` / `only` — walk mode only        |
| `scan_limit`    | Hard cap on jobs scanned — walk mode only                     |
| `limit`         | Hard cap on jobs mutated — walk mode only                     |

To run a workspace-wide rollback (most common shape):
1. Set `webhook_id` and leave `job_id` / `job_ids_from` empty.
2. Run with `apply=false` first — the dry-run logs which jobs would be
   detached without mutating anything.
3. Re-run with `apply=true`.

To roll back a specific list of jobs from `jobs_inventory.csv` (or the
`bundle_jobs.csv` produced by a prior `remove_webhooks` walk-mode run):
1. Upload the CSV to a UC Volume (e.g. `/Volumes/main/webhook_rollout/csvs/jobs.csv`).
2. Set `job_ids_from=/Volumes/.../jobs.csv` and (optionally) `webhook_id` to
   limit the removal to that specific destination.
3. Dry-run, then apply.

Bundle-managed jobs in per-job mode proceed with a `WARNING` (API edits are
non-durable — `databricks bundle deploy` will re-add the webhook unless the
bundle YAML is also patched via `patch_bundle_yaml`). In walk mode they
follow `bundle_jobs` (default `skip`).

## 5. `patch_bundle_yaml` — bundle-managed jobs

This is **single-workspace** (a DAB lives in one Git repo, deploys via CI).
Different from the other four notebooks in two ways:
- No multi-workspace dispatcher.
- It does a `%pip install ruamel.yaml` (the runtime doesn't ship it).
- The notebook **stops at "patched files + git diff"** — it does NOT call
  `databricks bundle validate` or `databricks bundle deploy`. PR review and
  CI deploy are intentional gates.

Widgets:

| Widget        | Purpose                                                       |
|---------------|---------------------------------------------------------------|
| `bundle_dir`  | Path to the bundle root (contains `databricks.yml`)           |
| `webhook_id`  | Destination ID from step 2                                    |
| `events`      | Comma-separated event list                                    |
| `job`        | Filter by job `name:` field (comma-separated)                  |
| `tag`         | Filter by job-resource tag (`key=value` or `key`)              |
| `apply`       | `false` = dry-run + diff. `true` = write files in place.       |

To run:
1. Bundle owner clones the DAB repo as a Databricks Git folder, e.g.
   `/Workspace/Repos/<user>/<dab-repo>`.
2. Opens `notebooks/patch_bundle_yaml_nb` (from **this** repo's checkout, not
   the DAB's).
3. Sets `bundle_dir` to the DAB Git folder path, `webhook_id` to the
   destination from step 2.
4. Runs with `apply=false` first → review the diff in cell output.
5. Re-runs with `apply=true` → YAMLs are edited in place in the DAB Git
   folder.
6. Bundle owner commits + pushes from the Repos UI; CI runs
   `databricks bundle validate` and merge triggers
   `databricks bundle deploy`.

Patcher caveats (the notebook handles all three; documented in detail in the
[top-level README](../README.md#caveats-bundle-workflow)):
- Per-target overrides (`targets.<env>.resources.jobs.<name>`) are **never
  written** — DAB deep-merge concatenates event lists at deploy and patching
  both base + override produces a `Duplicate webhook ids` deploy failure.
  The patcher walks them only to detect and warn.
- Event lists containing `${var.*}` entries are skipped with a WARNING.

---

## Gotchas

### `scan_limit` vs `limit`

`apply_webhooks_to_direct_jobs` exposes both, and they're not interchangeable:

- **`scan_limit`** caps the **walk itself** — stops after N jobs scanned,
  regardless of how many matched. Use this when you want "touch only the
  first N jobs the workspace returns."
- **`limit`** caps **mutations** — stops after N jobs would have been
  updated. If matches are sparse, the walk still iterates the full workspace
  looking for more matches.

In the first PepsiCo demo, `--limit 5` against a workspace of 4,376 jobs
walked all 4,376 to find 2 matches — that's `limit` working as designed.
Use `scan_limit` if you wanted the walk to stop early.

### SDK version skew

The Databricks runtime ships an older SDK (verified `0.20.0` on the
workspace we tested against). Two consequences:
- `inventory_jobs`, `apply_webhooks_to_direct_jobs`, `remove_webhooks`, and
  `create_webhook_destination` **do not** `%pip install databricks-sdk`.
  Installing it upgrades `protobuf` past the runtime's pinned version and
  breaks PySpark (which the Delta writes need). The notebooks rely on
  whatever the runtime ships.
- `create_webhook_destination` calls the REST endpoint directly because the
  typed `notification_destinations` API isn't in 0.20.0. Already handled —
  no action needed.

### Per-workspace failures don't halt the loop

The multi-workspace dispatcher logs `ERROR <host>: <message>` and continues
to the next workspace. At the end it raises `SystemExit(1)` if any
workspace failed, so the notebook ends in red, but successful workspaces
already wrote their Delta partitions. Re-running for just the failed
workspace replaces only that workspace's partition.

### Notebook-auto-auth fallback

Setting `WORKSPACE_URLS = []` skips the secret-scope preflight and
authenticates as **the user running the notebook** against the **current
workspace**. Useful for first-pass testing. Don't use this for production
rollouts — you lose the multi-workspace and SP-identity properties.
