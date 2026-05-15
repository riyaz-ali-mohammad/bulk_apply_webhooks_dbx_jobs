# Bulk Apply Webhook Notifications to Databricks Jobs

Four Python scripts for rolling out a webhook-based Notification Destination across every job in a Databricks workspace:

1. **`inventory_jobs.py`** — read-only discovery. Walks the Jobs API and reports how many jobs are in the workspace and how many are DAB-deployed versus directly-deployed. Run this first when assessing a new workspace; the CSV it writes feeds steps 3 and 4.
2. **`create_webhook_destination.py`** — one-shot setup. Creates a generic-webhook Notification Destination from a URL, so you can skip the Admin Settings UI. Idempotent on display name.
3. **`bulk_apply_webhooks.py`** — workspace-side script. Walks every job via the Jobs API, attaches the webhook to manually-created jobs, and inventories bundle-managed jobs for hand-off (because API edits to bundle jobs are non-durable).
4. **`patch_bundle_yaml.py`** — companion script. Runs on a local checkout of a bundle repo, edits the bundle YAML in place to add `webhook_notifications` to job resources, producing a review-ready git diff. This is the durable fix for bundle-managed jobs.

All mutating scripts default to dry-run and are idempotent. `inventory_jobs.py` is read-only by design. The inventory, bulk, and patch scripts support tag/owner filters for staged rollout.

---

## What `inventory_jobs.py` does

Read-only discovery step. Use this when you've been pointed at a new workspace and need to understand the shape of it before planning a rollout — in particular, how many jobs need each of the two follow-up scripts.

- Walks `GET /api/2.2/jobs/list` (paginated).
- Classifies each job using the same check as `bulk_apply_webhooks.py`: `settings.deployment.kind == BUNDLE` ⇒ DAB-deployed, otherwise directly-deployed.
- Prints a short summary to stdout: total, `DAB-deployed`, `directly-deployed`, and top-N breakdowns by creator and (with `--enrich-bundles`) by bundle name.
- Writes `jobs_inventory.csv` — every job with a `deployment_kind` column up front. Disable via `--output ''`.
- `--enrich-bundles` reads each unique bundle's `metadata.json` from `/Workspace` to populate `bundle_name`, `target`, and `git_*` columns. Off by default because it adds one workspace download per unique bundle.
- Supports `--tag` and `--owner` filters, identical in shape to the bulk script.
- No mutation, no `--apply` flag. Safe to run anywhere with read access to the Jobs API.

---

## What `create_webhook_destination.py` does

- Validates the supplied URL is `http(s)://`.
- Lists existing notification destinations via the Databricks SDK and checks for a `display_name` collision.
- If a destination with that name already exists, prints its ID/URL and exits 0 (no mutation — idempotent on re-run).
- Otherwise creates a new generic-webhook destination via `notification_destinations.create()`.
- Prints a human-readable summary including the destination ID — that ID is what `bulk_apply_webhooks.py` and `patch_bundle_yaml.py` consume downstream.

---

## What `bulk_apply_webhooks.py` does

**Add mode (default)**

- Enumerates jobs via `GET /api/2.2/jobs/list` (paginated).
- For each job, computes the desired `webhook_notifications` block by merging the supplied webhook ID into the configured event lists (defaults: `on_failure`, `on_duration_warning_threshold_exceeded` — chosen to stay low-noise across many workspaces; pass `--events on_failure,on_success,on_start` if you also want lifecycle events). Existing webhooks are preserved.
- Calls `POST /api/2.2/jobs/update` to apply the change.
- Skips bundle-managed jobs by default (because `databricks bundle deploy` would overwrite API edits), and emits an inventory CSV of those jobs for hand-off to bundle owners.
- Honors rate limits with exponential backoff plus jitter, and paces calls between updates.

**Remove mode (`--remove`)**

Three shapes (see "Removing webhooks" below for full detail):
- **Per-job by ID** — `--job-id` (repeatable) and/or `--job-ids-from <path>` (text/CSV). Looks up each job directly, no list pagination.
- **Workspace-walk by webhook ID** — omit `--job-id`; `--webhook-id` REQUIRED. Walks `jobs/list` with the same `--tag` / `--owner` / `--bundle-jobs` filters as add mode, pre-checks each job for the webhook, and removes it only from jobs that have it.
- All shapes share dry-run / `--apply` / backoff semantics. Bundle-managed jobs: per-job mode WARNs and proceeds; workspace-walk mode follows `--bundle-jobs` (default `skip`, consistent with add mode — API removal is non-durable across `bundle deploy`).

---

## Prerequisites

| Tool | Required version | Why |
|------|------------------|-----|
| Python | 3.9+ | Runtime for the script. |
| `databricks-sdk` | >= 0.30.0 | API client used by `inventory_jobs.py`, `bulk_apply_webhooks.py`, and `create_webhook_destination.py`. Installed via `requirements.txt`. |
| `ruamel.yaml` | >= 0.17.0 | Round-trip YAML reader/writer used by `patch_bundle_yaml.py`. Installed via `requirements.txt`. |
| Databricks CLI | v0.230+ | Needed to fetch the notification destination ID, and for `databricks bundle deploy` after patching YAML. |
| Workspace permissions | Read on `jobs/list` for `inventory_jobs.py`; "Can Manage" on every job in scope (or workspace-admin) for `bulk_apply_webhooks.py` | `jobs/update` enforces "Can Manage" per-job. The inventory script only lists, so a regular user is enough. |
| Read access to bundle deployment metadata | For each bundle being inventoried (with `--enrich-bundles`, or any `bulk_apply_webhooks.py` run that encounters bundle jobs) | The script reads `/Workspace/Users/<owner>/.bundle/.../state/metadata.json` to enrich the CSV. Missing permission is non-fatal; affected rows just have empty bundle fields. |

---

## Install

```bash
git clone <this-repo-url>   # or copy the relevant scripts into a directory
cd bulk_apply_webhooks_dbx_jobs
pip install -r requirements.txt
```

Pick the scripts you actually need: `inventory_jobs.py` for read-only discovery, `bulk_apply_webhooks.py` for attaching/removing webhooks against directly-deployed jobs, `patch_bundle_yaml.py` for the bundle-YAML PR path, and `create_webhook_destination.py` if you want to create the destination from the same CLI flow. `requirements.txt` covers all four.

---

## Authentication

The script uses the standard Databricks SDK credential chain. Use whichever path your team already has set up:

**Personal access token via env vars:**
```bash
export DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
export DATABRICKS_TOKEN=<pat>
```

**CLI profile (`~/.databrickscfg`):**
```bash
export DATABRICKS_CONFIG_PROFILE=<profile-name>
# or pass --profile <name> on the command line
```

**OAuth (machine user or interactive):**
```bash
databricks auth login --host https://<workspace>.cloud.databricks.com
```

Verify auth before running anything else:
```bash
databricks current-user me
```

---

## Inventory a workspace first (`inventory_jobs.py`)

Before generating webhook IDs or touching jobs, get a clean picture of what's in the workspace. The inventory script is read-only and answers two questions in one pass:

1. How many jobs are here in total?
2. How many of those are DAB-deployed (need `patch_bundle_yaml.py` + a PR to the owning bundle) versus directly-deployed (the bulk script can touch them)?

```bash
# Fast path: just the counts. No per-bundle workspace download.
python3 inventory_jobs.py

# Full path: counts + bundle metadata enrichment. Adds one /Workspace download per
# unique bundle, populating bundle_name / target / git_origin / git_branch / git_commit
# in the CSV so you can hand it off to bundle owners directly.
python3 inventory_jobs.py --enrich-bundles
```

Summary on a fresh workspace looks like:

```
Workspace job inventory
  total:           184
  DAB-deployed:    71 (38.6%)
  directly-deployed: 113 (61.4%)

Top 10 creators (by job count):
      42  alice@example.com
      31  ci-bot@example.com
      ...

Top 10 bundles (by job count):
      18  data-platform
      14  analytics
      ...
```

The CSV (`jobs_inventory.csv` by default) has one row per job with `deployment_kind` (`BUNDLE` or `DIRECT`) in column 4, followed by the same bundle-metadata columns as `bundle_jobs.csv`. Useful filters:

```bash
# Just the DAB-deployed jobs — same set bulk_apply_webhooks.py would skip
awk -F, '$4=="BUNDLE"' jobs_inventory.csv

# Group bundle jobs by owning bundle + target
awk -F, 'NR>1 && $4=="BUNDLE" {print $5"\t"$6}' jobs_inventory.csv | sort | uniq -c | sort -rn

# Job IDs the bulk script would mutate
awk -F, 'NR>1 && $4=="DIRECT" {print $1}' jobs_inventory.csv
```

**CLI reference**

```
--profile <name>            Databricks CLI profile to use.
--tag key=value | key       Filter to jobs whose tags contain key=value
                            (or just key for presence-only).
--owner <email>             Filter by creator_user_name. Repeatable.
--output <path>             CSV output path. Default jobs_inventory.csv.
                            Pass '' to disable the CSV.
--enrich-bundles            Read each unique bundle's metadata.json from
                            /Workspace to populate bundle_name / target /
                            git_*. Off by default; adds one workspace API
                            call per unique bundle.
--top-n <N>                 How many creators / bundles to show in the
                            summary. Default 10.
--progress-every <N>        Log a tally every N scanned jobs. Default 500.
                            Set 0 to disable.
-v, --verbose               DEBUG-level logging.
```

**Required permissions**

The principal needs read access to `jobs/list`. To populate the bundle columns with `--enrich-bundles`, you additionally need read access to each bundle's `/Workspace/Users/<owner>/.bundle/.../state/metadata.json`. Missing read on a particular path is non-fatal: the row appears with empty bundle fields and a warning is logged.

---

## Get a webhook destination ID

The bulk script takes a destination **ID**, not a URL. You have two options for getting one.

### Option A — create one programmatically (`create_webhook_destination.py`)

Use this when you already have the receiving URL (Slack incoming-webhook, PagerDuty bridge, a `webhook.site` test endpoint, your team's HTTP listener, etc.) and want a scriptable, idempotent setup. The script wraps the `POST /api/2.0/notification-destinations` endpoint via the Databricks SDK.

**What it does**

- Validates the URL is `http(s)://` and warns if plain `http` (payloads would be unencrypted).
- Looks up existing destinations and checks for one with the same `display_name`. If found, prints its details and exits 0 — no mutation. This makes the script safe to run repeatedly in CI or setup pipelines.
- If no existing destination matches, calls `notification_destinations.create()` with a `Config(generic_webhook=...)` payload. The destination type is inferred from the populated sub-config — you don't need to specify it.
- Prints a human-readable summary on success: `id`, `display_name`, `type`, and `url`.
- Dry-run by default. Pass `--apply` to actually create.

**Quick start**

```bash
# Dry-run first — confirms auth, checks for a name collision, doesn't create anything
python3 create_webhook_destination.py \
    --url https://hooks.example.com/abc \
    --name my-team-webhook

# Apply
python3 create_webhook_destination.py \
    --url https://hooks.example.com/abc \
    --name my-team-webhook \
    --apply
```

Output on a fresh create looks like:
```
Created:
  id:           4c6145d0-1fbe-4ae0-b019-6f621361a04c
  display_name: my-team-webhook
  type:         WEBHOOK
  url:          https://hooks.example.com/abc
```

Re-running the same command is a no-op:
```
Destination already exists with display_name='my-team-webhook':
  id:           4c6145d0-1fbe-4ae0-b019-6f621361a04c
  ...
```

**CLI reference**

```
--url <url>             Required. Webhook URL the destination will POST to.
                        Must be http(s)://. Warns on plain http.
--name <name>           Required. Display name for the destination.
                        Used as the idempotency key — re-running with the
                        same name returns the existing destination.
--apply                 Actually create. Default: dry-run.
--profile <name>        Databricks CLI profile to use.
                        Falls back to the standard SDK credential chain.
-v, --verbose           DEBUG-level logging (SDK HTTP traces).
```

**Required permissions**

The principal you authenticate as needs **workspace-admin** to create notification destinations. A regular "Can Manage" job principal can attach an existing destination but cannot create new ones — you'll get a `PERMISSION_DENIED` on `--apply` if running unprivileged. The dry-run path only calls `list`, which most users can do.

**Caveats**

- **No URL update path.** This script only creates destinations. If you need to change the URL of an existing destination, update it in the UI or via `databricks notification-destinations update`. The script intentionally won't mutate existing destinations because that could silently redirect notifications mid-run.
- **No auth headers.** The script creates an unauthenticated `generic_webhook` destination. If your receiving endpoint requires bearer tokens or signing keys, configure them in the Admin Settings UI instead — the underlying API supports a `username`/`password` pair, but exposing those as flags risks them landing in shell history.
- **Display name uniqueness.** The Databricks API itself doesn't strictly enforce unique names, but this script relies on name as the idempotency key. If you already have duplicates, the script picks whichever the API returns first.

### Option B — create one manually

Create the destination in **Admin Settings → Notifications**, then look up its ID via the CLI:
```bash
# By display name, returning just the ID:
databricks notification-destinations list -o json \
  | jq -r '(.results? // .)[] | select(.display_name=="<your-destination-name>") | .id'
```

Either way, stash it in a variable for the rest of the session:
```bash
WEBHOOK_ID=$(databricks notification-destinations list -o json \
  | jq -r '(.results? // .)[] | select(.display_name=="<your-destination-name>") | .id')
```

---

## Quick start: inventory, then dry-run, then apply

The bulk script is **dry-run by default**. It only mutates when `--apply` is passed.

```bash
# 0. Size up the workspace before doing anything else. No mutation, no webhook ID needed.
#    Tells you how many jobs are DAB-deployed (need step 4) vs directly-deployed
#    (need step 3). Writes jobs_inventory.csv.
python3 inventory_jobs.py --enrich-bundles

# 1. Dry-run the full workspace against the bulk script. No changes.
python3 bulk_apply_webhooks.py --webhook-id "$WEBHOOK_ID"

# 2. Inventory bundle-managed jobs separately via the bulk script's --bundle-jobs only
#    pass. Produces bundle_jobs.csv (subset of jobs_inventory.csv's BUNDLE rows).
python3 bulk_apply_webhooks.py --webhook-id "$WEBHOOK_ID" --bundle-jobs only

# 3. Apply to all non-bundle jobs.
python3 bulk_apply_webhooks.py --webhook-id "$WEBHOOK_ID" --apply

# 4. For the bundle-managed jobs the bulk script skipped, patch each owning bundle's
#    YAML and open a PR — see "Companion script: patch_bundle_yaml.py" below.
```

For workspaces with many jobs, do a staged rollout first — see the next section.

---

## Removing webhooks (`--remove`)

If a rollout needs to be reversed — receiver is melting under load, destination was attached in error, etc. — use `--remove` mode. It comes in three shapes, from surgical to broad:

| Shape | When to use | Walk jobs/list? | `--webhook-id` |
|---|---|---|---|
| Per-job by ID | You already know the job IDs (one-off cleanups) | No, `jobs.get` per ID | Optional (omit → clear all webhooks on those jobs) |
| By job-ID list / CSV | Rolling back a known cohort, especially `jobs_inventory.csv` rows | No, `jobs.get` per ID | Optional (same semantics as per-job) |
| Workspace-walk by webhook ID | Incident-style rollback: "take this destination off every job that has it" | Yes, `jobs/list` paginated | **Required** |

### Shape 1 — per-job-id rollback

```bash
# Remove ALL webhook_notifications from specific jobs
python3 bulk_apply_webhooks.py --remove --job-id 1234 --job-id 5678 --apply

# Remove only one destination, leaving any other subscriptions intact
python3 bulk_apply_webhooks.py --remove --job-id 1234 --webhook-id "$WEBHOOK_ID" --apply

# Dry-run first (default) — confirms what would be removed before mutating
python3 bulk_apply_webhooks.py --remove --job-id 1234 --webhook-id "$WEBHOOK_ID"
```

### Shape 2 — rollback driven by a job-ID list (`--job-ids-from`)

`--job-ids-from <path>` reads job IDs from a text file or CSV — one ID per line, or the first column of a CSV. A header row is auto-detected (so `jobs_inventory.csv` and `bundle_jobs.csv` pipe in directly), and explicit `--job-id` flags merge with the file (de-duplicated).

```bash
# Roll back every directly-deployed job listed in jobs_inventory.csv
python3 bulk_apply_webhooks.py --remove \
    --webhook-id "$WEBHOOK_ID" \
    --job-ids-from <(awk -F, 'NR>1 && $4=="DIRECT" {print $1}' jobs_inventory.csv) \
    --apply

# Or feed bundle_jobs.csv directly (header auto-skipped)
python3 bulk_apply_webhooks.py --remove --webhook-id "$WEBHOOK_ID" \
    --job-ids-from bundle_jobs.csv

# Combine with --job-id (results merged, de-duplicated)
python3 bulk_apply_webhooks.py --remove --webhook-id "$WEBHOOK_ID" \
    --job-ids-from rollback.txt --job-id 99999
```

### Shape 3 — workspace-walk rollback (`--remove --webhook-id <id>` with no `--job-id`)

Mirrors add mode's main loop: paginate `jobs/list`, apply `--tag` / `--owner` / `--bundle-jobs` filters, and remove the named destination from every matching job that currently has it. Jobs without the destination are short-circuited with a DEBUG line — no `jobs.update` calls — so it's safe to run broadly.

```bash
# Workspace-wide: detach this destination from every job that has it. Dry-run.
python3 bulk_apply_webhooks.py --remove --webhook-id "$WEBHOOK_ID"

# Same, applied
python3 bulk_apply_webhooks.py --remove --webhook-id "$WEBHOOK_ID" --apply

# Scoped to a cohort by tag — common during a phased rollback
python3 bulk_apply_webhooks.py --remove --webhook-id "$WEBHOOK_ID" --tag team=platform --apply

# Limit the blast radius of the first apply
python3 bulk_apply_webhooks.py --remove --webhook-id "$WEBHOOK_ID" --limit 10 --apply
```

**Guardrail.** Workspace-walk mode **requires** `--webhook-id`. The "omit `--webhook-id` to clear all" shortcut from per-job mode does **not** extend to the workspace walk — clearing every webhook in the workspace from one CLI call would be too easy to misfire. `parse_args` rejects the combination explicitly. If you actually need to wipe every webhook from every job, do it via a per-job-ID list.

### Semantics common to all three shapes

- `--webhook-id` filter behavior when provided: only entries matching that ID are removed from every event list. Other webhooks on the same job survive.
- `--events` is ignored in remove mode — when a destination is removed, it's removed from every event it was subscribed to.
- Re-running with the same arguments is a no-op (logs `not currently attached`).
- Dry-run by default; pass `--apply` to write.
- Pacing + backoff identical to add mode (`--base-sleep`, `--jitter`, `--max-retries`).

**Bundle-managed jobs**

- Per-job-id / `--job-ids-from` rollback: bundle jobs proceed with a `WARNING` (the user pointed at the job explicitly, so we honor it). The API change is non-durable — the next `databricks bundle deploy` re-adds whatever the bundle YAML specifies.
- Workspace-walk rollback: bundle jobs follow `--bundle-jobs` (default **`skip`**), consistent with add mode and the non-durability concern above. They're still recorded into `bundle_jobs.csv` so you can patch the owning bundles separately. `--bundle-jobs=include` is the explicit opt-in for "take it off everywhere now, we'll patch bundles later."
- Durable bundle rollback in either case is to revert (or modify) the `patch_bundle_yaml.py`-generated PR — the bulk script only affects live job settings.

---

## Staged rollout with filters

`--tag` and `--owner` are evaluated client-side after the list call. Combine them with `--limit` for safety on the first apply.

```bash
# A specific team's jobs
python3 bulk_apply_webhooks.py --webhook-id "$WEBHOOK_ID" --tag team=platform --apply

# Tag presence only (any value)
python3 bulk_apply_webhooks.py --webhook-id "$WEBHOOK_ID" --tag rollout-cohort-1 --apply

# Specific owners (repeatable)
python3 bulk_apply_webhooks.py --webhook-id "$WEBHOOK_ID" \
    --owner alice@example.com --owner bob@example.com --apply

# Belt-and-suspenders: hard cap on number of updates
python3 bulk_apply_webhooks.py --webhook-id "$WEBHOOK_ID" \
    --tag webhook-rollout-pilot --limit 5 --apply
```

Recommended ramp:
1. Dry-run with the chosen filter to confirm match count.
2. Apply with `--limit 5` and verify in the UI.
3. Re-run without `--limit` to fan out.
4. Final pass with no filter to catch the long tail (idempotent — `already_attached` jobs are no-ops).

---

## Bundle-managed (Asset Bundle / DAB) jobs

This is the most important caveat.

Jobs deployed via `databricks bundle deploy` carry `settings.deployment.kind == BUNDLE`. The bundle's YAML in the source repo is the source of truth — any direct API edit (including the one this script would make) will be **silently overwritten on the next `bundle deploy`**. Worse: bundle deployments in `mode: production` set `edit_mode: UI_LOCKED`, but API edits still succeed, making the regression invisible until the next deploy.

To handle this, the script ships three policies via `--bundle-jobs`:

| Policy | Behavior |
|--------|----------|
| `skip` (default) | Bundle jobs are detected and recorded in `bundle_jobs.csv` but never mutated. Other jobs are processed normally. |
| `only` | Process only bundle jobs (useful for auditing — pair with dry-run). |
| `include` | Write through anyway. **Use only when you have already added the webhook to the bundle YAML and want a one-time fast-path before the next deploy.** Otherwise the change is non-durable. |

### The right way to add a webhook to a bundle-managed job

Use the inventory CSV (see next section) to find the owning bundles, then patch the YAML in each bundle's source repo. Use `patch_bundle_yaml.py` to do this automatically — see the next section.

The patched YAML will look like:

```yaml
# in resources/jobs.yml (or wherever the job resource is defined)
resources:
  jobs:
    my_job:
      name: my-job
      webhook_notifications:
        on_failure:
          - id: <WEBHOOK_DESTINATION_ID>
        on_duration_warning_threshold_exceeded:
          - id: <WEBHOOK_DESTINATION_ID>
      tasks: [...]
```

Then open a PR. After merge + CI redeploy (`databricks bundle deploy`), the webhook is wired up durably.

---

## Companion script: `patch_bundle_yaml.py`

Automates the YAML edit above. Runs locally on a checked-out bundle repo, produces a `git diff` you can open as a PR.

### What it does

- Reads `databricks.yml` plus every file matched by its `include:` glob patterns.
- Finds every base `resources.jobs.<name>` block and merges the supplied webhook ID into each event list under `webhook_notifications`, inserting the block just before `tasks:` for review-friendly diffs.
- Per-target overrides (`targets.<env>.resources.jobs.<name>`) are **detected but never written to**. DAB deep-merge concatenates `webhook_notifications` event lists at deploy time, so the base patch propagates into every target automatically. Writing the override too would produce a duplicate that Databricks rejects with `cannot update job: Duplicate webhook ids ...` at deploy.
- Skips any event list that already contains a `${var.*}` reference with a WARNING — the patcher can't resolve variables and could otherwise create a deploy-time duplicate when the variable resolves to the same destination.
- Logs a WARNING when a per-target override already contains the same `--webhook-id` (the override + base concat would still produce a duplicate; hand-edit needed before deploy).
- Preserves comments, key order, anchors, and quoting via `ruamel.yaml` round-trip.
- Idempotent: re-running with the same webhook ID is a no-op.
- Dry-run by default (prints unified diff); `--apply` writes files in place.

### Quick start

```bash
# Clone the owning bundle repo and branch off
git clone <bundle-repo> && cd <bundle-repo>
git checkout -b add-webhook-notifications

# Dry-run from a sibling directory containing the scripts:
python3 /path/to/patch_bundle_yaml.py --bundle-dir . --webhook-id "$WEBHOOK_ID"

# Apply
python3 /path/to/patch_bundle_yaml.py --bundle-dir . --webhook-id "$WEBHOOK_ID" --apply

# Validate that the YAML still parses
databricks bundle validate

# Review with git, then PR
git diff
git add -p && git commit -m "Add webhook_notifications to all bundle jobs"
git push -u origin add-webhook-notifications
```

The owning team reviews the PR; their CI runs `databricks bundle deploy` on merge and the webhook becomes durable.

### CLI reference

```
--bundle-dir <path>       Path to bundle root (contains databricks.yml). Default: cwd.
--webhook-id <id>         Required. Webhook destination ID to attach.
--events on_failure,on_duration_warning_threshold_exceeded
                          Comma-separated event list. Same valid values as the
                          bulk script. Default is intentionally low-noise; pass
                          on_start/on_success explicitly to also notify on those.
--job <name>              Limit to jobs whose `name:` field matches. Repeatable.
--tag key=value | key     Filter by the job's YAML `tags:` block.
--apply                   Write files in place. Default: dry-run diff to stdout.
-v, --verbose             DEBUG-level logging (logs "already has webhook" hits).

Per-target overrides are always skipped (DAB deep-merge propagates the base
patch into every target via list concatenation). There is no flag to write
through to overrides — doing so would produce duplicates that Databricks
rejects at deploy.
```

### Caveats worth flagging to bundle owners

- **Templated webhook IDs**: the patcher writes literal IDs and can't resolve `${var.*}` references. To avoid producing a duplicate that Databricks rejects at deploy time (`cannot update job: Duplicate webhook ids ...`), the patcher now **skips any event list whose existing entries include a `${var.*}` reference** and logs a WARNING. Hand-edit the affected files if you need both. See the worked example below.
- **Anchors and `<<:` merges**: ruamel preserves anchors on round-trip, but if jobs share configuration via `<<: *base`, you may want to put the webhook block on the base anchor rather than each job. Review the diff carefully on those repos. See the worked example below.
- **No `bundle validate` built in**: run `databricks bundle validate` after `--apply` and before opening the PR — it'll catch any structural issue (rare, but cheap insurance).
- **Per-target overrides**: the patcher never writes to `targets.<env>.resources.jobs.<name>` blocks. DAB deep-merge **concatenates** `webhook_notifications` event lists at deploy time, so the base patch alone propagates into every target automatically. Patching the override too would produce `[base_webhook, override_webhook]` lists with duplicates that Databricks rejects (`cannot update job: Duplicate webhook ids ...`). If an override already contains the same `--webhook-id`, the patcher logs a WARNING — the resulting concat would still produce a duplicate, so the override needs a hand-edit before `bundle deploy`. See the worked example below.

### Anchors and `<<:` merges: worked example

Some bundles DRY up job configuration with a YAML anchor (`&name`) and merge key (`<<: *name`) so multiple jobs share defaults from one block. The patcher doesn't follow merge keys — it walks `resources.jobs.<name>` directly and patches each consumer job individually. That's correct but produces N copies of the same `webhook_notifications` block when there are N jobs sharing an anchor.

**Starting state**

```yaml
# resources/jobs/etl.yml
definitions:
  job_defaults: &defaults
    tags:
      team: data-platform
    email_notifications:
      on_failure:
        - alerts@example.com
    timeout_seconds: 3600

resources:
  jobs:
    extract_job:
      <<: *defaults
      name: extract
      tasks: [...]
    transform_job:
      <<: *defaults
      name: transform
      tasks: [...]
    load_job:
      <<: *defaults
      name: load
      tasks: [...]
```

Three jobs, one anchor.

**What the patcher does by default**

```bash
python3 patch_bundle_yaml.py --bundle-dir . --webhook-id WID --apply
```

It writes `webhook_notifications` into each consumer job:

```yaml
resources:
  jobs:
    extract_job:
      <<: *defaults
      name: extract
      webhook_notifications:    # <-- added per-job
        on_failure:
          - id: WID
        on_duration_warning_threshold_exceeded:
          - id: WID
      tasks: [...]
    transform_job:
      <<: *defaults
      name: transform
      webhook_notifications:    # <-- duplicated
        on_failure:
          - id: WID
        on_duration_warning_threshold_exceeded:
          - id: WID
      tasks: [...]
    load_job:
      <<: *defaults
      name: load
      webhook_notifications:    # <-- duplicated again
        on_failure:
          - id: WID
        on_duration_warning_threshold_exceeded:
          - id: WID
      tasks: [...]
```

Functionally correct, but 3× the diff. Adding/removing a destination later means three edits, not one.

**What a hand-edit on the anchor looks like**

Move the webhook block onto `&defaults` once and revert the per-job patches:

```yaml
definitions:
  job_defaults: &defaults
    tags:
      team: data-platform
    email_notifications:
      on_failure:
        - alerts@example.com
    timeout_seconds: 3600
    webhook_notifications:        # <-- added once on the anchor
      on_failure:
        - id: WID
      on_duration_warning_threshold_exceeded:
        - id: WID

resources:
  jobs:
    extract_job:
      <<: *defaults
      name: extract
      tasks: [...]
    transform_job:
      <<: *defaults
      name: transform
      tasks: [...]
    load_job:
      <<: *defaults
      name: load
      tasks: [...]
```

One block, three jobs. All consumers inherit `webhook_notifications` via the merge at deploy time. Future changes touch one place.

**Why the patcher doesn't do this for you**

- The anchor source isn't necessarily under `resources.jobs` — it might live in `definitions:` (as above), in a separate include file, or as a discarded `.base` key. The patcher only walks `resources.jobs.<name>`.
- Anchors are often consumed by non-job resources too (pipelines, schedules). Patching the anchor blind could touch resources the patcher shouldn't.
- Mutating through merge resolution while iterating consumers is fragile — exactly the kind of round-trip subtlety the script avoids.

**How to spot anchors in a bundle**

```bash
grep -REn '<<:\s*\*|&[A-Za-z_]' path/to/bundle
```

Reveals every anchor definition (`&name`) and merge consumer (`<<: *name`). If you see the same anchor consumed by several jobs, consider doing the hand-edit instead of (or after reverting) the per-job patch.

**Tl;dr:** patcher output is correct but verbose for anchor-heavy repos. Review the diff; for bundles where many jobs share a `<<: *base`, the cleaner long-term shape is one block on the anchor. The patcher's per-job writes are a safe default, not always the best one.

### Per-target overrides: worked example

DAB deep-merge **concatenates** `webhook_notifications` event lists at deploy time. The patcher relies on this: it only ever writes to base `resources.jobs.<name>` blocks, and the merge fans the webhook out to every target automatically. There's no flag — overrides are always skipped, and the patcher logs a WARNING in the one case where you'd hit a deploy-rejecting duplicate.

**Starting state** (the `analytics_job` shape from `examples/caveats/`):

```yaml
# resources/analytics.yml — base
resources:
  jobs:
    analytics_job:
      name: analytics
      tags: { team: analytics }
      tasks: [...]

# databricks.yml — prod target adds a prod-only failure destination
targets:
  prod:
    mode: production
    resources:
      jobs:
        analytics_job:
          name: analytics-prod
          webhook_notifications:
            on_failure:
              - id: prod-only-pagerduty-uuid
          timeout_seconds: 7200
```

Run the patcher:

```bash
python3 patch_bundle_yaml.py --bundle-dir examples/caveats --webhook-id WID --apply
```

**Log output**

```
INFO   resources/analytics.yml :: resources.jobs.analytics_job -> patched
INFO   databricks.yml :: targets.prod.resources.jobs.analytics_job -> skipped (target override; DAB merge propagates base patch)
```

**Patched base file**

```yaml
# resources/analytics.yml — webhook_notifications written to the base only
resources:
  jobs:
    analytics_job:
      name: analytics
      tags: { team: analytics }
      webhook_notifications:       # <-- added by patcher
        on_failure:
          - id: WID
        on_duration_warning_threshold_exceeded:
          - id: WID
      tasks: [...]
```

The prod override in `databricks.yml` is untouched.

**What ships at `bundle deploy`**

DAB concatenates per-event for each target:

| Event | base list (from analytics.yml) | prod override (from databricks.yml) | result in prod |
|---|---|---|---|
| `on_failure` | `[WID]` | `[prod-only-pagerduty-uuid]` | `[WID, prod-only-pagerduty-uuid]` |
| `on_duration_warning_threshold_exceeded` | `[WID]` | _(none)_ | `[WID]` |

No duplicates. The rollout webhook reaches every target, and prod keeps its specialty failure destination.

#### The one case that needs a hand-edit

If an override already contains the same `--webhook-id` the patcher would add to the base, DAB concat would produce `[WID, ..., WID]` and Databricks rejects the deploy. The patcher detects this and warns:

```yaml
# Hypothetical: prod override already lists WID alongside its specialty destination
targets:
  prod:
    resources:
      jobs:
        analytics_job:
          webhook_notifications:
            on_failure:
              - id: WID
              - id: prod-only-pagerduty-uuid
```

```
WARNING  databricks.yml :: targets.prod.resources.jobs.analytics_job -> override already contains webhook WID on events ['on_failure']. DAB merge will concatenate base + override at deploy, producing duplicates that Databricks rejects. Hand-edit the override to remove the redundant entries before `bundle deploy`.
```

Fix by hand: drop the `- id: WID` line from the override. The base patch supplies `WID` to every target via concat anyway.

**Tl;dr**: patcher writes base only, DAB merge fans the webhook out to every target, override-specific destinations survive untouched. The only WARNING path is an override that already lists the same ID the patcher is adding to base.

### Templated webhook IDs: worked example

Bundles often indirect the destination ID through a variable so one ID lives in `databricks.yml` and every job references it via `${var.webhook_id}`. The patcher walks each `webhook_notifications` list literally — it doesn't resolve variables, doesn't know what `${var.webhook_id}` will be at deploy time, and **could produce a duplicate that Databricks rejects with `cannot update job: Duplicate webhook ids ...`**. To prevent that, the patcher now skips any event list that already contains a `${var.*}` entry and logs a WARNING.

**Starting state**

```yaml
# databricks.yml
variables:
  webhook_id:
    default: "4c6145d0-1fbe-4ae0-b019-6f621361a04c"

# resources/reporting.yml
resources:
  jobs:
    reporting_job:
      name: reporting
      webhook_notifications:
        on_failure:
          - id: ${var.webhook_id}
        on_duration_warning_threshold_exceeded:
          - id: ${var.webhook_id}
      tasks: [...]
```

**What happens now**

```bash
python3 patch_bundle_yaml.py --bundle-dir . --webhook-id 4c6145d0-1fbe-4ae0-b019-6f621361a04c --apply
```

```
WARNING  resources/reporting.yml :: resources.jobs.reporting_job -> events ['on_failure', 'on_duration_warning_threshold_exceeded'] skipped: existing ${var.*} reference. Patcher writes literal IDs; if the variable resolves to the same destination, Databricks rejects the deploy with 'Duplicate webhook ids'. Hand-edit if needed.
Done. mode=APPLY files_changed=0 jobs_seen=1 jobs_matched=1 jobs_patched=0 overrides_skipped=0 var_skipped_events=2
```

`reporting.yml` is left alone — the `${var.webhook_id}` references stay, no literal is appended. Bundle deploys cleanly.

**Previous behavior (before this skip-with-WARNING rule)**

The patcher would append the literal alongside the variable:

```yaml
on_failure:
  - id: ${var.webhook_id}
  - id: 4c6145d0-1fbe-4ae0-b019-6f621361a04c
```

Then `databricks bundle deploy` would fail terraform apply:

```
Error: cannot update job: Duplicate webhook ids '4c6145d0-1fbe-4ae0-b019-6f621361a04c' found for on_failure
```

…because the variable resolves to the same ID. This was the templated-ID caveat firing — now headed off at the patcher.

**When you actually do want both**

If you're intentionally layering destinations (e.g. variable resolves to one team's channel and you want to also notify a second team via a literal), the skip is in your way. Two clean options:

1. Hand-edit the affected event list to add the literal entry. The patcher's idempotency check will leave it alone on re-runs (the literal is already present; the variable is still skipped).
2. Remove the `${var.*}` reference first if you want the patcher to manage that event list going forward.

The patcher won't second-guess either choice; it just refuses to mix variables and literals automatically.

**Detection without running the patcher**

```bash
grep -REn '\$\{var\.' path/to/bundle
```

Lists every variable reference in the bundle. If any sit inside a `webhook_notifications` block, expect the patcher to skip those event lists.

---

## Output: `bundle_jobs.csv`

Written at the end of any run that encountered bundle jobs (unless `--bundle-report ''` is passed). Columns:

| Column | Source |
|--------|--------|
| `job_id` | Top-level `job_id`. |
| `name` | `settings.name` (with `[dev <user>]` prefix for `mode: development` deploys). |
| `creator` | `creator_user_name`. |
| `bundle_name` | `bundle.name` from the deployment metadata JSON. |
| `target` | `bundle.target` (e.g. `dev`, `prod`). Tells YAML editors which `targets:` block to patch. |
| `git_origin` | `bundle.git.origin_url` — the repo URL bundle owners should PR against. |
| `git_branch` | `bundle.git.branch`. |
| `git_commit` | `bundle.git.commit` at deploy time. |
| `workspace_root` | `workspace.root_path` — where the bundle is deployed in the workspace. |
| `workspace_file_path` | `workspace.file_path`. |
| `metadata_file_path` | Workspace path to the JSON the script read. |

If the script couldn't read a bundle's metadata (e.g. ACL on the workspace path), bundle-specific columns will be empty for that row but the job_id/name still appear. A warning is logged.

---

## CLI reference

```
--webhook-id <id>          Notification destination ID. Required in add mode.
                           In per-job --remove (with --job-id / --job-ids-from):
                             provide to remove only that destination;
                             omit to clear ALL webhooks on those jobs.
                           In workspace-walk --remove (no --job-id):
                             REQUIRED — guardrail against clearing every
                             webhook in the workspace from one CLI call.
--events on_failure,on_duration_warning_threshold_exceeded
                           Comma-separated event list (add mode only). Valid:
                           on_start, on_success, on_failure,
                           on_duration_warning_threshold_exceeded.
                           Default: on_failure,on_duration_warning_threshold_exceeded
                           (chosen to stay low-noise; add on_start/on_success
                           explicitly if you also want lifecycle events).

--remove                   Switch to remove mode. Three shapes — see
                           "Removing webhooks (--remove)" above.
--job-id <id>              Job ID to operate on. Repeatable. Use with --remove
                           for per-job rollback. Invalid in add mode.
                           Mutually exclusive with --tag/--owner in remove mode.
--job-ids-from <path>      Path to a text or CSV file with job IDs (first
                           column). Header row auto-detected. Combines with
                           --job-id (de-duplicated). Use with --remove.

--tag key=value | key      Filter to jobs whose tags contain key=value
                           (or just key for presence-only). Used in add mode
                           and in workspace-walk --remove.
--owner <email>            Filter by creator_user_name. Repeatable.
                           Used in add mode and in workspace-walk --remove.

--bundle-jobs {skip,include,only}
                           Policy for jobs with deployment.kind=BUNDLE.
                           Default: skip. Honored in add mode and in
                           workspace-walk --remove. In per-job --remove
                           (explicit --job-id / --job-ids-from), bundle jobs
                           proceed with a WARNING regardless of this flag —
                           the user pointed at them explicitly.
--bundle-report <path>     CSV output path for bundle jobs encountered.
                           Default: bundle_jobs.csv. Pass '' to disable.

--apply                    Actually call jobs/update. Default is dry-run.
--limit <N>                Stop after N updates (matched + would_update).
                           Add mode and workspace-walk --remove.
--progress-every <N>       Log a tally every N scanned jobs. Default 500.
                           Set 0 to disable. Add mode and workspace-walk
                           --remove.

--profile <name>           Databricks CLI profile to use.
--max-retries <N>          Max retries on 429/5xx per call. Default 5.
--base-sleep <s>           Base sleep between updates. Default 0.3.
--jitter <s>               Max random jitter added per update. Default 0.4.
-v, --verbose              DEBUG-level logging (includes SDK HTTP traces).
```

---

## Rate-limit handling

- The script paces updates with `base_sleep + uniform(0, jitter)` between calls.
- Transient failures (HTTP 429, 5xx, `RATE_LIMIT_EXCEEDED`) are retried with exponential backoff and added jitter, up to `--max-retries` attempts.
- Non-transient errors (4xx other than 429) are not retried — the job is recorded as `errored`, and the loop continues.
- A non-zero `errored` count causes a non-zero process exit.

For very large workspaces, consider running with `--limit 200 --base-sleep 1.0` in chunks rather than blasting all jobs at once.

---

## Idempotency

Re-running the script with the same `--webhook-id` is safe. Jobs that already have the webhook on every target event are detected and counted under `already_attached` without an API call. This makes it natural to:

- Re-run after creating a few new jobs to "fill in" the latecomers.
- Re-run with broader filters after a staged rollout.

---

## Troubleshooting

**Auth error: `default auth: cannot configure default credentials`**
The SDK couldn't find credentials. Set `DATABRICKS_HOST` + `DATABRICKS_TOKEN`, or `DATABRICKS_CONFIG_PROFILE`, or run `databricks auth login`.

**`PERMISSION_DENIED` on `jobs/update` for some jobs**
The principal you authenticated as lacks "Can Manage" on those jobs. Either run as a workspace admin, or split the run by `--owner` so each owner runs against their own jobs.

**`bundle_total > 0` but bundle columns are empty in the CSV**
The script couldn't read the workspace metadata file (ACL or path no longer exists). Affects enrichment only — detection still works. Grant read on `/Workspace/Users/<owner>/.bundle/` for full coverage, or accept the empty fields.

**Output looks frozen mid-run**
The Jobs `list` API has no server-side tag filter, so even targeted runs page through every job. Use `--progress-every 250` to get steadier ticks, and `--limit N` to short-circuit once N matches have been processed.

**Run reports `errored=N`**
Inspect the log for `update failed:` lines. Most commonly: rate-limit exhaustion (raise `--max-retries` and `--base-sleep`) or per-job permission. Re-running is safe — already-applied jobs are no-ops.

**`create_webhook_destination.py --apply` fails with `PERMISSION_DENIED`**
Creating notification destinations requires workspace-admin. Either run the script as an admin, or have an admin run it once and share the resulting ID with the rest of the team via the same `WEBHOOK_ID` variable.

**`create_webhook_destination.py` reports "already exists" but you don't see it in the UI**
The display-name lookup is API-driven and case-sensitive. Check `databricks notification-destinations list` to see exactly what's there; the destination may exist under a slightly different name.

---

## Example bundles (reference)

The `examples/` directory contains three reference Asset Bundles you can use to validate both scripts end-to-end:

- **`examples/simple/`** — single-job bundle, single target. Good for the smoke-test workflow below.
- **`examples/complex/`** — multi-file bundle exercising variables, `include:` globs, YAML anchors/aliases, per-target overrides, and a mix of job and pipeline resources. Useful for stress-testing the patcher.
- **`examples/caveats/`** — purpose-built to hit every footgun in the "Caveats worth flagging to bundle owners" list in one place: a `${var.webhook_id}` reference, a `<<: *defaults` anchor shared across three jobs, and a per-target override that defines its own `webhook_notifications`. Use this when you want to see exactly how the patcher behaves on each caveat. Walkthrough below.

### Caveats-bundle workflow

```bash
# 1. Confirm the bundle parses cleanly
cd examples/caveats && databricks bundle validate && cd ../..

# 2. Deploy a clean baseline to dev (and optionally prod) so you can
#    compare workspace state before vs after the patcher run.
cd examples/caveats && databricks bundle deploy && cd ../..
cd examples/caveats && databricks bundle deploy -t prod && cd ../..   # optional

# 3. Dry-run the patcher. Expected output:
#    - resources/etl.yml :: extract_job -> patched (anchor target; the
#      <<: *defaults consumers inherit it via the merge, no per-job duplication).
#    - resources/analytics.yml :: analytics_job -> patched
#    - resources/reporting.yml :: WARNING for both default events skipped
#      (existing ${var.webhook_id} references on on_failure and
#      on_duration_warning_threshold_exceeded; patcher refuses to mix
#      literals + variables).
#    - databricks.yml :: targets.prod.resources.jobs.analytics_job ->
#      INFO "skipped (target override; DAB merge propagates base patch)".
python3 patch_bundle_yaml.py --bundle-dir examples/caveats --webhook-id WID-LITERAL-12345

# 4. Apply, then redeploy to both targets. Both succeed — DAB concat
#    yields [WID, prod-only-pagerduty-uuid] on prod's on_failure, with
#    no duplicates.
python3 patch_bundle_yaml.py --bundle-dir examples/caveats --webhook-id WID-LITERAL-12345 --apply
cd examples/caveats && databricks bundle deploy && cd ../..
cd examples/caveats && databricks bundle deploy -t prod && cd ../..
```

Full smoke-test workflow against the simple bundle:

```bash
# 1. Deploy the example bundle
cd examples/simple
databricks bundle validate
databricks bundle deploy
cd ../..

# 2. Confirm the bulk script detects the deployed job as bundle-managed.
#    Look for bundle_total=1 in the summary and inspect bundle_jobs.csv.
python3 bulk_apply_webhooks.py --webhook-id "$WEBHOOK_ID" \
    --tag webhook-test=simple --bundle-jobs only

# 3. Confirm the default skip behavior leaves bundle jobs untouched even
#    with --apply. Look for `SKIP bundle-managed` and updated=0.
python3 bulk_apply_webhooks.py --webhook-id "$WEBHOOK_ID" \
    --tag webhook-test=simple --apply

# 4. Patch the YAML via the companion script (dry-run first).
python3 patch_bundle_yaml.py --bundle-dir examples/simple --webhook-id "$WEBHOOK_ID"
python3 patch_bundle_yaml.py --bundle-dir examples/simple --webhook-id "$WEBHOOK_ID" --apply

# 5. Redeploy and verify the webhook is now attached durably.
cd examples/simple && databricks bundle deploy && cd ../..

# 6. From the workspace UI, Run now on the test job and confirm the POST
#    arrives at your webhook capture endpoint.
```

Use this as a template for setting up similar end-to-end smoke tests in your own environments.
