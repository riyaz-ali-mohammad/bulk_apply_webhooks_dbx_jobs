# Bulk Apply Webhook Notifications to Databricks Jobs

Three Python scripts for rolling out a webhook-based Notification Destination across every job in a Databricks workspace:

1. **`create_webhook_destination.py`** — one-shot setup. Creates a generic-webhook Notification Destination from a URL, so you can skip the Admin Settings UI. Idempotent on display name.
2. **`bulk_apply_webhooks.py`** — workspace-side script. Walks every job via the Jobs API, attaches the webhook to manually-created jobs, and inventories bundle-managed jobs for hand-off (because API edits to bundle jobs are non-durable).
3. **`patch_bundle_yaml.py`** — companion script. Runs on a local checkout of a bundle repo, edits the bundle YAML in place to add `webhook_notifications` to job resources, producing a review-ready git diff. This is the durable fix for bundle-managed jobs.

All three scripts default to dry-run and are idempotent. The bulk and patch scripts also support tag/owner filters for staged rollout.

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
- For each job, computes the desired `webhook_notifications` block by merging the supplied webhook ID into the configured event lists (defaults: `on_failure`, `on_success`, `on_start`). Existing webhooks are preserved.
- Calls `POST /api/2.2/jobs/update` to apply the change.
- Skips bundle-managed jobs by default (because `databricks bundle deploy` would overwrite API edits), and emits an inventory CSV of those jobs for hand-off to bundle owners.
- Honors rate limits with exponential backoff plus jitter, and paces calls between updates.

**Remove mode (`--remove`)**

- Takes explicit `--job-id` arguments — no listing or filtering.
- Removes either a specific webhook destination (when `--webhook-id` is given) or every webhook entry (when it isn't).
- Same dry-run / `--apply` / backoff semantics. Logs a WARNING on bundle-managed jobs because the removal is non-durable across `bundle deploy`.

---

## Prerequisites

| Tool | Required version | Why |
|------|------------------|-----|
| Python | 3.9+ | Runtime for the script. |
| `databricks-sdk` | >= 0.30.0 | API client used by `bulk_apply_webhooks.py`. Installed via `requirements.txt`. |
| `ruamel.yaml` | >= 0.17.0 | Round-trip YAML reader/writer used by `patch_bundle_yaml.py`. Installed via `requirements.txt`. |
| Databricks CLI | v0.230+ | Needed to fetch the notification destination ID, and for `databricks bundle deploy` after patching YAML. |
| Workspace permissions | "Can Manage" on every job in scope, or workspace-admin | `jobs/update` enforces this per-job. |
| Read access to bundle deployment metadata | For each bundle being inventoried | The script reads `/Workspace/Users/<owner>/.bundle/.../state/metadata.json` to enrich the CSV. Missing permission is non-fatal; affected rows just have empty bundle fields. |

---

## Install

```bash
git clone <this-repo-url>   # or copy the two files into a directory
cd bulk_apply_webhooks_dbx_jobs
pip install -r requirements.txt
```

The two files needed at minimum are `bulk_apply_webhooks.py` and `requirements.txt`.

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
  | jq -r '(.results // .) | .[] | select(.display_name=="<your-destination-name>") | .id'
```

Either way, stash it in a variable for the rest of the session:
```bash
WEBHOOK_ID=$(databricks notification-destinations list -o json \
  | jq -r '(.results // .) | .[] | select(.display_name=="<your-destination-name>") | .id')
```

---

## Quick start: dry-run, then apply

The script is **dry-run by default**. It only mutates when `--apply` is passed.

```bash
# 1. Dry-run the full workspace. No changes.
python3 bulk_apply_webhooks.py --webhook-id "$WEBHOOK_ID"

# 2. Inventory bundle-managed jobs separately. Produces bundle_jobs.csv.
python3 bulk_apply_webhooks.py --webhook-id "$WEBHOOK_ID" --bundle-jobs only

# 3. Apply to all non-bundle jobs.
python3 bulk_apply_webhooks.py --webhook-id "$WEBHOOK_ID" --apply
```

For workspaces with many jobs, do a staged rollout first — see the next section.

---

## Removing webhooks (`--remove`)

If the rollout needs to be reversed — say the receiver is melting under load, or a destination was attached in error — use `--remove` mode. It takes explicit job IDs rather than walking the whole workspace, so it's surgical and fast.

```bash
# Remove ALL webhook_notifications from specific jobs
python3 bulk_apply_webhooks.py --remove --job-id 1234 --job-id 5678 --apply

# Remove only one destination, leaving any other subscriptions intact
python3 bulk_apply_webhooks.py --remove --job-id 1234 --webhook-id "$WEBHOOK_ID" --apply

# Dry-run first (default) — confirms what would be removed before mutating
python3 bulk_apply_webhooks.py --remove --job-id 1234 --webhook-id "$WEBHOOK_ID"
```

**Semantics**

- `--job-id` is required and repeatable; the script looks up each job directly (`jobs.get`), no full list pagination.
- `--webhook-id` is **optional** in remove mode:
  - Provided → only entries matching that destination ID are removed from every event list. Other webhooks on the same job survive.
  - Omitted → every event list is cleared (`on_start`, `on_success`, `on_failure`, `on_duration_warning_threshold_exceeded`). Use with caution.
- `--events` is ignored in remove mode — when a destination is removed, it's removed from every event it was subscribed to.
- Re-running with the same arguments is a no-op (logs `not currently attached`).
- Dry-run by default; pass `--apply` to write.

**Bundle-managed jobs**

`--remove` proceeds against bundle-managed jobs but logs a `WARNING`. The API change is non-durable: the next `databricks bundle deploy` will re-add whatever the bundle YAML specifies. To remove a webhook durably from a bundle job, either drop the corresponding `webhook_notifications` entry from the bundle YAML and redeploy, or temporarily empty the block in YAML. The bulk script can't do that — it only affects the live job settings.

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
        on_success:
          - id: <WEBHOOK_DESTINATION_ID>
        on_start:
          - id: <WEBHOOK_DESTINATION_ID>
      tasks: [...]
```

Then open a PR. After merge + CI redeploy (`databricks bundle deploy`), the webhook is wired up durably.

---

## Companion script: `patch_bundle_yaml.py`

Automates the YAML edit above. Runs locally on a checked-out bundle repo, produces a `git diff` you can open as a PR.

### What it does

- Reads `databricks.yml` plus every file matched by its `include:` glob patterns.
- Finds every `resources.jobs.<name>` block (top-level and per-target `targets.<env>.resources.jobs.<name>`).
- Merges the supplied webhook ID into each event list under `webhook_notifications`, inserting the block just before `tasks:` for review-friendly diffs.
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
--events on_failure,on_success,on_start
                          Comma-separated event list. Same valid values as the
                          bulk script.
--job <name>              Limit to jobs whose `name:` field matches. Repeatable.
--tag key=value | key     Filter by the job's YAML `tags:` block.
--skip-target-overrides   Skip `targets.<env>.resources.jobs.<name>` blocks.
                          Tighter diffs; relies on bundle deep-merge to push
                          the webhook config into each target.
--apply                   Write files in place. Default: dry-run diff to stdout.
-v, --verbose             DEBUG-level logging (logs "already has webhook" hits).
```

### Caveats worth flagging to bundle owners

- **Templated webhook IDs**: bundles using `${var.webhook_id}` should add the variable definition manually instead of using this script. The script writes literal IDs.
- **Anchors and `<<:` merges**: ruamel preserves anchors on round-trip, but if jobs share configuration via `<<: *base`, you may want to put the webhook block on the base anchor rather than each job. Review the diff carefully on those repos. See the worked example below.
- **No `bundle validate` built in**: run `databricks bundle validate` after `--apply` and before opening the PR — it'll catch any structural issue (rare, but cheap insurance).
- **Per-target overrides**: by default, the script patches both base job definitions and per-target overrides (`targets.<env>.resources.jobs.<name>`). This is explicit but produces extra diff lines. Pass `--skip-target-overrides` to patch only base definitions and rely on bundle deep-merge to propagate webhook config into each target. When that flag is set, the script logs a WARNING for any override that already has its own `webhook_notifications` block — bundle merge would let the override's list win over the base, so those need a manual patch. See the worked example below for what each of the three cases produces.

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
        on_success:
          - id: WID
        on_start:
          - id: WID
      tasks: [...]
    transform_job:
      <<: *defaults
      name: transform
      webhook_notifications:    # <-- duplicated
        on_failure:
          - id: WID
        on_success:
          - id: WID
        on_start:
          - id: WID
      tasks: [...]
    load_job:
      <<: *defaults
      name: load
      webhook_notifications:    # <-- duplicated again
        on_failure:
          - id: WID
        on_success:
          - id: WID
        on_start:
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
      on_success:
        - id: WID
      on_start:
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

Using the `examples/complex/` bundle in this repo as a starting point — it has an `analytics_job` defined in the base and partially overridden in the `prod` target:

```yaml
# examples/complex/resources/jobs/analytics.yml  (base)
resources:
  jobs:
    analytics_job:
      name: analytics
      tags:
        team: analytics
      tasks:
        - task_key: run_report
          notebook_task:
            notebook_path: ../../notebooks/common.py

# examples/complex/databricks.yml  (per-target override)
targets:
  prod:
    mode: production
    resources:
      jobs:
        analytics_job:
          name: analytics-prod
          timeout_seconds: 7200
```

The override touches only `name` and `timeout_seconds`. No `webhook_notifications` of its own.

#### Case 1 — default behavior: patch base AND override

```bash
python3 patch_bundle_yaml.py --bundle-dir examples/complex --webhook-id WID --apply
```

Both blocks get the same `webhook_notifications` written into them:

```yaml
# resources/jobs/analytics.yml — base, patched
analytics_job:
  name: analytics
  tags:
    team: analytics
  webhook_notifications:        # <-- added
    on_failure:
      - id: WID
    on_success:
      - id: WID
    on_start:
      - id: WID
  tasks: [...]

# databricks.yml — override ALSO patched
targets:
  prod:
    resources:
      jobs:
        analytics_job:
          name: analytics-prod
          webhook_notifications:    # <-- redundantly added
            on_failure:
              - id: WID
            on_success:
              - id: WID
            on_start:
              - id: WID
          timeout_seconds: 7200
```

The override patch is redundant — at deploy time, bundle deep-merge would already have propagated the base's `webhook_notifications` into prod. But the diff is explicit, which some reviewers prefer.

#### Case 2 — `--skip-target-overrides`, override has no own webhook block (INFO log)

```bash
python3 patch_bundle_yaml.py --bundle-dir examples/complex --webhook-id WID --skip-target-overrides --apply
```

```
INFO  databricks.yml :: targets.prod.resources.jobs.analytics_job -> skipped (target override)
```

Only the base file is patched. `databricks.yml` is untouched. At deploy time, bundle deep-merge combines base + override → prod ends up with `name: analytics-prod`, `timeout_seconds: 7200`, **and** `webhook_notifications: {...}` from the base. Same operational result as Case 1, half the diff lines. This is the happy path for `--skip-target-overrides`.

#### Case 3 — `--skip-target-overrides`, override HAS its own webhook block (WARNING log)

Imagine prod already wired up a different destination (e.g. a PagerDuty-only one):

```yaml
# databricks.yml — override has its own webhook_notifications
targets:
  prod:
    resources:
      jobs:
        analytics_job:
          name: analytics-prod
          webhook_notifications:
            on_failure:
              - id: prod-pagerduty-destination-uuid
          timeout_seconds: 7200
```

Running with `--skip-target-overrides`:

```
WARNING  databricks.yml :: targets.prod.resources.jobs.analytics_job -> skipped (target override; has own webhook_notifications, may need manual patch)
```

What actually ships:

- Base gets the new `WID` webhook patched in.
- prod's override is untouched.
- **Bundle deep-merge replaces lists, doesn't concatenate them.** So in prod, the override's `webhook_notifications: { on_failure: [pagerduty-uuid] }` wins entirely — the base's `WID` never reaches prod. Prod is silently missing the rollout.

Fix by hand-editing `databricks.yml` to add `WID` into prod's existing event lists:

```yaml
webhook_notifications:
  on_failure:
    - id: prod-pagerduty-destination-uuid
    - id: WID            # <-- add by hand so prod gets both
  on_success:
    - id: WID            # <-- on_success/on_start didn't exist before; add
  on_start:
    - id: WID
```

Or, if you don't want the WARNING-then-manual-edit dance, drop `--skip-target-overrides` and let the patcher write through (Case 1). That always wins because the override's `webhook_notifications` gets fully rewritten.

**Tl;dr:** `--skip-target-overrides` is great when overrides only customize unrelated fields (name, timeout, tags). It's a footgun when an override defines its own `webhook_notifications`, because DAB list-merge semantics are "override replaces base." The WARNING is your only signal.

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
                           In --remove mode, optional: provide to remove only
                           that destination; omit to clear ALL webhooks.
--events on_failure,on_success,on_start
                           Comma-separated event list (add mode only). Valid:
                           on_start, on_success, on_failure,
                           on_duration_warning_threshold_exceeded.
                           Default: on_failure,on_success,on_start.

--remove                   Switch to remove mode. Pair with --job-id.
--job-id <id>              Job ID to operate on (required with --remove).
                           Repeatable. Invalid in add mode.

--tag key=value | key      Filter to jobs whose tags contain key=value
                           (or just key for presence-only). Add mode only.
--owner <email>            Filter by creator_user_name. Repeatable. Add mode only.

--bundle-jobs {skip,include,only}
                           Policy for jobs with deployment.kind=BUNDLE.
                           Default: skip. Add mode only — in --remove mode
                           bundle jobs proceed with a WARNING.
--bundle-report <path>     CSV output path. Default: bundle_jobs.csv.
                           Pass '' to disable.

--apply                    Actually call jobs/update. Default is dry-run.
--limit <N>                Stop after N updates (matched + would_update).
                           Add mode only.
--progress-every <N>       Log a tally every N scanned jobs. Default 500.
                           Set 0 to disable. Add mode only.

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
# 1. Confirm the bundle parses cleanly before any patcher run
cd examples/caveats
databricks bundle validate
cd ../..

# 2. (Optional) Deploy to your dev workspace so you can compare the
#    before/after of an actual deploy.
cd examples/caveats
databricks bundle deploy
cd ../..

# 3. Default behavior — patches base + every per-target override.
#    Inspect the diff: you'll see the per-job duplication from the
#    anchor caveat, the literal-ID-appended-to-${var.webhook_id} from
#    the variable caveat, and both the base and the prod override of
#    analytics_job getting patched.
python3 patch_bundle_yaml.py --bundle-dir examples/caveats --webhook-id WID-LITERAL-12345

# 4. --skip-target-overrides — exposes the WARNING for the prod override
#    that already has its own webhook_notifications.
python3 patch_bundle_yaml.py --bundle-dir examples/caveats --webhook-id WID-LITERAL-12345 --skip-target-overrides

# 5. Apply (after reviewing the diff) and redeploy to compare against
#    the pre-patch deploy from step 2.
python3 patch_bundle_yaml.py --bundle-dir examples/caveats --webhook-id WID-LITERAL-12345 --apply
cd examples/caveats && databricks bundle deploy && cd ../..
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
