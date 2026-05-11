# Bulk Apply Webhook Notifications to Databricks Jobs

Two Python scripts for rolling out a webhook-based Notification Destination across every job in a Databricks workspace:

1. **`bulk_apply_webhooks.py`** — workspace-side script. Walks every job via the Jobs API, attaches the webhook to manually-created jobs, and inventories bundle-managed jobs for hand-off (because API edits to bundle jobs are non-durable).
2. **`patch_bundle_yaml.py`** — companion script. Runs on a local checkout of a bundle repo, edits the bundle YAML in place to add `webhook_notifications` to job resources, producing a review-ready git diff. This is the durable fix for bundle-managed jobs.

Both scripts default to dry-run, support tag/owner filters for staged rollout, and are idempotent.

---

## What `bulk_apply_webhooks.py` does

- Enumerates jobs via `GET /api/2.2/jobs/list` (paginated).
- For each job, computes the desired `webhook_notifications` block by merging the supplied webhook ID into the configured event lists (defaults: `on_failure`, `on_success`, `on_start`). Existing webhooks are preserved.
- Calls `POST /api/2.2/jobs/update` to apply the change.
- Skips bundle-managed jobs by default (because `databricks bundle deploy` would overwrite API edits), and emits an inventory CSV of those jobs for hand-off to bundle owners.
- Honors rate limits with exponential backoff plus jitter, and paces calls between updates.

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

## Find your webhook destination ID

The script takes a destination **ID**, not a URL. Create the destination once in Admin Settings → Notifications, then look up its ID:

```bash
# By display name, returning just the ID:
databricks notification-destinations list -o json \
  | jq -r '(.results // .) | .[] | select(.display_name=="<your-destination-name>") | .id'
```

Stash it in a variable for the rest of the session:
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
- **Anchors and `<<:` merges**: ruamel preserves anchors on round-trip, but if jobs share configuration via `<<: *base`, you may want to put the webhook block on the base anchor rather than each job. Review the diff carefully on those repos.
- **No `bundle validate` built in**: run `databricks bundle validate` after `--apply` and before opening the PR — it'll catch any structural issue (rare, but cheap insurance).
- **Per-target overrides**: by default, the script patches both base job definitions and per-target overrides (`targets.<env>.resources.jobs.<name>`). This is explicit but produces extra diff lines. Pass `--skip-target-overrides` to patch only base definitions and rely on bundle deep-merge to propagate webhook config into each target. When that flag is set, the script logs a WARNING for any override that already has its own `webhook_notifications` block — bundle merge would let the override's list win over the base, so those need a manual patch.

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
--webhook-id <id>          Required. Notification destination ID to attach.
--events on_failure,on_success,on_start
                           Comma-separated event list. Valid values:
                           on_start, on_success, on_failure,
                           on_duration_warning_threshold_exceeded.
                           Default: on_failure,on_success,on_start.

--tag key=value | key      Filter to jobs whose tags contain key=value
                           (or just key for presence-only).
--owner <email>            Filter by creator_user_name. Repeatable.

--bundle-jobs {skip,include,only}
                           Policy for jobs with deployment.kind=BUNDLE.
                           Default: skip.
--bundle-report <path>     CSV output path. Default: bundle_jobs.csv.
                           Pass '' to disable.

--apply                    Actually call jobs/update. Default is dry-run.
--limit <N>                Stop after N updates (matched + would_update).
--progress-every <N>       Log a tally every N scanned jobs. Default 500.
                           Set 0 to disable.

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

---

## Example bundles (reference)

The `examples/` directory contains two reference Asset Bundles you can use to validate both scripts end-to-end:

- **`examples/simple/`** — single-job bundle, single target. Good for the smoke-test workflow below.
- **`examples/complex/`** — multi-file bundle exercising variables, `include:` globs, YAML anchors/aliases, per-target overrides, and a mix of job and pipeline resources. Useful for stress-testing the patcher.

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
