# CLAUDE.md

## Project overview

Four single-file Python CLIs for rolling out a Databricks webhook Notification Destination across every job in a workspace. `inventory_jobs.py` is the read-only discovery step: it walks the Jobs API and reports how many jobs are DAB-deployed vs directly-deployed, writing a per-job CSV with optional bundle-metadata enrichment. `create_webhook_destination.py` creates the destination idempotently. `bulk_apply_webhooks.py` walks the Jobs API and attaches the webhook to manually-created jobs (skipping bundle-managed ones, since `databricks bundle deploy` would silently overwrite API edits). `patch_bundle_yaml.py` is the durable counterpart for bundle-managed jobs — it runs against a local checkout of a Databricks Asset Bundle (DAB) repo and edits `databricks.yml` plus its `include:` files in place, producing a review-ready git diff. Stack: Python 3.9+, `databricks-sdk`, `ruamel.yaml`. No framework, no package layout — top-level scripts only.

## Key commands

```bash
# Install (prod)
pip install -r requirements.txt

# Install (dev — includes pytest)
pip install -r requirements-dev.txt

# Run (mutating scripts default to dry-run; --apply mutates. inventory_jobs.py is
# read-only by design — no --apply needed.)
python3 inventory_jobs.py [--enrich-bundles]
python3 create_webhook_destination.py --url <https-url> --name <display-name>
python3 bulk_apply_webhooks.py --webhook-id <id>
python3 patch_bundle_yaml.py --bundle-dir <path> --webhook-id <id>

# Tests (no workspace required; uses examples/caveats as a tmp_path copy)
python3 -m pytest                       # full suite
python3 -m pytest tests/test_bulk_apply_webhooks.py
python3 -m pytest -k TestPatchWebhooksDABCaveats   # single class

# Validate a patched bundle (run from inside the bundle dir)
databricks bundle validate
databricks bundle deploy            # or: deploy -t prod
```

End-to-end validation against a real workspace is still manual via the `examples/` bundles — see README "Caveats-bundle workflow" and "Full smoke-test workflow" sections.

## Project structure

```
.
├── inventory_jobs.py              # Read-only: walks jobs/list, classifies BUNDLE vs DIRECT, writes jobs_inventory.csv.
├── bulk_apply_webhooks.py         # Workspace-side: walks jobs/list, attaches webhook, inventories bundle jobs to bundle_jobs.csv.
├── patch_bundle_yaml.py           # Local-checkout-side: round-trip YAML edit of DAB resources.
├── create_webhook_destination.py  # One-shot: create a generic-webhook Notification Destination.
├── requirements.txt               # Just databricks-sdk and ruamel.yaml.
├── requirements-dev.txt           # Pulls in requirements.txt + pytest.
├── pytest.ini                     # testpaths=tests, pythonpath=. so tests can import top-level scripts.
├── README.md                      # Long-form user docs; cite section names when changing behavior.
├── tests/
│   ├── test_inventory_jobs.py         # Classification + filters + CSV schema + end-to-end main() drive.
│   ├── test_bulk_apply_webhooks.py    # Non-DAB and DAB job handling for the bulk script.
│   └── test_patch_bundle_yaml.py      # Base-resource patches + DAB caveats; ends with an integration
│                                      # test that runs main() against a tmp-copy of examples/caveats.
└── examples/
    ├── simple/                    # Single-job bundle. Smoke test.
    ├── complex/                   # Multi-file bundle: variables, include globs, anchors, pipelines.
    └── caveats/                   # Purpose-built to hit every patcher caveat at once
                                   #   (${var.*}, <<: *anchor, per-target override).
```

`bundle_jobs.csv`, `jobs_inventory.csv`, and `.databricks/` are runtime artifacts — keep them out of source control.

## Code conventions

- **Single-file CLIs.** Each script is one module with a `main() -> int` plus `if __name__ == "__main__": sys.exit(main())`. No package, no shared library, no cross-imports between the four scripts. Duplicated logic across files is intentional — `EVENT_FIELDS` / `parse_events` (in the bulk and patcher scripts), and `_dig` / `BundleMetadata` / `is_bundle_job` / `fetch_bundle_metadata` / `job_matches` / `Filters` (in the bulk and inventory scripts). Keep it that way unless the user asks for a shared module.
- **Argparse-only.** `parse_args()` is the single entry for CLI handling, with `RawDescriptionHelpFormatter` and the module docstring used as `description=`.
- **Dataclasses for state.** `bulk_apply_webhooks.py` uses `@dataclass` for `Filters`, `Stats`, `BundleMetadata`, `BundleJobRecord`. Keep new state in dataclasses, not tuples or dicts.
- **Logging, not print.** `logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s")`. INFO is the normal level; DEBUG is gated behind `-v`. The only `print` calls are in `create_webhook_destination.py`'s `print_summary` (user-visible result block).
- **Type hints throughout.** `Optional`, `List`, `Dict`, `Tuple` from `typing`. Match existing style.
- **Dry-run by default.** Every mutating script is dry-run unless `--apply` is passed. New mutating features must follow the same pattern. `inventory_jobs.py` is read-only and has no `--apply` flag.
- **Idempotency is load-bearing.** All mutating scripts must be safe to re-run: `already_attached`, `find_existing` (name lookup), and the patcher's "already has webhook" check. Don't add code paths that mutate when state already matches.
- **Comments explain *why*, not *what*.** Look at the docstring at the top of `patch_bundle_yaml.py` and the `_override_dup_events` / `_has_variable_ref` docstrings — they document deploy-time behavior (DAB concat, `Duplicate webhook ids`) that is not obvious from the code. Preserve that style when touching this logic.

## Architecture decisions & constraints

- **`bulk_apply_webhooks.py` never mutates bundle jobs by default.** `--bundle-jobs=skip` is the default for a reason: API edits to BUNDLE-kind jobs get overwritten by the next `databricks bundle deploy`, and in `mode: production` the regression is invisible until then. Do not change the default. `include` is an escape hatch only.
- **`patch_bundle_yaml.py` never writes to per-target overrides.** DAB deep-merge **concatenates** `webhook_notifications` event lists at deploy. Patching both base and override produces a `Duplicate webhook ids` deploy failure. The patcher walks `targets.<env>.resources.jobs.<name>` only to *detect* and warn — never to write. There is intentionally no flag to override this. Reread the docstring + `_override_dup_events` before changing override handling.
- **`patch_bundle_yaml.py` skips event lists containing `${var.*}` entries.** Same reason: the patcher writes literal IDs, can't resolve variables, and a variable that resolves to the same ID produces a duplicate-rejection at deploy. Skip-with-WARNING is the safe behavior. Don't add auto-resolution.
- **`ruamel.yaml` round-trip is required.** Comments, key order, anchors, and quoting must survive the patcher. Use `make_yaml()` (configured with `preserve_quotes=True`, `width=4096`, `indent(mapping=2, sequence=4, offset=2)`). Do not swap in PyYAML.
- **Backoff and pacing are deliberate.** `call_with_backoff` retries only on transient errors (`is_transient`: 429, 5xx, RATE_LIMIT, INTERNAL, UNAVAILABLE). Between updates, the bulk script sleeps `base_sleep + uniform(0, jitter)`. Don't replace with a different retry library or unbounded retry.
- **Auth via SDK credential chain.** Never read tokens directly. `WorkspaceClient(profile=...)` or `WorkspaceClient()` handles env vars, profiles, and OAuth.
- **`bundle_jobs.csv` schema is part of the contract.** Bundle owners consume the columns to find which repo + target to patch. If you change columns, update `write_bundle_report`'s `columns` list and the README's "Output: `bundle_jobs.csv`" table together.

## Testing approach

Two layers:

**Pytest suite (`tests/`)** — runs without a workspace.
- `test_inventory_jobs.py` — `is_bundle_job`, `job_matches`, `fetch_bundle_metadata`, `write_inventory` (CSV schema), plus a `TestMainEndToEnd` class that drives `inventory_jobs.main()` against a mocked `WorkspaceClient`.
- `test_bulk_apply_webhooks.py` — split into "non-DAB" classes (`TestProcessJobNonDAB`, attach/detach logic, filters) and "DAB-deployed" classes (`TestIsBundleJob`, `TestFetchBundleMetadata`, `TestWriteBundleReport`, `TestProcessRemoveJobDAB`). DAB jobs are constructed with a real `JobDeployment(kind=JobDeploymentKind.BUNDLE, …)`.
- `test_patch_bundle_yaml.py` — split into "base-resource patching" (the non-DAB-style YAML edit case) and "DAB-specific caveats" (`${var.*}` skip, per-target override skip, `_override_dup_events`). The `TestCaveatsBundleEndToEnd` class copies `examples/caveats/` into `tmp_path` and runs `patcher.main()` against the copy — keep that fixture working, the README's caveats workflow is the spec it pins down.
- Run: `python3 -m pytest`. Single class: `python3 -m pytest -k <ClassName>`.

**End-to-end validation against a real workspace** — manual, against the `examples/` bundles:
- `examples/simple/` — smoke test for the bulk script's bundle detection + the patcher's happy path.
- `examples/complex/` — multi-file bundle, variables, anchors, pipelines.
- `examples/caveats/` — every patcher caveat in one bundle; expected log output is documented in the README's "Caveats-bundle workflow" section.

A change to any of the four scripts should pass `pytest` AND be dry-run against the relevant `examples/` bundle before claiming the change works. Hand the workspace-deploy commands to the user — they run anything that touches a Databricks workspace.

## Always

- Keep all mutating scripts **dry-run by default**; only mutate behind `--apply`. (`inventory_jobs.py` is read-only and has no `--apply`.)
- Preserve **idempotency** on re-run for every mutating code path.
- Run `python3 -m pytest` after any change to `inventory_jobs.py`, `bulk_apply_webhooks.py`, or `patch_bundle_yaml.py`. The DAB caveats are covered in the suite — a green run is the cheapest way to know the override-skip and `${var.*}`-skip invariants still hold.
- When changing patcher logic, re-read the relevant caveat section in the README and update both the docstring and the README in the same change. The README is the user-facing contract.
- Match the existing logging format and verbosity gating (`-v` → DEBUG).
- For workspace/bundle-touching command sequences, hand the commands to the user to run — don't execute them via Bash yourself.

## Never

- Never make `bulk_apply_webhooks.py` mutate bundle-managed jobs by default. `--bundle-jobs=skip` stays the default.
- Never write to `targets.<env>.resources.jobs.<name>` in `patch_bundle_yaml.py`. DAB merge concatenation will produce `Duplicate webhook ids` at deploy.
- Never swap `ruamel.yaml` for PyYAML or any non-round-trip YAML library. Comments, anchors, and quoting must survive.
- Never auto-resolve `${var.*}` references in the patcher. Skip with WARNING is the correct behavior.
- Never edit files under `.databricks/`, `bundle_jobs.csv`, or `jobs_inventory.csv` — they're runtime artifacts and are gitignored.
- Never add a shared utility module between the four scripts unless the user asks for it. They are intentionally standalone, even when that means duplicating helpers like `is_bundle_job` / `fetch_bundle_metadata` between the bulk and inventory scripts.
- Never bypass `--apply` checks by short-circuiting in helper functions.