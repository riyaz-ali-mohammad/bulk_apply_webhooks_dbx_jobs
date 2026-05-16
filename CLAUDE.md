# CLAUDE.md

## Project overview

Five single-file Python CLIs for rolling out a Databricks webhook Notification Destination across every job in a workspace. `inventory_jobs.py` is the read-only discovery step: it walks the Jobs API and reports how many jobs are DAB-deployed vs directly-deployed, writing a per-job CSV with optional bundle-metadata enrichment. `create_webhook_destination.py` creates the destination idempotently. `apply_webhooks_to_direct_jobs.py` walks the Jobs API and attaches the webhook to non-DAB jobs only; DAB-managed jobs are ALWAYS skipped (no flag to override) since `databricks bundle deploy` would silently overwrite API edits. `remove_webhooks.py` is the inverse — the rollback / detach path; two shapes (per-job by ID, or workspace-walk by webhook ID) picked by which flags are passed; unlike the attach script it still has `--bundle-jobs` since cleaning up stale references on bundle jobs is a legitimate use case. `patch_bundle_yaml.py` is the durable counterpart for bundle-managed jobs — it runs against a local checkout of a Databricks Asset Bundle (DAB) repo and edits `databricks.yml` plus its `include:` files in place, producing a review-ready git diff. Stack: Python 3.9+, `databricks-sdk`, `ruamel.yaml`. No framework, no package layout — top-level scripts only.

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
python3 apply_webhooks_to_direct_jobs.py --webhook-id <id>
python3 remove_webhooks.py --webhook-id <id>                # workspace-walk rollback
python3 remove_webhooks.py --job-id 1234 --job-id 5678      # per-job rollback
python3 patch_bundle_yaml.py --bundle-dir <path> --webhook-id <id>

# Tests (no workspace required; uses examples/caveats as a tmp_path copy)
python3 -m pytest                       # full suite
python3 -m pytest tests/test_apply_webhooks_to_direct_jobs.py
python3 -m pytest -k TestPatchWebhooksDABCaveats   # single class

# Validate a patched bundle (run from inside the bundle dir)
databricks bundle validate
databricks bundle deploy            # or: deploy -t prod
```

End-to-end validation against a real workspace is still manual via the `examples/` bundles — see README "Caveats-bundle workflow" and "Full smoke-test workflow" sections.

## Project structure

```
.
├── inventory_jobs.py              # Read-only: walks jobs/list, classifies BUNDLE vs DIRECT, writes jobs_inventory.csv (CLI) or Delta (notebook).
├── apply_webhooks_to_direct_jobs.py  # Workspace-side ADD: walks jobs/list, attaches webhook to non-DAB jobs. DAB jobs ALWAYS skipped (no flag, no escape hatch). No Delta output.
├── remove_webhooks.py             # Workspace-side REMOVE: per-job rollback (--job-id/--job-ids-from) or workspace-walk rollback (--webhook-id only). Retains --bundle-jobs/--bundle-report (CSV + Delta inventory of bundles encountered) for stale-reference cleanup.
├── patch_bundle_yaml.py           # Local-checkout-side: round-trip YAML edit of DAB resources.
├── create_webhook_destination.py  # One-shot: create a generic-webhook Notification Destination via raw REST (typed API absent in runtime SDK 0.20.0).
├── requirements.txt               # Just databricks-sdk and ruamel.yaml.
├── requirements-dev.txt           # Pulls in requirements.txt + pytest.
├── pytest.ini                     # testpaths=tests, pythonpath=. so tests can import top-level scripts.
├── README.md                      # Long-form user docs; cite section names when changing behavior.
├── notebooks/                     # Databricks source-format notebooks. All notebook files end in _nb.py to avoid name collisions with the top-level scripts they import (sys.path includes notebooks/, so a notebook named identically to its script would shadow it).
│   ├── README.md                              # Hands-on runbook for the support team running the notebooks.
│   ├── _auth.py                               # Shared client-builder used by the four SDK notebooks (multi-workspace via SP OAuth M2M). NOT a notebook — no _nb suffix.
│   ├── inventory_jobs_nb.py                   # Widgets + multi-workspace loop calling inventory_jobs.run(client=..., delta_table=...).
│   ├── apply_webhooks_to_direct_jobs_nb.py    # ADD-mode widgets + multi-workspace loop. No bundle_jobs widget, no Delta output.
│   ├── remove_webhooks_nb.py                  # REMOVE-mode widgets (per-job + walk); auto-picks shape from which widgets are filled.
│   ├── create_webhook_destination_nb.py       # Same pattern; no Delta output.
│   └── patch_bundle_yaml_nb.py                # Single-workspace; reads/writes YAMLs in a Databricks Git folder.
├── tests/
│   ├── test_inventory_jobs.py         # Classification + filters + CSV schema + end-to-end main() drive + notebook-only kwargs (client/scan_limit/name_filter).
│   ├── test_apply_webhooks_to_direct_jobs.py  # ADD-path-only: non-DAB attach, always-skip-bundle invariant, run-callable contract.
│   ├── test_remove_webhooks.py        # REMOVE-path: per-job rollback, walk-mode rollback, load_job_ids_from_file, duplicate-helpers smoke tests.
│   ├── test_create_webhook_destination.py  # Idempotency + client-injection contract for the multi-workspace dispatcher; mocks api_client.do.
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

- **Single-file CLIs.** Each script is one module with a `main() -> int` plus `if __name__ == "__main__": sys.exit(main())`. No package, no shared library, no cross-imports between the five scripts. Duplicated logic across files is intentional. `EVENT_FIELDS` lives in apply_webhooks_to_direct_jobs, remove_webhooks, and patch_bundle_yaml. `is_bundle_job` / `job_matches` / `Filters` / `is_transient` / `call_with_backoff` live in apply_webhooks_to_direct_jobs, remove_webhooks, and inventory_jobs. `_dig` / `BundleMetadata` / `BundleJobRecord` / `fetch_bundle_metadata` / `write_bundle_report` / `write_bundle_report_delta` live in remove_webhooks and inventory_jobs ONLY — the attach script does not need them because it always skips bundle jobs without fetching metadata. Keep it that way unless the user asks for a shared module.
- **Each script also exposes a `run(**kwargs)` callable.** `main()` is a thin shim over `run()` — it calls `run(**vars(parse_args()))`. The notebook layer imports `run()` directly. Kwargs split into two groups: CLI-shape kwargs (1:1 with `parse_args()`) and notebook-only kwargs (`client`, `spark`, `delta_table`, `scan_limit`, `name_filter`, `workspace_label`). The notebook-only kwargs default to None so CLI users get the existing behaviour. Keep `run()` signatures stable — notebook widgets map to them.
- **`client` kwarg semantics.** When `run(client=...)` is passed a pre-built `WorkspaceClient`, `build_client()` MUST NOT be called. This is how the multi-workspace dispatcher in `notebooks/_auth.py` injects an SP-authed client per target workspace. Tests pin this — see `TestRunNotebookKwargs.test_client_kwarg_bypasses_build_client`.
- **`scan_limit` vs `limit`.** `limit` is a CLI flag that caps **mutations** (jobs that would actually get updated). `scan_limit` is a notebook-only kwarg that caps the **workspace walk itself** (jobs scanned, regardless of matches). They're not interchangeable; the README "Scan performance" section explains the distinction. The transcript feedback (Riyaz @ 44:03) called out that `--limit` alone doesn't shorten a scan when matches are sparse — that's by design, and `scan_limit` is the fix.
- **Notebook layer is allowed to share.** The "no shared module between the five scripts" invariant applies to the .py scripts. `notebooks/_auth.py` is a notebook-side helper that the four SDK notebooks import — that's allowed and intentional (the multi-workspace dispatcher is identical across notebooks).
- **Argparse-only.** `parse_args()` is the single entry for CLI handling, with `RawDescriptionHelpFormatter` and the module docstring used as `description=`.
- **Dataclasses for state.** `apply_webhooks_to_direct_jobs.py` uses `@dataclass` for `Filters` and `Stats`; `remove_webhooks.py` adds `BundleMetadata` and `BundleJobRecord` for its walk-mode inventory. Keep new state in dataclasses, not tuples or dicts.
- **Logging, not print.** `logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s")`. INFO is the normal level; DEBUG is gated behind `-v`. The only `print` calls are in `create_webhook_destination.py`'s `print_summary` (user-visible result block).
- **Type hints throughout.** `Optional`, `List`, `Dict`, `Tuple` from `typing`. Match existing style.
- **Dry-run by default.** Every mutating script is dry-run unless `--apply` is passed. New mutating features must follow the same pattern. `inventory_jobs.py` is read-only and has no `--apply` flag.
- **Idempotency is load-bearing.** All mutating scripts must be safe to re-run: `already_attached`, `find_existing` (name lookup), and the patcher's "already has webhook" check. Don't add code paths that mutate when state already matches.
- **Comments explain *why*, not *what*.** Look at the docstring at the top of `patch_bundle_yaml.py` and the `_override_dup_events` / `_has_variable_ref` docstrings — they document deploy-time behavior (DAB concat, `Duplicate webhook ids`) that is not obvious from the code. Preserve that style when touching this logic.

## Architecture decisions & constraints

- **`apply_webhooks_to_direct_jobs.py` never mutates bundle jobs.** No flag, no escape hatch. The reason: API edits to BUNDLE-kind jobs get overwritten by the next `databricks bundle deploy`, and in `mode: production` the regression is invisible until then. Bundle jobs are exclusively the patcher's responsibility. `remove_webhooks.py` still has the policy (default `skip`) because cleaning up stale references on bundle jobs is a legitimate one-shot use case.
- **`patch_bundle_yaml.py` never writes to per-target overrides.** DAB deep-merge **concatenates** `webhook_notifications` event lists at deploy. Patching both base and override produces a `Duplicate webhook ids` deploy failure. The patcher walks `targets.<env>.resources.jobs.<name>` only to *detect* and warn — never to write. There is intentionally no flag to override this. Reread the docstring + `_override_dup_events` before changing override handling.
- **`patch_bundle_yaml.py` skips event lists containing `${var.*}` entries.** Same reason: the patcher writes literal IDs, can't resolve variables, and a variable that resolves to the same ID produces a duplicate-rejection at deploy. Skip-with-WARNING is the safe behavior. Don't add auto-resolution.
- **`ruamel.yaml` round-trip is required.** Comments, key order, anchors, and quoting must survive the patcher. Use `make_yaml()` (configured with `preserve_quotes=True`, `width=4096`, `indent(mapping=2, sequence=4, offset=2)`). Do not swap in PyYAML.
- **Backoff and pacing are deliberate.** `call_with_backoff` retries only on transient errors (`is_transient`: 429, 5xx, RATE_LIMIT, INTERNAL, UNAVAILABLE). Between updates, the bulk script sleeps `base_sleep + uniform(0, jitter)`. Don't replace with a different retry library or unbounded retry.
- **Auth via SDK credential chain.** Never read tokens directly. `WorkspaceClient(profile=...)` or `WorkspaceClient()` handles env vars, profiles, and OAuth.
- **`bundle_jobs.csv` schema (produced by `remove_webhooks.py` only) is part of the contract.** Bundle owners consume the columns to find which repo + target to patch. If you change columns, update `write_bundle_report`'s `columns` list in `remove_webhooks.py` and the README's "Output: `bundle_jobs.csv`" table together. The attach script no longer produces this CSV.

## Testing approach

Two layers:

**Pytest suite (`tests/`)** — runs without a workspace.
- `test_inventory_jobs.py` — `is_bundle_job`, `job_matches`, `fetch_bundle_metadata`, `write_inventory` (CSV schema), plus a `TestMainEndToEnd` class that drives `inventory_jobs.main()` against a mocked `WorkspaceClient`.
- `test_apply_webhooks_to_direct_jobs.py` — ADD-path coverage. `TestProcessJobNonDAB` for attach logic, `TestIsBundleJob` for the always-skip detection, and `TestBundleJobsAlwaysSkipped` to pin the invariant (bundle jobs go to `stats.bundle_skipped`; `jobs.update` is never called for them; the mutation `--limit` is not consumed by skips). DAB jobs are constructed with a real `JobDeployment(kind=JobDeploymentKind.BUNDLE, …)`.
- `test_remove_webhooks.py` — REMOVE-path coverage. `TestRemoveWebhooks`, `TestWebhookAttached`, `TestLoadJobIdsFromFile`, `TestProcessRemoveJobDAB`, `TestRemoveWalkMode`, `TestResolveRemoveJobIds`, plus `TestRunCallable` (cross-flag guards) and `TestRunNotebookKwargs` (client + scan_limit) for the run-callable contract. Also smoke-tests the helpers duplicated from `apply_webhooks_to_direct_jobs.py` so drift is caught early.
- `test_patch_bundle_yaml.py` — split into "base-resource patching" (the non-DAB-style YAML edit case) and "DAB-specific caveats" (`${var.*}` skip, per-target override skip, `_override_dup_events`). The `TestCaveatsBundleEndToEnd` class copies `examples/caveats/` into `tmp_path` and runs `patcher.main()` against the copy — keep that fixture working, the README's caveats workflow is the spec it pins down.
- Run: `python3 -m pytest`. Single class: `python3 -m pytest -k <ClassName>`.

**End-to-end validation against a real workspace** — manual, against the `examples/` bundles:
- `examples/simple/` — smoke test for the bulk script's bundle detection + the patcher's happy path.
- `examples/complex/` — multi-file bundle, variables, anchors, pipelines.
- `examples/caveats/` — every patcher caveat in one bundle; expected log output is documented in the README's "Caveats-bundle workflow" section.

A change to any of the five scripts should pass `pytest` AND be dry-run against the relevant `examples/` bundle before claiming the change works. Hand the workspace-deploy commands to the user — they run anything that touches a Databricks workspace.

## Honesty / anti-hallucination guardrails

**This is the most important section in this file. Read it before claiming any fact.**

In a prior session, Claude confidently stated that the Databricks Jobs API `name` parameter does **substring matching**, when in fact it does **exact (case-insensitive) match**. This was sold as a feature in the README, a widget description, and a section explanation to the user, who then debugged for a real chunk of time before discovering the lie. That kind of fabrication is not acceptable. The rules below exist to prevent it from happening again.

- **Don't invent API, SDK, or library behavior.** If you are about to state a fact about a Databricks API parameter, an SDK method signature, a library default, an OS behavior, or any external service — verify it first. Acceptable sources, in priority order: (1) source code you have read in this session via `Read`/`Bash grep`; (2) official docs you have fetched in this session via `WebFetch`; (3) a test the user has run in this session. Recall from training data is **not** a source — it is a guess, and guesses presented as facts are hallucinations.
- **Default to "I don't know" when uncertain.** When you are not sure, say so plainly: "I don't know", "I haven't verified this", "I'm guessing — let's test". Hedge words like "I think", "typically", "should work", "usually", "in my experience", "AFAIK", and "I believe" are how guesses get smuggled in as facts. If you catch yourself reaching for one, stop and either (a) verify, or (b) mark the statement explicitly as unverified.
- **Cite what you verified, mark what you didn't.** When stating something observed from source/docs/tests in this session, cite the source (file:line, the doc URL, or the cell output). When stating something from general knowledge, prefix it with "unverified" and offer the user a verification step. Never blur the two.
- **Treat user pushback as evidence you are wrong.** When the user says "this isn't working", "I think you're wrong about X", or "I'm seeing the opposite of what you said" — default to investigating, not defending. Run a verification, read the source, or admit the gap. Do not argue from authority. The user is sitting in front of the failing output; you are not.
- **Prefer a 3-line test over a paragraph of recall.** When the user can run code to verify behavior, give them the test, not the explanation. Especially true for Databricks API / SDK behavior — a `list(w.jobs.list(name="x"))` cell tells the truth faster than any reasoning you could do from memory.
- **No false precision.** Don't state version numbers, defaults, parameter shapes, error message wording, retry counts, timeout values, or release dates unless you have actually observed them. "Added in SDK 0.18.0" requires a changelog you read this session; otherwise say "added in some recent SDK version — check the changelog if it matters."
- **When you make a mistake, lead with the correction.** If a previous claim turns out to be wrong, the next message must start with the correction explicitly ("I was wrong about X — actually Y"). Do not bury it. Do not pivot to a fix without acknowledging the error.

## Always

- Keep all mutating scripts **dry-run by default**; only mutate behind `--apply`. (`inventory_jobs.py` is read-only and has no `--apply`.)
- Preserve **idempotency** on re-run for every mutating code path.
- Run `python3 -m pytest` after any change to `inventory_jobs.py`, `apply_webhooks_to_direct_jobs.py`, `remove_webhooks.py`, or `patch_bundle_yaml.py`. The DAB caveats are covered in the suite — a green run is the cheapest way to know the always-skip-bundle invariant, the override-skip, and the `${var.*}`-skip invariants still hold.
- When changing patcher logic, re-read the relevant caveat section in the README and update both the docstring and the README in the same change. The README is the user-facing contract.
- Match the existing logging format and verbosity gating (`-v` → DEBUG).
- For workspace/bundle-touching command sequences, hand the commands to the user to run — don't execute them via Bash yourself.
- When changing `run()` kwargs in any of the four SDK scripts (`inventory_jobs.py`, `apply_webhooks_to_direct_jobs.py`, `remove_webhooks.py`, `create_webhook_destination.py`), update the corresponding notebook under `notebooks/` in the same change. The notebook layer is the contract the support team uses; drift between `run()` and the notebooks breaks them.
- When changing the Delta-write schemas in `write_inventory_delta` / `write_bundle_report_delta`, update the README's "Output: Delta tables" section. The schema is part of the contract — downstream consumers SELECT specific columns.

## Never

- Never let `apply_webhooks_to_direct_jobs.py` mutate bundle-managed jobs. There is no flag, no kwarg, no escape hatch — the skip is unconditional. Adding one back is a design regression; if the user asks for it, push back and discuss before complying.
- Never write to `targets.<env>.resources.jobs.<name>` in `patch_bundle_yaml.py`. DAB merge concatenation will produce `Duplicate webhook ids` at deploy.
- Never swap `ruamel.yaml` for PyYAML or any non-round-trip YAML library. Comments, anchors, and quoting must survive.
- Never auto-resolve `${var.*}` references in the patcher. Skip with WARNING is the correct behavior.
- Never edit files under `.databricks/`, `bundle_jobs.csv`, or `jobs_inventory.csv` — they're runtime artifacts and are gitignored.
- Never add a shared utility module between the five scripts unless the user asks for it. They are intentionally standalone, even when that means duplicating helpers like `is_bundle_job` / `fetch_bundle_metadata` between the bulk, remove, and inventory scripts. (Note: `notebooks/_auth.py` is a separate notebook-layer helper — that's allowed and not what this rule is about.)
- Never call `databricks bundle validate` or `databricks bundle deploy` from inside the patcher notebook. The PR-review gate is load-bearing — validate/deploy live in CI. The patcher notebook stops at "patched files + diff."
- Never bypass `--apply` checks by short-circuiting in helper functions.