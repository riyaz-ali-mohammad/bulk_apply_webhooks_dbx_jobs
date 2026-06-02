#!/usr/bin/env python3
"""Attach a Databricks webhook notification destination to non-DAB jobs in a workspace.

Two shapes (the script picks based on which flags are passed):
  (1) Workspace-walk (default): omit --job-id/--job-ids-from. Walks the Jobs API
      and attaches the supplied webhook to every matching job, honoring
      --tag/--owner filters.
  (2) Per-job by ID: pass --job-id / --job-ids-from to attach to explicit job
      IDs only (looked up directly via jobs.get — no list pagination).
      Mutually exclusive with the --tag/--owner walk filters.

In BOTH shapes, DAB-managed (Asset Bundle) jobs — `settings.deployment.kind`
== `BUNDLE` — are **always** skipped, even when a bundle job's ID is passed
explicitly. `databricks bundle deploy` would silently overwrite API edits, so
they belong to the patcher (`patch_bundle_yaml.py`). There is no flag to
override this. For an inventory of DAB-deployed jobs in a workspace, use
`inventory_jobs.py` and filter `WHERE deployment_kind = 'BUNDLE'`.

For the rollback / detach path, see the companion script `remove_webhooks.py`.

Auth follows the standard Databricks SDK credential chain:
  - env vars DATABRICKS_HOST + DATABRICKS_TOKEN, or
  - DATABRICKS_CONFIG_PROFILE / --profile, or
  - OAuth via `databricks auth login`.

Examples:
  # Dry-run against every non-DAB job in the workspace
  python apply_webhooks_to_direct_jobs.py --webhook-id 0123abcd

  # Apply to jobs tagged team=platform
  python apply_webhooks_to_direct_jobs.py --webhook-id 0123abcd --tag team=platform --apply

  # Attach to specific jobs by ID (repeat --job-id for more than one)
  python apply_webhooks_to_direct_jobs.py --webhook-id 0123abcd \
      --job-id 1234 --job-id 5678 --apply

  # Attach to job IDs read from a file (one per line, or first column of a CSV)
  python apply_webhooks_to_direct_jobs.py --webhook-id 0123abcd \
      --job-ids-from rollout.txt --apply

  # Staged rollout: cap to 25 owner-filtered jobs, then re-run for the rest
  python apply_webhooks_to_direct_jobs.py --webhook-id 0123abcd \
      --owner alice@example.com --owner bob@example.com --limit 25 --apply
"""

import argparse
import csv
import logging
import random
import sys
import time
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.jobs import (
    BaseJob,
    JobSettings,
    Webhook,
    WebhookNotifications,
)


EVENT_FIELDS = (
    "on_start",
    "on_success",
    "on_failure",
    "on_duration_warning_threshold_exceeded",
)


@dataclass
class Filters:
    tag_key: Optional[str] = None
    tag_value: Optional[str] = None
    owners: List[str] = field(default_factory=list)


@dataclass
class Stats:
    scanned: int = 0
    matched: int = 0
    already_attached: int = 0
    bundle_skipped: int = 0
    would_update: int = 0
    updated: int = 0
    errored: int = 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "--webhook-id",
        required=True,
        help="Notification destination (webhook) ID to attach.",
    )
    p.add_argument(
        "--events",
        default="on_failure,on_duration_warning_threshold_exceeded",
        help=(
            f"Comma-separated event list. Valid: {', '.join(EVENT_FIELDS)}. "
            "Default keeps notifications low-noise; pass on_start/on_success explicitly if needed."
        ),
    )
    p.add_argument(
        "--job-id",
        action="append",
        default=[],
        type=int,
        help=(
            "Job ID to attach the webhook to (per-job mode). Repeatable. "
            "Mutually exclusive with --tag/--owner filters."
        ),
    )
    p.add_argument(
        "--job-ids-from",
        help=(
            "Path to a text or CSV file containing job IDs, one per line "
            "(first column if CSV). Header row auto-detected. Combines with --job-id. "
            "Composes with jobs_inventory.csv via `awk -F, ...`."
        ),
    )
    p.add_argument(
        "--tag",
        help="Filter by job tag. Format: key=value, or just key for presence-only. Walk mode only.",
    )
    p.add_argument(
        "--owner",
        action="append",
        default=[],
        help="Filter by creator_user_name. Repeatable for multiple owners. Walk mode only.",
    )
    p.add_argument("--apply", action="store_true", help="Actually call jobs/update. Default is dry-run.")
    p.add_argument("--profile", help="Databricks CLI profile name.")
    p.add_argument("--max-retries", type=int, default=5, help="Max retries on 429/5xx.")
    p.add_argument("--base-sleep", type=float, default=0.3, help="Base sleep (s) between updates for pacing.")
    p.add_argument("--jitter", type=float, default=0.4, help="Max random jitter (s) added per update.")
    p.add_argument("--limit", type=int, help="Cap on number of jobs to update in this run.")
    p.add_argument(
        "--progress-every",
        type=int,
        default=500,
        help="Log a progress line every N jobs scanned. 0 disables.",
    )
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()
    has_explicit_jobs = bool(args.job_id) or bool(args.job_ids_from)
    has_filters = bool(args.tag) or bool(args.owner)
    if has_explicit_jobs and has_filters:
        p.error(
            "--tag/--owner filter the workspace walk; they don't combine with "
            "explicit --job-id/--job-ids-from. Drop the filters, or drop the explicit job list."
        )
    return args


def setup_logging(verbose: bool) -> None:
    # force=True overrides any pre-existing root logger handlers (e.g. the
    # Databricks runtime installs its own, which makes a plain basicConfig()
    # call a silent no-op and swallows our INFO output in notebook cells).
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        force=True,
    )


def parse_filters(args: argparse.Namespace) -> Filters:
    tag_key = tag_value = None
    if args.tag:
        if "=" in args.tag:
            tag_key, tag_value = args.tag.split("=", 1)
        else:
            tag_key = args.tag
    return Filters(tag_key=tag_key, tag_value=tag_value, owners=list(args.owner))


def load_job_ids_from_file(path: str) -> List[int]:
    """Read job IDs from a text or CSV file (first column).

    Tolerates: blank lines, surrounding whitespace, BOM on the first cell,
    and a CSV header row (auto-detected when the first cell isn't an int —
    so `jobs_inventory.csv` can be piped in directly). Raises SystemExit on
    empty input or a non-numeric mid-file cell. Duplicated from
    remove_webhooks.py per the project's no-shared-module convention."""
    job_ids: List[int] = []
    first_data_row = True
    with open(path, newline="", encoding="utf-8-sig") as fh:  # utf-8-sig drops a leading BOM
        reader = csv.reader(fh)
        for row in reader:
            if not row:
                continue
            cell = row[0].strip()
            if not cell:
                continue
            try:
                job_ids.append(int(cell))
                first_data_row = False
            except ValueError:
                if first_data_row:
                    # Looks like a header row; skip silently.
                    first_data_row = False
                    continue
                raise SystemExit(f"Bad job ID in {path}: {cell!r}")
    if not job_ids:
        raise SystemExit(f"No job IDs found in {path}.")
    return job_ids


def _resolve_job_ids(args: argparse.Namespace) -> List[int]:
    """Merge --job-id and --job-ids-from into a single de-duplicated ordered list."""
    job_ids: List[int] = list(args.job_id)
    if args.job_ids_from:
        job_ids.extend(load_job_ids_from_file(args.job_ids_from))
    # Preserve order, drop duplicates.
    seen = set()
    deduped: List[int] = []
    for jid in job_ids:
        if jid not in seen:
            seen.add(jid)
            deduped.append(jid)
    return deduped


def parse_events(s: str) -> List[str]:
    events = [e.strip() for e in s.split(",") if e.strip()]
    unknown = [e for e in events if e not in EVENT_FIELDS]
    if unknown:
        raise SystemExit(f"Unknown event(s): {unknown}. Valid: {EVENT_FIELDS}")
    return events


def is_bundle_job(job: BaseJob) -> Tuple[bool, Optional[str]]:
    """Return (is_bundle, metadata_file_path). Detected via settings.deployment.kind == BUNDLE.

    metadata_file_path is returned for completeness — this script never reads it,
    since DAB jobs are always skipped. The tuple shape mirrors the helper in
    inventory_jobs.py / remove_webhooks.py so the same test-fixture shape works."""
    s = job.settings
    if not s or not getattr(s, "deployment", None):
        return False, None
    kind = getattr(s.deployment, "kind", None)
    kind_str = getattr(kind, "value", None) or str(kind) if kind is not None else ""
    is_bundle = "BUNDLE" in kind_str.upper()
    return is_bundle, getattr(s.deployment, "metadata_file_path", None)


def job_matches(job: BaseJob, f: Filters) -> bool:
    if f.owners and (job.creator_user_name or "") not in f.owners:
        return False
    if f.tag_key:
        tags = (job.settings.tags if job.settings else None) or {}
        if f.tag_key not in tags:
            return False
        if f.tag_value is not None and tags[f.tag_key] != f.tag_value:
            return False
    return True


def already_attached(existing: Optional[WebhookNotifications], webhook_id: str, events: List[str]) -> bool:
    if not existing:
        return False
    for ev in events:
        cur = getattr(existing, ev, None) or []
        if not any(w.id == webhook_id for w in cur):
            return False
    return True


def merge_webhooks(
    existing: Optional[WebhookNotifications],
    webhook_id: str,
    events: List[str],
) -> WebhookNotifications:
    """Return a new WebhookNotifications with webhook_id added to each target event list,
    preserving any existing webhooks (including on events we aren't touching)."""
    current = existing or WebhookNotifications()
    kwargs = {}
    for ev in EVENT_FIELDS:
        cur = list(getattr(current, ev, None) or [])
        if ev in events and not any(w.id == webhook_id for w in cur):
            cur.append(Webhook(id=webhook_id))
        if cur:
            kwargs[ev] = cur
    return WebhookNotifications(**kwargs)


def is_transient(err: Exception) -> bool:
    msg = str(err).upper()
    code = getattr(err, "error_code", "") or ""
    if "RATE" in msg or "TOO MANY" in msg or "429" in msg:
        return True
    if str(code).startswith("5") or "INTERNAL" in msg or "UNAVAILABLE" in msg:
        return True
    return False


def call_with_backoff(fn, *, max_retries: int):
    for attempt in range(max_retries + 1):
        try:
            return fn()
        except DatabricksError as e:
            if attempt == max_retries or not is_transient(e):
                raise
            sleep = (2 ** attempt) + random.uniform(0, 1.0)
            logging.warning("Transient error, retrying in %.2fs: %s", sleep, e)
            time.sleep(sleep)


def build_client(profile: Optional[str]) -> WorkspaceClient:
    return WorkspaceClient(profile=profile) if profile else WorkspaceClient()


def process_job(
    w: WorkspaceClient,
    job: BaseJob,
    webhook_id: str,
    events: List[str],
    apply: bool,
    max_retries: int,
    stats: Stats,
) -> None:
    existing = job.settings.webhook_notifications if job.settings else None
    name = (job.settings.name if job.settings else None) or "<unnamed>"

    if already_attached(existing, webhook_id, events):
        stats.already_attached += 1
        logging.debug("Job %s (%s): webhook already attached to all target events.", job.job_id, name)
        return

    new_wh = merge_webhooks(existing, webhook_id, events)
    logging.info("Job %s (%s): %s webhook=%s on %s",
                 job.job_id, name, "applying" if apply else "DRY-RUN would apply",
                 webhook_id, events)

    if not apply:
        stats.would_update += 1
        return

    try:
        call_with_backoff(
            lambda: w.jobs.update(
                job_id=job.job_id,
                new_settings=JobSettings(webhook_notifications=new_wh),
            ),
            max_retries=max_retries,
        )
        stats.updated += 1
    except Exception as e:
        stats.errored += 1
        logging.error("Job %s (%s): update failed: %s", job.job_id, name, e)


def process_job_by_id(
    w: WorkspaceClient,
    job_id: int,
    webhook_id: str,
    events: List[str],
    apply: bool,
    max_retries: int,
    stats: Stats,
) -> None:
    """Per-job-id attach: look the job up, then delegate to `process_job`.

    Bundle-managed jobs are STILL skipped here even though the user pointed at
    them explicitly — the always-skip invariant has no escape hatch (API edits
    to bundle jobs are non-durable across `databricks bundle deploy`; use
    patch_bundle_yaml.py instead). This differs from remove_webhooks.py, where
    per-job mode proceeds on bundle jobs with a WARNING."""
    try:
        job = call_with_backoff(lambda: w.jobs.get(job_id=job_id), max_retries=max_retries)
    except Exception as e:
        stats.errored += 1
        logging.error("Job %s: lookup failed: %s", job_id, e)
        return

    name = (job.settings.name if job.settings else None) or "<unnamed>"
    stats.matched += 1

    bundle, _meta_path = is_bundle_job(job)
    if bundle:
        stats.bundle_skipped += 1
        logging.info(
            "Job %s (%s): SKIP bundle-managed (use patch_bundle_yaml for DAB jobs).",
            job_id, name,
        )
        return

    process_job(w, job, webhook_id, events, apply, max_retries, stats)


def _validate_run_kwargs(args: argparse.Namespace) -> None:
    """Re-implement the CLI's post-parse validation so that notebook callers
    fail the same way the CLI does. argparse's required=True on --webhook-id
    and the cross-flag check are the source of truth at [parse_args()] — keep
    in sync."""
    if not args.webhook_id:
        raise SystemExit("webhook_id is required.")
    has_explicit_jobs = bool(args.job_id) or bool(args.job_ids_from)
    has_filters = bool(args.tag) or bool(args.owner)
    if has_explicit_jobs and has_filters:
        raise SystemExit(
            "--tag/--owner filter the workspace walk; they don't combine with "
            "explicit --job-id/--job-ids-from. Drop the filters, or drop the explicit job list."
        )


def run_by_id_mode(
    args: argparse.Namespace,
    w: WorkspaceClient,
    parsed_events: List[str],
    stats: Stats,
) -> int:
    """Per-job-id attach path. Explicit job IDs, no workspace listing, no filters.

    `scan_limit`/`name_filter` don't apply here (there's no walk to cap). `limit`
    still caps mutations so a long --job-ids-from list can be staged."""
    job_ids = _resolve_job_ids(args)
    logging.info(
        "Mode=%s op=ADD-BY-ID webhook=%s events=%s job_count=%d",
        "APPLY" if args.apply else "DRY-RUN",
        args.webhook_id, parsed_events, len(job_ids),
    )
    for job_id in job_ids:
        stats.scanned += 1
        process_job_by_id(w, job_id, args.webhook_id, parsed_events, args.apply, args.max_retries, stats)

        if args.apply:
            time.sleep(args.base_sleep + random.uniform(0, args.jitter))

        if args.limit is not None and (stats.updated + stats.would_update) >= args.limit:
            logging.info("Reached --limit=%d, stopping.", args.limit)
            break

    logging.info(
        "Done. op=ADD-BY-ID scanned=%d matched=%d already_attached=%d bundle_skipped=%d "
        "would_update=%d updated=%d errored=%d",
        stats.scanned, stats.matched, stats.already_attached, stats.bundle_skipped,
        stats.would_update, stats.updated, stats.errored,
    )
    return 1 if stats.errored else 0


def run_walk_mode(
    args: argparse.Namespace,
    w: WorkspaceClient,
    parsed_events: List[str],
    stats: Stats,
) -> int:
    """Workspace-walk attach path: list -> filter -> bundle skip -> attach."""
    filters = parse_filters(args)

    logging.info(
        "Mode=%s op=ADD-WALK webhook=%s events=%s tag=%s owners=%s limit=%s scan_limit=%s name_filter=%s",
        "APPLY" if args.apply else "DRY-RUN",
        args.webhook_id, parsed_events,
        f"{filters.tag_key}={filters.tag_value}" if filters.tag_key else None,
        filters.owners or None,
        args.limit,
        args.scan_limit,
        args.name_filter,
    )

    list_kwargs = {"expand_tasks": False}
    if args.name_filter:
        list_kwargs["name"] = args.name_filter

    for job in w.jobs.list(**list_kwargs):
        stats.scanned += 1
        if args.progress_every and stats.scanned % args.progress_every == 0:
            logging.info(
                "...scanned=%d matched=%d would_update=%d updated=%d bundle_skipped=%d",
                stats.scanned, stats.matched, stats.would_update, stats.updated, stats.bundle_skipped,
            )
        if args.scan_limit is not None and stats.scanned >= args.scan_limit:
            logging.info("Reached scan_limit=%d, stopping scan.", args.scan_limit)
            break
        if not job_matches(job, filters):
            continue
        stats.matched += 1

        name = (job.settings.name if job.settings else None) or "<unnamed>"
        bundle, _meta_path = is_bundle_job(job)
        if bundle:
            stats.bundle_skipped += 1
            logging.info(
                "Job %s (%s): SKIP bundle-managed (use patch_bundle_yaml for DAB jobs).",
                job.job_id, name,
            )
            continue

        process_job(w, job, args.webhook_id, parsed_events, args.apply, args.max_retries, stats)

        if args.apply:
            time.sleep(args.base_sleep + random.uniform(0, args.jitter))

        if args.limit is not None and (stats.updated + stats.would_update) >= args.limit:
            logging.info("Reached --limit=%d, stopping.", args.limit)
            break

    logging.info(
        "Done. op=ADD-WALK scanned=%d matched=%d already_attached=%d bundle_skipped=%d would_update=%d updated=%d errored=%d",
        stats.scanned, stats.matched, stats.already_attached, stats.bundle_skipped,
        stats.would_update, stats.updated, stats.errored,
    )
    return 1 if stats.errored else 0


def run(
    webhook_id: Optional[str] = None,
    events: str = "on_failure,on_duration_warning_threshold_exceeded",
    job_id: Optional[List[int]] = None,
    job_ids_from: Optional[str] = None,
    tag: Optional[str] = None,
    owner: Optional[List[str]] = None,
    apply: bool = False,
    profile: Optional[str] = None,
    max_retries: int = 5,
    base_sleep: float = 0.3,
    jitter: float = 0.4,
    limit: Optional[int] = None,
    progress_every: int = 500,
    verbose: bool = False,
    client=None,
    scan_limit: Optional[int] = None,
    name_filter: Optional[str] = None,
    workspace_label: Optional[str] = None,
) -> int:
    """Library entry point. Notebooks import this and map widgets → kwargs.

    Kwargs mirror `parse_args()` 1:1 for the CLI shape; notebook-only kwargs
    follow (`client`, `scan_limit`, `name_filter`, `workspace_label`).

    Two shapes, picked by whether `job_id`/`job_ids_from` is set:
    per-job-id attach (explicit IDs, no walk, no filters) vs workspace-walk
    attach. `job_id`/`job_ids_from` are mutually exclusive with `tag`/`owner`.

    `limit` (mutation cap) and `scan_limit` (scan cap) are distinct: `limit`
    stops the loop once N jobs would-update / updated; `scan_limit` stops the
    walk after N jobs scanned regardless of matches. When matches are sparse,
    `limit` alone won't shorten the scan — set `scan_limit` for that.
    (`scan_limit`/`name_filter` are walk-mode only — there's no walk to cap in
    per-job-id mode.)

    DAB-managed jobs are always skipped (counted in `stats.bundle_skipped`),
    in BOTH shapes — even an explicitly-passed bundle job ID is skipped. There
    is no `--bundle-jobs` flag — that escape hatch was removed because API edits
    to bundle jobs are non-durable across `databricks bundle deploy`. Use
    `patch_bundle_yaml.py` for the bundle path."""
    args = argparse.Namespace(
        webhook_id=webhook_id,
        events=events,
        job_id=list(job_id or []),
        job_ids_from=job_ids_from,
        tag=tag,
        owner=list(owner or []),
        apply=apply,
        profile=profile,
        max_retries=max_retries,
        base_sleep=base_sleep,
        jitter=jitter,
        limit=limit,
        progress_every=progress_every,
        verbose=verbose,
        scan_limit=scan_limit,
        name_filter=name_filter,
        workspace_label=workspace_label,
    )
    _validate_run_kwargs(args)
    setup_logging(args.verbose)

    w = client if client is not None else build_client(args.profile)
    args.workspace_label = args.workspace_label or w.config.host
    stats = Stats()

    parsed_events = parse_events(args.events)

    if args.job_id or args.job_ids_from:
        return run_by_id_mode(args, w, parsed_events, stats)
    return run_walk_mode(args, w, parsed_events, stats)


def main() -> int:
    return run(**vars(parse_args()))


if __name__ == "__main__":
    sys.exit(main())
