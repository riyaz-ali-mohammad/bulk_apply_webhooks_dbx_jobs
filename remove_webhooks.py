#!/usr/bin/env python3
"""Remove (detach) a Databricks webhook notification destination from jobs.

Companion to apply_webhooks_to_direct_jobs.py — separates the rollback /
detach path from the bulk-attach path so each script has a single, clear
purpose. Unlike the attach script, this one CAN detach from DAB-managed jobs
(useful for cleaning up stale references before re-patching via
patch_bundle_yaml); see `--bundle-jobs` below.

Three shapes (the script picks based on which flags are passed):
  (1) Per-job rollback: pair with --job-id / --job-ids-from to detach from
      explicit job IDs. --webhook-id is OPTIONAL — omit it to clear ALL
      webhook_notifications on those jobs.
  (2) Workspace-walk rollback: omit --job-id; --webhook-id is REQUIRED. Walks
      the workspace honoring --tag/--owner/--bundle-jobs and removes only
      that destination from every matching job that currently has it.
  (3) Bundle-managed jobs in per-job mode proceed with a WARNING (API edits
      are non-durable — `databricks bundle deploy` will re-add the webhook
      unless the bundle YAML is also patched via patch_bundle_yaml.py). In
      walk mode they follow --bundle-jobs (default `skip`).

Default is dry-run. Pass --apply to actually call jobs.update.

Auth follows the standard Databricks SDK credential chain.

Examples:
  # Remove all webhooks from specific jobs
  python remove_webhooks.py --job-id 1234 --job-id 5678 --apply

  # Remove only one destination from specific jobs (others survive)
  python remove_webhooks.py --job-id 1234 --webhook-id 0123abcd --apply

  # Workspace-wide rollback: detach the destination from every job that has it
  # (--webhook-id is REQUIRED in this shape; --tag/--owner/--bundle-jobs honored)
  python remove_webhooks.py --webhook-id 0123abcd --apply

  # CSV-driven rollback: read job IDs from a file (first column; header auto-skipped)
  python remove_webhooks.py --webhook-id 0123abcd \
      --job-ids-from rollback.txt --apply
"""

import argparse
import csv
import json
import logging
import random
import sys
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.jobs import (
    BaseJob,
    JobSettings,
    Webhook,
    WebhookNotifications,
)


# Event fields the Jobs API recognizes on `webhook_notifications`.
# Duplicated with apply_webhooks_to_direct_jobs.py per the project's "no shared module
# between the four scripts" convention (see CLAUDE.md).
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


@dataclass
class BundleMetadata:
    bundle_name: Optional[str] = None
    target: Optional[str] = None
    git_origin: Optional[str] = None
    git_branch: Optional[str] = None
    git_commit: Optional[str] = None
    workspace_root: Optional[str] = None
    workspace_file_path: Optional[str] = None


@dataclass
class BundleJobRecord:
    job_id: int
    name: str
    metadata_file_path: Optional[str]
    creator: Optional[str]
    metadata: Optional[BundleMetadata] = None


@dataclass
class RemovalRecord:
    """One row of the audit log for a successful webhook removal.

    Built only after `w.jobs.update` returns OK (apply mode); dry-runs do not
    produce rows. `bundle` / `metadata_file_path` are populated for bundle-managed
    jobs and left None for direct jobs."""
    job_id: int
    name: str
    creator: Optional[str]
    is_bundle: bool
    webhook_ids_removed: List[str]
    events_affected: List[str]
    metadata_file_path: Optional[str] = None
    bundle: Optional[BundleMetadata] = None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "--webhook-id",
        help=(
            "Notification destination (webhook) ID to detach. "
            "Required for workspace-walk mode (no --job-id/--job-ids-from). "
            "Optional in per-job mode: when given, only that destination is removed; "
            "when omitted, ALL webhook_notifications are cleared from the targeted jobs."
        ),
    )
    p.add_argument(
        "--job-id",
        action="append",
        default=[],
        type=int,
        help=(
            "Job ID to operate on (per-job rollback). Repeatable. "
            "Mutually exclusive with --tag/--owner filters."
        ),
    )
    p.add_argument(
        "--job-ids-from",
        help=(
            "Path to a text or CSV file containing job IDs, one per line "
            "(first column if CSV). Header row auto-detected. Combines with --job-id. "
            "Composes with jobs_inventory.csv / bundle_jobs.csv via `awk -F, ...`."
        ),
    )
    p.add_argument(
        "--tag",
        help=(
            "Filter by job tag. Format: key=value, or just key for presence-only. "
            "Used in workspace-walk mode (no --job-id)."
        ),
    )
    p.add_argument(
        "--owner",
        action="append",
        default=[],
        help=(
            "Filter by creator_user_name. Repeatable for multiple owners. "
            "Used in workspace-walk mode (no --job-id)."
        ),
    )
    p.add_argument(
        "--bundle-jobs",
        choices=("skip", "include", "only"),
        default="skip",
        help=(
            "Policy for Asset-Bundle-deployed jobs (settings.deployment.kind == BUNDLE). "
            "skip (default): leave them alone, since `bundle deploy` will overwrite API edits. "
            "include: write through anyway (rare; you must also patch the bundle YAML). "
            "only: process bundle jobs exclusively (useful for auditing)."
        ),
    )
    p.add_argument(
        "--bundle-report",
        default="bundle_jobs.csv",
        help="CSV path to write the list of bundle-managed jobs encountered (walk mode). Set to '' to disable.",
    )
    p.add_argument("--apply", action="store_true", help="Actually call jobs/update. Default is dry-run.")
    p.add_argument("--profile", help="Databricks CLI profile name.")
    p.add_argument("--max-retries", type=int, default=5, help="Max retries on 429/5xx.")
    p.add_argument("--base-sleep", type=float, default=0.3, help="Base sleep (s) between updates for pacing.")
    p.add_argument("--jitter", type=float, default=0.4, help="Max random jitter (s) added per update.")
    p.add_argument("--limit", type=int, help="Cap on number of jobs to update in this run (walk mode).")
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
    if not has_explicit_jobs and not args.webhook_id:
        p.error(
            "workspace-walk mode (no --job-id/--job-ids-from) REQUIRES --webhook-id "
            "(refusing to clear every webhook in the workspace from one CLI call)."
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


def _dig(d, *keys):
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def fetch_bundle_metadata(
    w: WorkspaceClient,
    path: Optional[str],
    cache: Dict[str, Optional[BundleMetadata]],
) -> Optional[BundleMetadata]:
    """Read and parse the bundle deployment metadata JSON from the workspace. Cached per-path."""
    if not path:
        return None
    if path in cache:
        return cache[path]
    try:
        with w.workspace.download(path) as fh:
            data = json.load(fh)
    except Exception as e:
        logging.warning("Could not read bundle metadata at %s: %s", path, e)
        cache[path] = None
        return None

    cfg = data.get("config") if isinstance(data, dict) else None
    if not isinstance(cfg, dict):
        cfg = data if isinstance(data, dict) else {}

    md = BundleMetadata(
        bundle_name=_dig(cfg, "bundle", "name"),
        target=_dig(cfg, "bundle", "target"),
        git_origin=_dig(cfg, "bundle", "git", "origin_url") or _dig(cfg, "bundle", "git", "OriginURL"),
        git_branch=_dig(cfg, "bundle", "git", "branch") or _dig(cfg, "bundle", "git", "Branch"),
        git_commit=_dig(cfg, "bundle", "git", "commit") or _dig(cfg, "bundle", "git", "Commit"),
        workspace_root=_dig(cfg, "workspace", "root_path"),
        workspace_file_path=_dig(cfg, "workspace", "file_path"),
    )
    cache[path] = md
    return md


def is_bundle_job(job: BaseJob) -> Tuple[bool, Optional[str]]:
    """Return (is_bundle, metadata_file_path). Detected via settings.deployment.kind == BUNDLE."""
    s = job.settings
    if not s or not getattr(s, "deployment", None):
        return False, None
    kind = getattr(s.deployment, "kind", None)
    kind_str = getattr(kind, "value", None) or str(kind) if kind is not None else ""
    is_bundle = "BUNDLE" in kind_str.upper()
    return is_bundle, getattr(s.deployment, "metadata_file_path", None)


def write_bundle_report(path: str, records: List[BundleJobRecord]) -> None:
    if not path or not records:
        return
    columns = [
        "job_id", "name", "creator",
        "bundle_name", "target",
        "git_origin", "git_branch", "git_commit",
        "workspace_root", "workspace_file_path",
        "metadata_file_path",
    ]
    with open(path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(columns)
        for r in records:
            md = r.metadata or BundleMetadata()
            writer.writerow([
                r.job_id, r.name, r.creator or "",
                md.bundle_name or "", md.target or "",
                md.git_origin or "", md.git_branch or "", md.git_commit or "",
                md.workspace_root or "", md.workspace_file_path or "",
                r.metadata_file_path or "",
            ])
    logging.info("Wrote %d bundle-job records to %s", len(records), path)


def write_removal_log_delta(spark, table: str, records: List["RemovalRecord"], workspace_label: str) -> None:
    """Append removal-audit rows to a Delta table partitioned by workspace_host.

    This is an audit log — every row records one successful `jobs.update` that
    detached at least one webhook. Mode is `append` (not overwrite): re-running
    the rollback later adds new rows; it never clobbers history. `is_bundle=true`
    rows carry bundle metadata (name, target, git, workspace path); `is_bundle=false`
    rows leave those columns empty for direct jobs."""
    if not records:
        logging.info("No removal records to write to %s.", table)
        return
    import datetime
    deleted_at = datetime.datetime.now(datetime.timezone.utc)
    rows = []
    for r in records:
        md = r.bundle or BundleMetadata()
        rows.append({
            "workspace_host": workspace_label,
            "deleted_at": deleted_at,
            "job_id": r.job_id,
            "name": r.name,
            "creator": r.creator or "",
            "is_bundle": r.is_bundle,
            "webhook_ids_removed": list(r.webhook_ids_removed),
            "events_affected": list(r.events_affected),
            "bundle_name": md.bundle_name or "",
            "target": md.target or "",
            "git_origin": md.git_origin or "",
            "git_branch": md.git_branch or "",
            "git_commit": md.git_commit or "",
            "workspace_root": md.workspace_root or "",
            "workspace_file_path": md.workspace_file_path or "",
            "metadata_file_path": r.metadata_file_path or "",
        })
    df = spark.createDataFrame(rows)
    (df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("workspace_host")
        .saveAsTable(table))
    logging.info("Appended %d removal records to Delta table %s (workspace_host=%s)",
                 len(records), table, workspace_label)


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


def webhook_attached(existing: Optional[WebhookNotifications], webhook_id: str) -> bool:
    """True if webhook_id appears on at least one event list in `existing`.
    Used by the workspace-walk rollback to skip jobs that don't have the destination."""
    if not existing:
        return False
    for ev in EVENT_FIELDS:
        cur = getattr(existing, ev, None) or []
        if any(w.id == webhook_id for w in cur):
            return True
    return False


def load_job_ids_from_file(path: str) -> List[int]:
    """Read job IDs from a text or CSV file (first column).

    Tolerates: blank lines, surrounding whitespace, BOM on the first cell,
    and a CSV header row (auto-detected when the first cell isn't an int —
    so `jobs_inventory.csv` / `bundle_jobs.csv` can be piped in directly).
    Raises SystemExit on empty input or a non-numeric mid-file cell."""
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


def remove_webhooks(
    existing: Optional[WebhookNotifications],
    webhook_id: Optional[str],
) -> Tuple[WebhookNotifications, int, List[str], List[str]]:
    """Compute the new webhook_notifications block plus an audit summary.

    Returns (new_notifications, count_removed, ids_removed, events_affected).
    `ids_removed` is deduped (first-occurrence order) — useful when webhook_id is
    None and a single job had several distinct destinations cleared. Events that
    end up empty are sent as [] so the API actually clears the list rather than
    treating absent == no-change."""
    if not existing:
        return WebhookNotifications(), 0, [], []
    kwargs = {}
    removed = 0
    ids_removed: List[str] = []
    events_affected: List[str] = []
    seen_ids = set()
    for ev in EVENT_FIELDS:
        cur = list(getattr(existing, ev, None) or [])
        if not cur:
            continue
        if webhook_id is None:
            removed += len(cur)
            kwargs[ev] = []
            events_affected.append(ev)
            for wh in cur:
                if wh.id and wh.id not in seen_ids:
                    seen_ids.add(wh.id)
                    ids_removed.append(wh.id)
        else:
            kept = [wh for wh in cur if wh.id != webhook_id]
            if len(kept) != len(cur):
                removed += len(cur) - len(kept)
                kwargs[ev] = kept
                events_affected.append(ev)
                if webhook_id not in seen_ids:
                    seen_ids.add(webhook_id)
                    ids_removed.append(webhook_id)
            else:
                kwargs[ev] = cur
    return WebhookNotifications(**kwargs), removed, ids_removed, events_affected


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


def _apply_remove_to_job(
    w: WorkspaceClient,
    job: BaseJob,
    webhook_id: Optional[str],
    apply: bool,
    max_retries: int,
    stats: Stats,
    *,
    removal_records: Optional[List[RemovalRecord]] = None,
    bundle_meta: Optional[BundleMetadata] = None,
    bundle_meta_path: Optional[str] = None,
) -> None:
    """Compute the new webhook_notifications and (if --apply) call jobs.update.

    Caller is responsible for bundle-policy decisions / WARNINGs and any pre-filter
    (e.g. walk-mode's webhook_attached check). This function only touches `stats`
    for the outcomes of the remove step itself.

    If `removal_records` is passed, a `RemovalRecord` is appended on each
    successful `jobs.update` — dry-run rows are intentionally not recorded
    (the audit log captures actual deletions, not previews)."""
    job_id = job.job_id
    name = (job.settings.name if job.settings else None) or "<unnamed>"

    existing = job.settings.webhook_notifications if job.settings else None
    if not existing:
        stats.already_attached += 1
        logging.info("Job %s (%s): no webhook_notifications attached; nothing to remove.",
                     job_id, name)
        return

    new_wh, count, ids_removed, events_affected = remove_webhooks(existing, webhook_id)
    if count == 0:
        stats.already_attached += 1
        logging.info("Job %s (%s): webhook %s not currently attached; no-op.",
                     job_id, name, webhook_id)
        return

    logging.info(
        "Job %s (%s): %s removing %d webhook entr%s (filter=%s)",
        job_id, name,
        "applying" if apply else "DRY-RUN would apply",
        count, "y" if count == 1 else "ies",
        webhook_id or "<all>",
    )

    if not apply:
        stats.would_update += 1
        return

    try:
        call_with_backoff(
            lambda: w.jobs.update(
                job_id=job_id,
                new_settings=JobSettings(webhook_notifications=new_wh),
            ),
            max_retries=max_retries,
        )
        stats.updated += 1
        if removal_records is not None:
            is_bundle, _ = is_bundle_job(job)
            removal_records.append(RemovalRecord(
                job_id=job_id,
                name=name,
                creator=job.creator_user_name,
                is_bundle=is_bundle,
                webhook_ids_removed=ids_removed,
                events_affected=events_affected,
                metadata_file_path=bundle_meta_path,
                bundle=bundle_meta,
            ))
    except Exception as e:
        stats.errored += 1
        logging.error("Job %s (%s): remove failed: %s", job_id, name, e)


def process_remove_job(
    w: WorkspaceClient,
    job_id: int,
    webhook_id: Optional[str],
    apply: bool,
    max_retries: int,
    stats: Stats,
    *,
    removal_records: Optional[List[RemovalRecord]] = None,
    bundle_meta_cache: Optional[Dict[str, Optional[BundleMetadata]]] = None,
) -> None:
    """Per-job-id remove: looks the job up, WARNs if bundle-managed, then delegates
    to `_apply_remove_to_job`. Bundle jobs proceed because each job-id is explicit
    here — the user chose to target them.

    When `removal_records` is set, bundle metadata is fetched for bundle-managed
    jobs so the audit row carries bundle/git context."""
    try:
        job = call_with_backoff(lambda: w.jobs.get(job_id=job_id), max_retries=max_retries)
    except Exception as e:
        stats.errored += 1
        logging.error("Job %s: lookup failed: %s", job_id, e)
        return

    name = (job.settings.name if job.settings else None) or "<unnamed>"
    stats.matched += 1

    bundle, meta_path = is_bundle_job(job)
    bundle_meta: Optional[BundleMetadata] = None
    if bundle:
        logging.warning(
            "Job %s (%s): bundle-managed — API removal is non-durable; next `bundle deploy` "
            "will re-add the webhook unless the bundle YAML is also patched.",
            job_id, name,
        )
        if removal_records is not None and bundle_meta_cache is not None and meta_path:
            bundle_meta = fetch_bundle_metadata(w, meta_path, bundle_meta_cache)

    _apply_remove_to_job(
        w, job, webhook_id, apply, max_retries, stats,
        removal_records=removal_records,
        bundle_meta=bundle_meta,
        bundle_meta_path=meta_path,
    )


def _resolve_remove_job_ids(args: argparse.Namespace) -> List[int]:
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


def run_remove_mode(args: argparse.Namespace, w: WorkspaceClient, stats: Stats) -> int:
    """Per-job-id rollback path. Explicit job IDs, no workspace listing, no filters."""
    job_ids = _resolve_remove_job_ids(args)
    delta_table = getattr(args, "delta_table", None)
    spark = getattr(args, "spark", None)
    workspace_label = getattr(args, "workspace_label", None) or w.config.host

    removal_records: List[RemovalRecord] = []
    bundle_meta_cache: Dict[str, Optional[BundleMetadata]] = {}

    logging.info(
        "Mode=%s op=REMOVE webhook_filter=%s job_count=%d",
        "APPLY" if args.apply else "DRY-RUN",
        args.webhook_id or "<all>",
        len(job_ids),
    )
    for job_id in job_ids:
        stats.scanned += 1
        process_remove_job(
            w, job_id, args.webhook_id, args.apply, args.max_retries, stats,
            removal_records=removal_records if (delta_table and args.apply) else None,
            bundle_meta_cache=bundle_meta_cache if (delta_table and args.apply) else None,
        )
        if args.apply:
            time.sleep(args.base_sleep + random.uniform(0, args.jitter))

    if delta_table:
        if not args.apply:
            logging.info("Dry-run: skipping write to Delta table %s (audit log is apply-only).",
                         delta_table)
        elif spark is None:
            raise SystemExit("delta_table requires a `spark` kwarg (the notebook's SparkSession).")
        else:
            write_removal_log_delta(spark, delta_table, removal_records, workspace_label)

    logging.info(
        "Done. op=REMOVE scanned=%d matched=%d nothing_to_remove=%d would_update=%d updated=%d errored=%d",
        stats.scanned, stats.matched, stats.already_attached,
        stats.would_update, stats.updated, stats.errored,
    )
    return 1 if stats.errored else 0


def run_remove_walk_mode(args: argparse.Namespace, w: WorkspaceClient, stats: Stats) -> int:
    """Workspace-walk rollback path. Mirrors add mode's main loop:
    list -> filter -> bundle policy -> mutate. Pre-filters via `webhook_attached`
    so jobs that don't currently have the destination get a DEBUG line and skip
    the API call.

    Two independent output streams: `bundle_records` feeds the CLI's
    `--bundle-report` CSV (inventory of bundle-managed jobs encountered, regardless
    of whether removal happened), and `removal_records` feeds the notebook's
    Delta audit log (one row per successful `jobs.update`, apply-only)."""
    filters = parse_filters(args)
    bundle_records: List[BundleJobRecord] = []
    bundle_meta_cache: Dict[str, Optional[BundleMetadata]] = {}
    removal_records: List[RemovalRecord] = []

    delta_table = getattr(args, "delta_table", None)
    spark = getattr(args, "spark", None)
    workspace_label = getattr(args, "workspace_label", None) or w.config.host
    track_removals = bool(delta_table and args.apply)

    scan_limit = getattr(args, "scan_limit", None)
    name_filter = getattr(args, "name_filter", None)
    logging.info(
        "Mode=%s op=REMOVE-WALK webhook=%s tag=%s owners=%s bundle_jobs=%s limit=%s scan_limit=%s name_filter=%s",
        "APPLY" if args.apply else "DRY-RUN",
        args.webhook_id,
        f"{filters.tag_key}={filters.tag_value}" if filters.tag_key else None,
        filters.owners or None,
        args.bundle_jobs,
        args.limit,
        scan_limit,
        name_filter,
    )

    list_kwargs = {"expand_tasks": False}
    if name_filter:
        list_kwargs["name"] = name_filter

    for job in w.jobs.list(**list_kwargs):
        stats.scanned += 1
        if args.progress_every and stats.scanned % args.progress_every == 0:
            logging.info(
                "...scanned=%d matched=%d would_update=%d updated=%d bundle_skipped=%d",
                stats.scanned, stats.matched, stats.would_update, stats.updated, stats.bundle_skipped,
            )
        if scan_limit is not None and stats.scanned >= scan_limit:
            logging.info("Reached scan_limit=%d, stopping scan.", scan_limit)
            break
        if not job_matches(job, filters):
            continue
        stats.matched += 1

        bundle, meta_path = is_bundle_job(job)
        name = (job.settings.name if job.settings else None) or "<unnamed>"
        bundle_meta: Optional[BundleMetadata] = None

        if bundle:
            bundle_meta = fetch_bundle_metadata(w, meta_path, bundle_meta_cache)
            bundle_records.append(BundleJobRecord(
                job_id=job.job_id, name=name,
                metadata_file_path=meta_path, creator=job.creator_user_name,
                metadata=bundle_meta,
            ))
            if args.bundle_jobs == "skip":
                stats.bundle_skipped += 1
                logging.info("Job %s (%s): SKIP bundle-managed (deploy will re-add API removals). meta=%s",
                             job.job_id, name, meta_path)
                continue
            if args.bundle_jobs == "include":
                logging.warning(
                    "Job %s (%s): bundle-managed — API removal is non-durable; next `bundle deploy` "
                    "will re-add the webhook unless the bundle YAML is also patched.",
                    job.job_id, name,
                )
        else:
            if args.bundle_jobs == "only":
                logging.debug("Job %s (%s): skipping non-bundle job (--bundle-jobs=only).", job.job_id, name)
                continue

        existing = job.settings.webhook_notifications if job.settings else None
        if not webhook_attached(existing, args.webhook_id):
            stats.already_attached += 1
            logging.debug("Job %s (%s): webhook %s not currently attached; skipping.",
                          job.job_id, name, args.webhook_id)
            continue

        _apply_remove_to_job(
            w, job, args.webhook_id, args.apply, args.max_retries, stats,
            removal_records=removal_records if track_removals else None,
            bundle_meta=bundle_meta,
            bundle_meta_path=meta_path,
        )

        if args.apply:
            time.sleep(args.base_sleep + random.uniform(0, args.jitter))

        if args.limit is not None and (stats.updated + stats.would_update) >= args.limit:
            logging.info("Reached --limit=%d, stopping.", args.limit)
            break

    if delta_table:
        if not args.apply:
            logging.info("Dry-run: skipping write to Delta table %s (audit log is apply-only).",
                         delta_table)
        elif spark is None:
            raise SystemExit("delta_table requires a `spark` kwarg (the notebook's SparkSession).")
        else:
            write_removal_log_delta(spark, delta_table, removal_records, workspace_label)
    if args.bundle_report:
        write_bundle_report(args.bundle_report, bundle_records)

    logging.info(
        "Done. op=REMOVE-WALK scanned=%d matched=%d nothing_to_remove=%d bundle_skipped=%d "
        "would_update=%d updated=%d errored=%d bundle_total=%d",
        stats.scanned, stats.matched, stats.already_attached, stats.bundle_skipped,
        stats.would_update, stats.updated, stats.errored, len(bundle_records),
    )
    return 1 if stats.errored else 0


def _validate_run_kwargs(args: argparse.Namespace) -> None:
    """Re-implement the CLI's post-parse cross-flag validation so that notebook
    callers fail the same way the CLI does. argparse's `p.error` is the source
    of truth at [parse_args()] — keep these checks in sync."""
    has_explicit_jobs = bool(args.job_id) or bool(args.job_ids_from)
    has_filters = bool(args.tag) or bool(args.owner)
    if has_explicit_jobs and has_filters:
        raise SystemExit(
            "--tag/--owner filter the workspace walk; they don't combine with "
            "explicit --job-id/--job-ids-from. Drop the filters, or drop the explicit job list."
        )
    if not has_explicit_jobs and not args.webhook_id:
        raise SystemExit(
            "workspace-walk mode (no --job-id/--job-ids-from) REQUIRES --webhook-id "
            "(refusing to clear every webhook in the workspace from one CLI call)."
        )


def run(
    webhook_id: Optional[str] = None,
    job_id: Optional[List[int]] = None,
    job_ids_from: Optional[str] = None,
    tag: Optional[str] = None,
    owner: Optional[List[str]] = None,
    bundle_jobs: str = "skip",
    bundle_report: str = "bundle_jobs.csv",
    apply: bool = False,
    profile: Optional[str] = None,
    max_retries: int = 5,
    base_sleep: float = 0.3,
    jitter: float = 0.4,
    limit: Optional[int] = None,
    progress_every: int = 500,
    verbose: bool = False,
    client=None,
    spark=None,
    delta_table: Optional[str] = None,
    scan_limit: Optional[int] = None,
    name_filter: Optional[str] = None,
    workspace_label: Optional[str] = None,
) -> int:
    """Library entry point. Notebooks import this and map widgets → kwargs.

    Kwargs mirror `parse_args()` 1:1 for the CLI shape; notebook-only kwargs
    follow (`client`, `spark`, `delta_table`, `scan_limit`, `name_filter`,
    `workspace_label`). See the matching docstring on `inventory_jobs.run`.

    `delta_table` is an apply-only audit log: one row per successful
    `jobs.update`, with `is_bundle` + bundle metadata for bundle-managed jobs.
    Both per-job and walk modes write to it. Dry-runs skip the write entirely.
    `mode=append` so re-running the rollback adds rows, never clobbers history.

    `limit` (mutation cap) vs `scan_limit` (scan cap): `limit` stops the walk
    once N jobs have been would-updated / updated; `scan_limit` stops the walk
    after N jobs scanned regardless of matches. When matches are sparse,
    `limit` alone won't shorten the scan — set `scan_limit` for that."""
    args = argparse.Namespace(
        webhook_id=webhook_id,
        job_id=list(job_id or []),
        job_ids_from=job_ids_from,
        tag=tag,
        owner=list(owner or []),
        bundle_jobs=bundle_jobs,
        bundle_report=bundle_report,
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
        delta_table=delta_table,
        spark=spark,
        workspace_label=workspace_label,
    )
    _validate_run_kwargs(args)
    setup_logging(args.verbose)

    w = client if client is not None else build_client(args.profile)
    args.workspace_label = args.workspace_label or w.config.host
    stats = Stats()

    if args.job_id or args.job_ids_from:
        return run_remove_mode(args, w, stats)
    return run_remove_walk_mode(args, w, stats)


def main() -> int:
    return run(**vars(parse_args()))


if __name__ == "__main__":
    sys.exit(main())
