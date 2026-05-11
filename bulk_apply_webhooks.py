#!/usr/bin/env python3
"""Bulk-attach a Databricks webhook notification destination to jobs in a workspace.

Auth follows the standard Databricks SDK credential chain:
  - env vars DATABRICKS_HOST + DATABRICKS_TOKEN, or
  - DATABRICKS_CONFIG_PROFILE / --profile, or
  - OAuth via `databricks auth login`.

Examples:
  # Dry-run against every job in the workspace
  python bulk_apply_webhooks.py --webhook-id 0123abcd

  # Apply to jobs tagged team=platform
  python bulk_apply_webhooks.py --webhook-id 0123abcd --tag team=platform --apply

  # Staged rollout: cap to 25 owner-filtered jobs, then re-run for the rest
  python bulk_apply_webhooks.py --webhook-id 0123abcd \
      --owner alice@example.com --owner bob@example.com --limit 25 --apply
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


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--webhook-id", required=True, help="Notification destination (webhook) ID to attach.")
    p.add_argument(
        "--events",
        default="on_failure,on_success,on_start",
        help=f"Comma-separated event list. Valid: {', '.join(EVENT_FIELDS)}.",
    )
    p.add_argument("--tag", help="Filter by job tag. Format: key=value, or just key for presence-only.")
    p.add_argument(
        "--owner",
        action="append",
        default=[],
        help="Filter by creator_user_name. Repeatable for multiple owners.",
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
        help="CSV path to write the list of bundle-managed jobs encountered. Set to '' to disable.",
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
    return p.parse_args()


def setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def parse_filters(args: argparse.Namespace) -> Filters:
    tag_key = tag_value = None
    if args.tag:
        if "=" in args.tag:
            tag_key, tag_value = args.tag.split("=", 1)
        else:
            tag_key = args.tag
    return Filters(tag_key=tag_key, tag_value=tag_value, owners=list(args.owner))


def parse_events(s: str) -> List[str]:
    events = [e.strip() for e in s.split(",") if e.strip()]
    unknown = [e for e in events if e not in EVENT_FIELDS]
    if unknown:
        raise SystemExit(f"Unknown event(s): {unknown}. Valid: {EVENT_FIELDS}")
    return events


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
        # Some metadata variants put fields at top level; try both.
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


def main() -> int:
    args = parse_args()
    setup_logging(args.verbose)

    events = parse_events(args.events)
    filters = parse_filters(args)

    w = build_client(args.profile)
    stats = Stats()
    bundle_records: List[BundleJobRecord] = []
    bundle_meta_cache: Dict[str, Optional[BundleMetadata]] = {}

    logging.info(
        "Mode=%s webhook=%s events=%s tag=%s owners=%s bundle_jobs=%s limit=%s",
        "APPLY" if args.apply else "DRY-RUN",
        args.webhook_id, events,
        f"{filters.tag_key}={filters.tag_value}" if filters.tag_key else None,
        filters.owners or None,
        args.bundle_jobs,
        args.limit,
    )

    for job in w.jobs.list(expand_tasks=False):
        stats.scanned += 1
        if args.progress_every and stats.scanned % args.progress_every == 0:
            logging.info(
                "...scanned=%d matched=%d would_update=%d updated=%d bundle_skipped=%d",
                stats.scanned, stats.matched, stats.would_update, stats.updated, stats.bundle_skipped,
            )
        if not job_matches(job, filters):
            continue
        stats.matched += 1

        bundle, meta_path = is_bundle_job(job)
        name = (job.settings.name if job.settings else None) or "<unnamed>"

        if bundle:
            md = fetch_bundle_metadata(w, meta_path, bundle_meta_cache)
            bundle_records.append(BundleJobRecord(
                job_id=job.job_id, name=name,
                metadata_file_path=meta_path, creator=job.creator_user_name,
                metadata=md,
            ))
            if args.bundle_jobs == "skip":
                stats.bundle_skipped += 1
                logging.info("Job %s (%s): SKIP bundle-managed (deploy will clobber API edits). meta=%s",
                             job.job_id, name, meta_path)
                continue
        else:
            if args.bundle_jobs == "only":
                logging.debug("Job %s (%s): skipping non-bundle job (--bundle-jobs=only).", job.job_id, name)
                continue

        process_job(w, job, args.webhook_id, events, args.apply, args.max_retries, stats)

        if args.apply:
            time.sleep(args.base_sleep + random.uniform(0, args.jitter))

        if args.limit is not None and (stats.updated + stats.would_update) >= args.limit:
            logging.info("Reached --limit=%d, stopping.", args.limit)
            break

    write_bundle_report(args.bundle_report, bundle_records)

    logging.info(
        "Done. scanned=%d matched=%d already_attached=%d bundle_skipped=%d would_update=%d updated=%d errored=%d bundle_total=%d",
        stats.scanned, stats.matched, stats.already_attached, stats.bundle_skipped,
        stats.would_update, stats.updated, stats.errored, len(bundle_records),
    )
    return 1 if stats.errored else 0


if __name__ == "__main__":
    sys.exit(main())
