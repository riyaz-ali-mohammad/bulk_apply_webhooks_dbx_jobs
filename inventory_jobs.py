#!/usr/bin/env python3
"""Inventory jobs in a Databricks workspace and split DAB-deployed vs directly-deployed.

Read-only companion to `bulk_apply_webhooks.py`: run this first to size up a workspace
before planning a webhook rollout. The summary tells you how many jobs would be touched
by the bulk script (`DIRECT`) versus how many need a bundle-YAML PR via
`patch_bundle_yaml.py` (`BUNDLE`). A CSV inventory is written for hand-off.

Auth follows the standard Databricks SDK credential chain (env vars, profile, OAuth).

Examples:
  # Quick counts + CSV for the whole workspace
  python3 inventory_jobs.py

  # Same, but also fetch per-bundle metadata (slower; one workspace download per
  # unique bundle). Required to populate bundle_name/target/git_* in the CSV
  # and the top-N bundle breakdown in the summary.
  python3 inventory_jobs.py --enrich-bundles

  # Scope to a cohort (same filter shape as bulk_apply_webhooks.py)
  python3 inventory_jobs.py --tag team=platform --enrich-bundles
"""

import argparse
import csv
import json
import logging
import sys
from collections import Counter
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import BaseJob


@dataclass
class Filters:
    tag_key: Optional[str] = None
    tag_value: Optional[str] = None
    owners: List[str] = field(default_factory=list)


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
class JobRecord:
    job_id: int
    name: str
    creator: Optional[str]
    deployment_kind: str  # "BUNDLE" or "DIRECT"
    metadata_file_path: Optional[str] = None
    metadata: Optional[BundleMetadata] = None


@dataclass
class Stats:
    total: int = 0
    bundle: int = 0
    direct: int = 0
    bundles_by_name: Counter = field(default_factory=Counter)
    creators: Counter = field(default_factory=Counter)


CSV_COLUMNS = [
    "job_id", "name", "creator", "deployment_kind",
    "bundle_name", "target",
    "git_origin", "git_branch", "git_commit",
    "workspace_root", "workspace_file_path",
    "metadata_file_path",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--profile", help="Databricks CLI profile name.")
    p.add_argument("--tag", help="Filter by job tag. Format: key=value, or just key for presence-only.")
    p.add_argument(
        "--owner",
        action="append",
        default=[],
        help="Filter by creator_user_name. Repeatable for multiple owners.",
    )
    p.add_argument(
        "--output",
        default="jobs_inventory.csv",
        help="CSV output path. Pass '' to disable.",
    )
    p.add_argument(
        "--enrich-bundles",
        action="store_true",
        help=(
            "Fetch each unique bundle's deployment metadata from /Workspace to populate "
            "bundle_name/target/git_* in the CSV and the top-N bundle breakdown. "
            "Off by default — adds one workspace API call per unique bundle."
        ),
    )
    p.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="How many bundle names / creators to show in the summary. Default 10.",
    )
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


def write_inventory(path: str, records: List[JobRecord]) -> None:
    if not path or not records:
        return
    with open(path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(CSV_COLUMNS)
        for r in records:
            md = r.metadata or BundleMetadata()
            writer.writerow([
                r.job_id, r.name, r.creator or "", r.deployment_kind,
                md.bundle_name or "", md.target or "",
                md.git_origin or "", md.git_branch or "", md.git_commit or "",
                md.workspace_root or "", md.workspace_file_path or "",
                r.metadata_file_path or "",
            ])
    logging.info("Wrote %d job records to %s", len(records), path)


def print_summary(stats: Stats, top_n: int, enriched: bool) -> None:
    """Render the count breakdown to stdout. Logging goes to stderr so summary survives `| tee`."""
    bundle_pct = (stats.bundle / stats.total * 100) if stats.total else 0.0
    direct_pct = (stats.direct / stats.total * 100) if stats.total else 0.0
    print(f"\nWorkspace job inventory")
    print(f"  total:           {stats.total}")
    print(f"  DAB-deployed:    {stats.bundle} ({bundle_pct:.1f}%)")
    print(f"  directly-deployed: {stats.direct} ({direct_pct:.1f}%)")

    if top_n > 0 and stats.creators:
        print(f"\nTop {top_n} creators (by job count):")
        for creator, count in stats.creators.most_common(top_n):
            print(f"  {count:>6}  {creator}")

    if top_n > 0 and stats.bundles_by_name:
        print(f"\nTop {top_n} bundles (by job count):")
        for bundle_name, count in stats.bundles_by_name.most_common(top_n):
            print(f"  {count:>6}  {bundle_name}")
    elif stats.bundle > 0 and not enriched:
        print("\n(Bundle name breakdown requires --enrich-bundles; rerun with that flag for the per-bundle split.)")


def build_client(profile: Optional[str]) -> WorkspaceClient:
    return WorkspaceClient(profile=profile) if profile else WorkspaceClient()


def main() -> int:
    args = parse_args()
    setup_logging(args.verbose)

    w = build_client(args.profile)
    filters = parse_filters(args)
    stats = Stats()
    records: List[JobRecord] = []
    bundle_meta_cache: Dict[str, Optional[BundleMetadata]] = {}

    logging.info(
        "Scanning jobs host=%s tag=%s owners=%s enrich_bundles=%s",
        w.config.host,
        f"{filters.tag_key}={filters.tag_value}" if filters.tag_key else None,
        filters.owners or None,
        args.enrich_bundles,
    )

    scanned = 0
    for job in w.jobs.list(expand_tasks=False):
        scanned += 1
        if args.progress_every and scanned % args.progress_every == 0:
            logging.info("...scanned=%d matched=%d bundle=%d direct=%d",
                         scanned, stats.total, stats.bundle, stats.direct)

        if not job_matches(job, filters):
            continue

        bundle, meta_path = is_bundle_job(job)
        name = (job.settings.name if job.settings else None) or "<unnamed>"
        stats.total += 1
        stats.creators[job.creator_user_name or "<unknown>"] += 1

        if bundle:
            stats.bundle += 1
            md = fetch_bundle_metadata(w, meta_path, bundle_meta_cache) if args.enrich_bundles else None
            if md and md.bundle_name:
                stats.bundles_by_name[md.bundle_name] += 1
            records.append(JobRecord(
                job_id=job.job_id, name=name,
                creator=job.creator_user_name,
                deployment_kind="BUNDLE",
                metadata_file_path=meta_path,
                metadata=md,
            ))
        else:
            stats.direct += 1
            records.append(JobRecord(
                job_id=job.job_id, name=name,
                creator=job.creator_user_name,
                deployment_kind="DIRECT",
            ))

    write_inventory(args.output, records)
    print_summary(stats, args.top_n, args.enrich_bundles)
    logging.info("Done. scanned=%d matched=%d bundle=%d direct=%d",
                 scanned, stats.total, stats.bundle, stats.direct)
    return 0


if __name__ == "__main__":
    sys.exit(main())
