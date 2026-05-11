#!/usr/bin/env python3
"""Patch Databricks Asset Bundle YAMLs to add `webhook_notifications` to job resources.

Run on a local checkout of a bundle repo. Reads `databricks.yml` plus every file matched
by its `include:` globs, walks every `resources.jobs.<name>` block (including those nested
under `targets.<env>.resources.jobs.<name>`), and merges the supplied webhook_id into
each configured event list.

Idempotent: re-running with the same webhook id is a no-op.
Round-trip safe: comments, key order, anchors, and quoting style are preserved via ruamel.yaml.

Default is dry-run (prints unified diff). Pass `--apply` to write files in place.

Examples:
  python3 patch_bundle_yaml.py --bundle-dir ./my-bundle --webhook-id <ID>
  python3 patch_bundle_yaml.py --bundle-dir ./my-bundle --webhook-id <ID> --apply
  python3 patch_bundle_yaml.py --bundle-dir . --webhook-id <ID> \
      --job nightly-etl --events on_failure --apply
"""

import argparse
import difflib
import io
import logging
import sys
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq


EVENT_FIELDS = ("on_start", "on_success", "on_failure", "on_duration_warning_threshold_exceeded")


def make_yaml() -> YAML:
    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.width = 4096
    yaml.indent(mapping=2, sequence=4, offset=2)
    return yaml


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bundle-dir", default=".", help="Path to bundle root (contains databricks.yml). Default: cwd.")
    p.add_argument("--webhook-id", required=True, help="Webhook destination ID to attach.")
    p.add_argument(
        "--events",
        default="on_failure,on_success,on_start",
        help=f"Comma-separated event list. Valid: {', '.join(EVENT_FIELDS)}.",
    )
    p.add_argument(
        "--job",
        action="append",
        default=[],
        help="Limit to jobs whose `name:` field matches. Repeatable.",
    )
    p.add_argument("--tag", help="Filter by job-resource tag. Format: key=value, or just key.")
    p.add_argument("--apply", action="store_true", help="Write files in place. Default: dry-run diff to stdout.")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args()


def parse_events(s: str) -> List[str]:
    events = [e.strip() for e in s.split(",") if e.strip()]
    unknown = [e for e in events if e not in EVENT_FIELDS]
    if unknown:
        raise SystemExit(f"Unknown event(s): {unknown}. Valid: {EVENT_FIELDS}")
    return events


def parse_tag_filter(s: Optional[str]) -> Optional[Tuple[str, Optional[str]]]:
    if not s:
        return None
    if "=" in s:
        k, v = s.split("=", 1)
        return k, v
    return s, None


def discover_yaml_files(bundle_dir: Path, root_doc) -> List[Path]:
    files: List[Path] = [bundle_dir / "databricks.yml"]
    seen = {files[0].resolve()}
    includes = []
    if isinstance(root_doc, CommentedMap):
        includes = root_doc.get("include") or []
    for pat in includes:
        for p in sorted(bundle_dir.glob(pat)):
            resolved = p.resolve()
            if p.is_file() and resolved not in seen:
                files.append(p)
                seen.add(resolved)
    return files


def find_job_nodes(doc) -> Iterable[Tuple[str, CommentedMap]]:
    """Yield (location_label, job_node) for every resources.jobs.<name> in the doc,
    including those nested under targets.<env>."""
    if not isinstance(doc, CommentedMap):
        return

    resources = doc.get("resources")
    if isinstance(resources, CommentedMap):
        jobs = resources.get("jobs")
        if isinstance(jobs, CommentedMap):
            for name, node in jobs.items():
                if isinstance(node, CommentedMap):
                    yield f"resources.jobs.{name}", node

    targets = doc.get("targets")
    if isinstance(targets, CommentedMap):
        for tname, tnode in targets.items():
            if not isinstance(tnode, CommentedMap):
                continue
            tres = tnode.get("resources")
            if not isinstance(tres, CommentedMap):
                continue
            tjobs = tres.get("jobs")
            if not isinstance(tjobs, CommentedMap):
                continue
            for name, node in tjobs.items():
                if isinstance(node, CommentedMap):
                    yield f"targets.{tname}.resources.jobs.{name}", node


def job_matches(job_node: CommentedMap, names: List[str], tag: Optional[Tuple[str, Optional[str]]]) -> bool:
    if names:
        if job_node.get("name") not in names:
            return False
    if tag:
        key, val = tag
        tags = job_node.get("tags")
        if not isinstance(tags, dict) or key not in tags:
            return False
        if val is not None and tags[key] != val:
            return False
    return True


def patch_webhooks(job_node: CommentedMap, webhook_id: str, events: List[str]) -> bool:
    """Merge webhook_id into each event list under webhook_notifications. Returns True if mutated."""
    changed = False
    wn = job_node.get("webhook_notifications")
    if wn is None:
        wn = CommentedMap()
        # Insert just before `tasks` for review-friendly diffs; fall back to end.
        keys = list(job_node.keys())
        if "tasks" in keys:
            job_node.insert(keys.index("tasks"), "webhook_notifications", wn)
        else:
            job_node["webhook_notifications"] = wn
    for ev in events:
        lst = wn.get(ev)
        if lst is None:
            lst = CommentedSeq()
            wn[ev] = lst
        if not any(isinstance(x, dict) and x.get("id") == webhook_id for x in lst):
            entry = CommentedMap()
            entry["id"] = webhook_id
            lst.append(entry)
            changed = True
    return changed


def dump_to_string(yaml: YAML, doc) -> str:
    buf = io.StringIO()
    yaml.dump(doc, buf)
    return buf.getvalue()


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    bundle_dir = Path(args.bundle_dir).resolve()
    root_yaml = bundle_dir / "databricks.yml"
    if not root_yaml.exists():
        raise SystemExit(f"No databricks.yml found at {root_yaml}. Pass --bundle-dir.")

    events = parse_events(args.events)
    tag = parse_tag_filter(args.tag)
    yaml = make_yaml()

    with root_yaml.open() as f:
        root_doc = yaml.load(f)
    files = discover_yaml_files(bundle_dir, root_doc)

    logging.info(
        "Mode=%s bundle_dir=%s files=%d events=%s job_filter=%s tag=%s",
        "APPLY" if args.apply else "DRY-RUN",
        bundle_dir, len(files), events, args.job or None, args.tag,
    )

    total_files_changed = 0
    total_jobs_seen = 0
    total_jobs_matched = 0
    total_jobs_patched = 0

    for path in files:
        with path.open() as f:
            original = f.read()
        doc = yaml.load(io.StringIO(original))
        if doc is None:
            continue

        file_changed = False
        for loc, jnode in find_job_nodes(doc):
            total_jobs_seen += 1
            if not job_matches(jnode, args.job, tag):
                continue
            total_jobs_matched += 1
            if patch_webhooks(jnode, args.webhook_id, events):
                total_jobs_patched += 1
                file_changed = True
                logging.info("  %s :: %s -> patched", path.relative_to(bundle_dir), loc)
            else:
                logging.debug("  %s :: %s -> already has webhook", path.relative_to(bundle_dir), loc)

        if not file_changed:
            continue
        total_files_changed += 1
        new_content = dump_to_string(yaml, doc)
        if args.apply:
            with path.open("w") as f:
                f.write(new_content)
            logging.info("Wrote %s", path.relative_to(bundle_dir))
        else:
            sys.stdout.writelines(difflib.unified_diff(
                original.splitlines(keepends=True),
                new_content.splitlines(keepends=True),
                fromfile=f"a/{path.relative_to(bundle_dir)}",
                tofile=f"b/{path.relative_to(bundle_dir)}",
            ))

    logging.info(
        "Done. mode=%s files_changed=%d jobs_seen=%d jobs_matched=%d jobs_patched=%d",
        "APPLY" if args.apply else "DRY-RUN",
        total_files_changed, total_jobs_seen, total_jobs_matched, total_jobs_patched,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
