#!/usr/bin/env python3
"""Patch Databricks Asset Bundle YAMLs to add `webhook_notifications` to job resources.

Run on a local checkout of a bundle repo. Reads `databricks.yml` plus every file matched
by its `include:` globs, finds every base `resources.jobs.<name>` block, and merges the
supplied webhook_id into each configured event list.

Per-target overrides (`targets.<env>.resources.jobs.<name>`) are detected but NEVER
written to. DAB deep-merge concatenates `webhook_notifications` event lists at deploy
time, so the base patch propagates into every target automatically. Writing the
override too would produce a duplicate that Databricks rejects at deploy with
"Duplicate webhook ids". If an override already contains the same webhook_id the
patcher would add, a WARNING is logged so the user can hand-edit before deploying.

Variable references (`${var.webhook_id}`) are detected per event list: if any
existing entry looks like a bundle variable, that event list is skipped with a
WARNING — the patcher can't resolve variables and could otherwise create a
deploy-time duplicate when the variable resolves to the same destination.

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


def _override_dup_events(jnode: CommentedMap, webhook_id: str, events: List[str]) -> List[str]:
    """Return event names where a per-target override already contains webhook_id.
    These are the events where DAB merge concatenation would produce a duplicate
    against the base patch — Databricks rejects this at deploy with
    `Duplicate webhook ids`. The user must hand-edit the override before deploy."""
    dup_events: List[str] = []
    wn = jnode.get("webhook_notifications")
    if not isinstance(wn, dict):
        return dup_events
    for ev in events:
        lst = wn.get(ev) or []
        for x in lst:
            if isinstance(x, dict) and x.get("id") == webhook_id:
                dup_events.append(ev)
                break
    return dup_events


def _has_variable_ref(lst) -> bool:
    """True if any entry's id looks like a bundle variable reference (`${...}`).
    The patcher writes literal IDs and can't resolve variables; if a variable
    already resolves to the same destination at deploy time, appending the
    literal would produce a duplicate that Databricks rejects with
    `Duplicate webhook ids` at `bundle deploy`."""
    for x in lst:
        if not isinstance(x, dict):
            continue
        wid = x.get("id")
        if isinstance(wid, str) and wid.startswith("${"):
            return True
    return False


def patch_webhooks(
    job_node: CommentedMap, webhook_id: str, events: List[str]
) -> Tuple[bool, List[str]]:
    """Merge webhook_id into each event list under webhook_notifications.

    Skips any event list that already contains a `${var.*}` reference — the
    patcher can't resolve variables and appending the literal would produce
    a deploy-time duplicate when the variable resolves to the same ID.

    Returns (changed, events_skipped_due_to_variable_ref)."""
    changed = False
    var_skipped: List[str] = []
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
        if _has_variable_ref(lst):
            var_skipped.append(ev)
            continue
        if not any(isinstance(x, dict) and x.get("id") == webhook_id for x in lst):
            entry = CommentedMap()
            entry["id"] = webhook_id
            lst.append(entry)
            changed = True
    return changed, var_skipped


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
    total_overrides_skipped = 0
    total_var_skipped_events = 0

    for path in files:
        with path.open() as f:
            original = f.read()
        doc = yaml.load(io.StringIO(original))
        if doc is None:
            continue

        file_changed = False
        for loc, jnode in find_job_nodes(doc):
            total_jobs_seen += 1
            if loc.startswith("targets."):
                total_overrides_skipped += 1
                # Per-target overrides are never patched: DAB deep-merge
                # concatenates webhook_notifications event lists at deploy time,
                # so a base patch + override patch produces duplicates that
                # Databricks rejects with `Duplicate webhook ids`. Patching the
                # base alone is sufficient — DAB merge propagates it into every
                # target. If the override already contains the same webhook_id
                # the patcher would add to the base, we'd still hit a duplicate
                # at deploy via concatenation, so warn loudly.
                dup_events = _override_dup_events(jnode, args.webhook_id, events)
                if dup_events:
                    logging.warning(
                        "  %s :: %s -> override already contains webhook %s on events %s. "
                        "DAB merge will concatenate base + override at deploy, producing "
                        "duplicates that Databricks rejects. Hand-edit the override to "
                        "remove the redundant entries before `bundle deploy`.",
                        path.relative_to(bundle_dir), loc, args.webhook_id, dup_events,
                    )
                else:
                    logging.info(
                        "  %s :: %s -> skipped (target override; DAB merge propagates base patch)",
                        path.relative_to(bundle_dir), loc,
                    )
                continue
            if not job_matches(jnode, args.job, tag):
                continue
            total_jobs_matched += 1
            patched, var_skipped = patch_webhooks(jnode, args.webhook_id, events)
            if patched:
                total_jobs_patched += 1
                file_changed = True
                logging.info("  %s :: %s -> patched", path.relative_to(bundle_dir), loc)
            else:
                logging.debug("  %s :: %s -> already has webhook", path.relative_to(bundle_dir), loc)
            if var_skipped:
                total_var_skipped_events += len(var_skipped)
                logging.warning(
                    "  %s :: %s -> events %s skipped: existing ${var.*} reference. "
                    "Patcher writes literal IDs; if the variable resolves to the same destination, "
                    "Databricks rejects the deploy with 'Duplicate webhook ids'. Hand-edit if needed.",
                    path.relative_to(bundle_dir), loc, var_skipped,
                )

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
        "Done. mode=%s files_changed=%d jobs_seen=%d jobs_matched=%d jobs_patched=%d overrides_skipped=%d var_skipped_events=%d",
        "APPLY" if args.apply else "DRY-RUN",
        total_files_changed, total_jobs_seen, total_jobs_matched, total_jobs_patched,
        total_overrides_skipped, total_var_skipped_events,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
