#!/usr/bin/env python3
"""Create a Databricks generic-webhook notification destination from a URL.

Companion to bulk_apply_webhooks.py / patch_bundle_yaml.py: instead of clicking
through Admin Settings > Notifications to create a destination, run this once and
pipe the printed ID into the bulk script.

Idempotent on `display_name`: if a destination with the same name already exists,
the script reports the existing ID and exits 0 without mutating anything.

Default is dry-run. Pass `--apply` to actually create the destination.

Examples:
  python3 create_webhook_destination.py --url https://hooks.example.com/abc --name my-team-webhook
  python3 create_webhook_destination.py --url https://hooks.example.com/abc --name my-team-webhook --apply
"""

import argparse
import logging
import sys
from typing import Optional
from urllib.parse import urlparse

from databricks.sdk import WorkspaceClient

# We use w.api_client.do() (raw REST) instead of w.notification_destinations.*
# typed methods. The Databricks runtime ships an older SDK (e.g. 0.20.0) where
# the typed `notification_destinations` API and its `Config`/`GenericWebhookConfig`
# wrappers do not exist. `api_client.do()` is core SDK plumbing that's been
# stable across versions, so this works on both the runtime SDK and recent
# pip-installed releases.
_API_PATH = "/api/2.0/notification-destinations"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--url", required=True, help="Webhook URL the destination should POST to.")
    p.add_argument("--name", required=True, help="Display name for the destination (must be unique in the workspace).")
    p.add_argument("--apply", action="store_true", help="Actually create. Default: dry-run.")
    p.add_argument("--profile", help="Databricks CLI profile to use.")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args()


def validate_url(url: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise SystemExit(f"--url must be an http(s) URL; got: {url!r}")
    if parsed.scheme == "http":
        logging.warning("URL uses http://, not https:// — webhook payloads will be sent unencrypted.")


def _iter_destinations(w: WorkspaceClient):
    """Page through GET /api/2.0/notification-destinations, yielding raw dicts.

    The list endpoint omits the `config` field (admin-only, only populated on
    create/get-by-id), so URL info isn't available here — only id/display_name/
    destination_type."""
    query = {}
    while True:
        resp = w.api_client.do("GET", _API_PATH, query=query) or {}
        for d in resp.get("results", []) or []:
            yield d
        token = resp.get("next_page_token")
        if not token:
            return
        query["page_token"] = token


def find_existing(w: WorkspaceClient, name: str):
    for d in _iter_destinations(w):
        if d.get("display_name") == name:
            return d
    return None


def print_summary(label: str, dest: dict) -> None:
    cfg = (dest.get("config") or {}).get("generic_webhook") or {}
    url = cfg.get("url") or "(redacted by API)"
    print(f"{label}")
    print(f"  id:           {dest.get('id')}")
    print(f"  display_name: {dest.get('display_name')}")
    print(f"  type:         {dest.get('destination_type') or '?'}")
    print(f"  url:          {url}")


def run(
    url: str,
    name: str,
    apply: bool = False,
    profile: Optional[str] = None,
    verbose: bool = False,
    client=None,
) -> int:
    """Library entry point. Notebooks import this and map widgets → kwargs.

    Kwargs mirror `parse_args()` 1:1 for the CLI shape; `client` is a notebook-
    only kwarg that lets the multi-workspace dispatcher inject a pre-built
    `WorkspaceClient` per target workspace (replacing the `profile` lookup)."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )
    validate_url(url)

    if client is not None:
        w = client
    else:
        w = WorkspaceClient(profile=profile) if profile else WorkspaceClient()
    logging.info("Mode=%s host=%s name=%s", "APPLY" if apply else "DRY-RUN", w.config.host, name)

    existing = find_existing(w, name)
    if existing is not None:
        print_summary(f"Destination already exists with display_name={name!r}:", existing)
        return 0

    if not apply:
        logging.info("DRY-RUN: would create a generic-webhook destination named %r pointing at %s", name, url)
        logging.info("Re-run with apply=True to actually create it.")
        return 0

    dest = w.api_client.do(
        "POST",
        _API_PATH,
        body={
            "display_name": name,
            "config": {"generic_webhook": {"url": url}},
        },
    )
    print_summary("Created:", dest or {})
    return 0


def main() -> int:
    return run(**vars(parse_args()))


if __name__ == "__main__":
    sys.exit(main())
