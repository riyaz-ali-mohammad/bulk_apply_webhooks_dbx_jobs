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
from urllib.parse import urlparse

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.settings import (
    Config,
    DestinationType,
    GenericWebhookConfig,
)


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


def find_existing(w: WorkspaceClient, name: str):
    for d in w.notification_destinations.list():
        if d.display_name == name:
            return d
    return None


def print_summary(label: str, dest) -> None:
    cfg = dest.config.generic_webhook if dest.config else None
    url = cfg.url if cfg and cfg.url else "(redacted by API)"
    print(f"{label}")
    print(f"  id:           {dest.id}")
    print(f"  display_name: {dest.display_name}")
    print(f"  type:         {dest.destination_type.name if dest.destination_type else '?'}")
    print(f"  url:          {url}")


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )
    validate_url(args.url)

    w = WorkspaceClient(profile=args.profile) if args.profile else WorkspaceClient()
    logging.info("Mode=%s host=%s name=%s", "APPLY" if args.apply else "DRY-RUN", w.config.host, args.name)

    existing = find_existing(w, args.name)
    if existing is not None:
        print_summary(f"Destination already exists with display_name={args.name!r}:", existing)
        return 0

    if not args.apply:
        logging.info("DRY-RUN: would create a generic-webhook destination named %r pointing at %s", args.name, args.url)
        logging.info("Re-run with --apply to actually create it.")
        return 0

    dest = w.notification_destinations.create(
        display_name=args.name,
        destination_type=DestinationType.WEBHOOK,
        config=Config(generic_webhook=GenericWebhookConfig(url=args.url)),
    )
    print_summary("Created:", dest)
    return 0


if __name__ == "__main__":
    sys.exit(main())
