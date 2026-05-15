"""Tests for create_webhook_destination.py.

Coverage is intentionally minimal — the script is one short happy-path call to
the SDK. The notebook-only `client` kwarg matters most: it's how the multi-
workspace dispatcher loops the create across target workspaces."""

from unittest.mock import MagicMock

import pytest

import create_webhook_destination as cwd


WEBHOOK_URL = "https://hooks.example.com/abc"
WEBHOOK_NAME = "test-destination"


def _existing_destination(id_="dest-1", url=WEBHOOK_URL, name=WEBHOOK_NAME):
    dest = MagicMock()
    dest.id = id_
    dest.display_name = name
    dest.destination_type = MagicMock(name="GENERIC_WEBHOOK")
    dest.config.generic_webhook.url = url
    return dest


class TestRunCallable:
    def test_dry_run_no_existing_destination_does_not_mutate(self, capsys):
        w = MagicMock()
        w.notification_destinations.list.return_value = iter([])
        w.config.host = "https://test"

        rc = cwd.run(url=WEBHOOK_URL, name=WEBHOOK_NAME, apply=False, client=w)
        assert rc == 0
        w.notification_destinations.create.assert_not_called()

    def test_idempotent_when_destination_exists(self, capsys):
        existing = _existing_destination()
        w = MagicMock()
        w.notification_destinations.list.return_value = iter([existing])
        w.config.host = "https://test"

        rc = cwd.run(url=WEBHOOK_URL, name=WEBHOOK_NAME, apply=True, client=w)
        assert rc == 0
        # Existing destination short-circuits — create() must not be called.
        w.notification_destinations.create.assert_not_called()

    def test_apply_creates_when_missing(self):
        created = _existing_destination(id_="new-1")
        w = MagicMock()
        w.notification_destinations.list.return_value = iter([])
        w.notification_destinations.create.return_value = created
        w.config.host = "https://test"

        rc = cwd.run(url=WEBHOOK_URL, name=WEBHOOK_NAME, apply=True, client=w)
        assert rc == 0
        w.notification_destinations.create.assert_called_once()

    def test_client_kwarg_bypasses_workspaceclient_construction(self, monkeypatch):
        """When `client` is passed, WorkspaceClient must not be re-constructed.
        This is how the multi-workspace dispatcher injects SP-authed clients."""
        w = MagicMock()
        w.notification_destinations.list.return_value = iter([])
        w.config.host = "https://injected"

        def _should_not_construct(*args, **kwargs):
            raise AssertionError("WorkspaceClient must not be constructed when client= is provided")
        monkeypatch.setattr(cwd, "WorkspaceClient", _should_not_construct)

        rc = cwd.run(url=WEBHOOK_URL, name=WEBHOOK_NAME, apply=False, client=w)
        assert rc == 0

    def test_rejects_non_http_url(self):
        with pytest.raises(SystemExit, match="must be an http"):
            cwd.run(url="ftp://example.com/x", name=WEBHOOK_NAME)
