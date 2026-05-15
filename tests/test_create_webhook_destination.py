"""Tests for create_webhook_destination.py.

Coverage is intentionally minimal — the script is one short happy-path call to
the SDK. The notebook-only `client` kwarg matters most: it's how the multi-
workspace dispatcher loops the create across target workspaces.

These tests mock `w.api_client.do(...)` because the script uses raw REST via
the SDK's underlying ApiClient (the typed `w.notification_destinations.*` API
isn't present in the older SDK shipped on the Databricks runtime — see the
comment in `create_webhook_destination.py`)."""

from unittest.mock import MagicMock

import pytest

import create_webhook_destination as cwd


WEBHOOK_URL = "https://hooks.example.com/abc"
WEBHOOK_NAME = "test-destination"


def _existing_dict(id_="dest-1", name=WEBHOOK_NAME):
    """Shape returned by GET /api/2.0/notification-destinations 'results'.
    Note: list responses omit `config` (admin-only on create/get-by-id)."""
    return {
        "id": id_,
        "display_name": name,
        "destination_type": "GENERIC_WEBHOOK",
    }


def _make_client(list_responses=None, create_response=None):
    """Build a mock WorkspaceClient where `api_client.do(method, path, ...)`
    returns the right thing for GET vs POST. `list_responses` is a list of dicts
    (one per page) that the script will iterate through; `create_response` is
    the dict returned for the POST."""
    list_responses = list(list_responses or [{"results": []}])
    posts = []

    def _do(method, path, *, query=None, body=None, headers=None):
        if method == "GET":
            return list_responses.pop(0)
        if method == "POST":
            posts.append(body)
            return create_response or {}
        raise AssertionError(f"unexpected do() call: {method} {path}")

    w = MagicMock()
    w.api_client.do.side_effect = _do
    w.config.host = "https://test"
    w._posts = posts  # exposed for assertions
    return w


class TestRunCallable:
    def test_dry_run_no_existing_destination_does_not_mutate(self):
        w = _make_client()
        rc = cwd.run(url=WEBHOOK_URL, name=WEBHOOK_NAME, apply=False, client=w)
        assert rc == 0
        assert w._posts == []

    def test_idempotent_when_destination_exists(self):
        w = _make_client(list_responses=[{"results": [_existing_dict()]}])
        rc = cwd.run(url=WEBHOOK_URL, name=WEBHOOK_NAME, apply=True, client=w)
        assert rc == 0
        # Existing destination short-circuits — POST must not be called.
        assert w._posts == []

    def test_apply_creates_when_missing(self):
        created = _existing_dict(id_="new-1")
        w = _make_client(create_response=created)
        rc = cwd.run(url=WEBHOOK_URL, name=WEBHOOK_NAME, apply=True, client=w)
        assert rc == 0
        assert len(w._posts) == 1
        assert w._posts[0] == {
            "display_name": WEBHOOK_NAME,
            "config": {"generic_webhook": {"url": WEBHOOK_URL}},
        }

    def test_pagination_walks_all_pages(self):
        # Destination on the second page — find_existing must follow next_page_token.
        w = _make_client(list_responses=[
            {"results": [_existing_dict(id_="other", name="other-dest")], "next_page_token": "p2"},
            {"results": [_existing_dict()]},
        ])
        rc = cwd.run(url=WEBHOOK_URL, name=WEBHOOK_NAME, apply=True, client=w)
        assert rc == 0
        assert w._posts == []  # existing on page 2 → no POST

    def test_client_kwarg_bypasses_workspaceclient_construction(self, monkeypatch):
        """When `client` is passed, WorkspaceClient must not be re-constructed.
        This is how the multi-workspace dispatcher injects SP-authed clients."""
        w = _make_client()

        def _should_not_construct(*args, **kwargs):
            raise AssertionError("WorkspaceClient must not be constructed when client= is provided")
        monkeypatch.setattr(cwd, "WorkspaceClient", _should_not_construct)

        rc = cwd.run(url=WEBHOOK_URL, name=WEBHOOK_NAME, apply=False, client=w)
        assert rc == 0

    def test_rejects_non_http_url(self):
        with pytest.raises(SystemExit, match="must be an http"):
            cwd.run(url="ftp://example.com/x", name=WEBHOOK_NAME)
