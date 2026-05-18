"""Shared client-builder for the three SDK-using webhook-rollout notebooks.

Given a list of target workspace URLs and a Databricks secret scope holding the
service principal's OAuth client_id + client_secret, return one authenticated
`WorkspaceClient` per workspace. The notebook dispatcher loops over the list,
calling each script's `run(client=w, ...)`.

Auth model: Databricks OAuth M2M with a single Entra-ID service principal
registered as a Databricks-account-level service principal and granted
workspace-admin on every target workspace. This is the PepsiCo standard pattern
called out in the meeting transcript (Santosh @ 16:23, 1:10:30, 1:22:46).

If `workspace_urls` is empty, falls back to a single notebook-auto-authed
`WorkspaceClient()` — useful for one-off interactive testing in the current
workspace before secrets are wired up.

This file lives under `notebooks/` (not as a sibling of the four scripts)
because the "no shared module between the four scripts" invariant in CLAUDE.md
applies to the scripts themselves. The notebook layer is allowed to share."""

from typing import List, Optional

from databricks.sdk import WorkspaceClient


def parse_workspace_urls(raw: str) -> List[str]:
    """Parse a comma-separated string of workspace URLs into a list.
    Empty / whitespace-only string returns []."""
    if not raw:
        return []
    return [u.strip().rstrip("/") for u in raw.split(",") if u.strip()]


def build_clients(
    workspace_urls: List[str],
    secret_scope: Optional[str],
    client_id_key: str,
    client_secret_key: str,
    dbutils=None,
    auth_mode: str = "databricks-oauth",
    tenant_id_key: Optional[str] = None,
) -> List[WorkspaceClient]:
    """Build one `WorkspaceClient` per workspace URL, authenticated as the
    service principal whose credentials live in the given secret scope.

    Two auth modes:
      - `databricks-oauth` (default): Databricks OAuth M2M. The SP must be
        registered as a Databricks-account service principal and a Databricks-
        issued OAuth secret minted in the account console — the Azure-side
        Entra-ID secret is NOT what goes here. Use this whenever the SP can
        be registered at the Databricks account level.
      - `azure-direct`: Azure AD direct auth. Pass the Azure SP's Application
        ID, tenant ID, and Entra-ID secret. Use this when the SP cannot be
        registered as a Databricks-account SP (e.g. account-admin is not
        available). Requires `tenant_id_key`.

    Secret-scope key conventions:
      databricks-oauth: client_id_key=Databricks OAuth client_id (the Azure
        SP's Application ID), client_secret_key=Databricks-issued OAuth secret.
      azure-direct: client_id_key=Azure Application ID, client_secret_key=
        Azure Entra-ID secret, tenant_id_key=Azure tenant ID.

    If `workspace_urls` is empty, returns a single notebook-auto-authed client.
    If `workspace_urls` is non-empty but `secret_scope` is empty, raises —
    multi-workspace use without SP creds is a configuration error.

    `dbutils` must be passed from the notebook (it isn't importable here)."""
    if not workspace_urls:
        return [WorkspaceClient()]

    if not secret_scope:
        raise ValueError(
            "secret_scope is required when workspace_urls is non-empty. "
            "Multi-workspace runs authenticate as a service principal whose "
            "credentials live in a Databricks secret scope."
        )
    if dbutils is None:
        raise ValueError("dbutils must be passed (it isn't importable from a plain .py module)")

    client_id = dbutils.secrets.get(scope=secret_scope, key=client_id_key)
    client_secret = dbutils.secrets.get(scope=secret_scope, key=client_secret_key)

    clients = []
    if auth_mode == "databricks-oauth":
        for url in workspace_urls:
            clients.append(WorkspaceClient(
                host=url,
                client_id=client_id,
                client_secret=client_secret,
            ))
    elif auth_mode == "azure-direct":
        if not tenant_id_key:
            raise ValueError(
                "auth_mode='azure-direct' requires tenant_id_key (the secret-scope "
                "key holding the Azure tenant ID)."
            )
        tenant_id = dbutils.secrets.get(scope=secret_scope, key=tenant_id_key)
        for url in workspace_urls:
            clients.append(WorkspaceClient(
                host=url,
                azure_tenant_id=tenant_id,
                azure_client_id=client_id,
                azure_client_secret=client_secret,
            ))
    else:
        raise ValueError(
            f"Unknown auth_mode={auth_mode!r}. Expected 'databricks-oauth' or 'azure-direct'."
        )
    return clients
