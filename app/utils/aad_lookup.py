import re
from typing import Optional
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from app.config import settings
from app.logging_config import logger
from app.utils.retry import get_retry_session

_session = get_retry_session()

GRAPH_SCOPE = "https://graph.microsoft.com/.default"
GRAPH_API = "https://graph.microsoft.com/v1.0"

def _get_graph_token():
    """Obtain a bearer token for Microsoft Graph API.

    Uses DefaultAzureCredential when USE_AZURE_IDENTITY is enabled,
    otherwise falls back to ClientSecretCredential with explicit
    tenant/client/secret settings.

    Returns:
        Access token string scoped to Microsoft Graph.
    """
    if settings.USE_AZURE_IDENTITY:
        cred = DefaultAzureCredential()
        return cred.get_token(GRAPH_SCOPE).token
    cred = ClientSecretCredential(settings.AZURE_TENANT_ID, settings.AZURE_CLIENT_ID, settings.AZURE_CLIENT_SECRET)
    return cred.get_token(GRAPH_SCOPE).token

def resolve_group_identifier(group_identifier: str) -> Optional[str]:
    """
    Accepts either an object id or display name.
    If identifier looks like a GUID, return as-is.
    Otherwise query Microsoft Graph to find group by displayName.
    """
    if not group_identifier:
        return None
    # Quick GUID check
    if re.match(r"^[0-9a-fA-F-]{36}$", group_identifier):
        return group_identifier

    token = _get_graph_token()
    headers = {"Authorization": f"Bearer {token}"}
    # Search groups by displayName (exact match)
    params = {"$filter": f"displayName eq '{group_identifier}'", "$select": "id,displayName"}
    resp = _session.get(f"{GRAPH_API}/groups", headers=headers, params=params)
    if resp.status_code != 200:
        logger.error("aad_lookup_failed", status=resp.status_code, body=resp.text)
        raise RuntimeError("Failed to resolve AD group via Microsoft Graph")
    data = resp.json().get("value", [])
    if not data:
        raise ValueError(f"AD group not found: {group_identifier}")
    return data[0]["id"]
