from typing import Optional, Dict, Any
import requests
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from app.config import settings
from app.utils.retry import get_retry_session
from app.logging_config import logger

def _safe_json(resp: requests.Response) -> Dict[str, Any]:
    """Return parsed JSON body, or an empty dict for no-content responses.

    The Fabric API returns 200/202 with an empty body for async operations
    such as assignToCapacity. Calling .json() on an empty body raises a
    JSONDecodeError, so we guard against it here.

    Args:
        resp: Completed HTTP response.

    Returns:
        Parsed JSON dict, or {} if the body is empty or not JSON.
    """
    if not resp.content:
        return {}
    try:
        return resp.json()
    except Exception:
        return {}

def _raise_for_status(resp: requests.Response) -> None:
    """Raise an HTTPError with the full response body included in the message.

    The default requests raise_for_status() only includes the status code.
    This helper appends the raw response body so Fabric's error detail (e.g.
    'Capacity not found', 'Insufficient permissions') appears in the log.

    Args:
        resp: Completed HTTP response.

    Raises:
        requests.exceptions.HTTPError: If the response status is 4xx or 5xx.
    """
    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        body = resp.text[:2000]  # cap to avoid flooding the log
        logger.error(
            "fabric_api_error",
            status=resp.status_code,
            url=str(resp.url),
            body=body,
        )
        raise requests.exceptions.HTTPError(
            f"{exc} — response body: {body}", response=resp
        ) from exc

FABRIC_SCOPE = "https://analysis.windows.net/powerbi/api/.default"

class FabricClient:
    """HTTP client for the Microsoft Fabric REST API.

    Wraps workspace, capacity, domain, and role-assignment operations.
    Authentication is handled per-request via Azure credential providers.
    All requests are executed through a retry-enabled session.
    """

    def __init__(self):
        """Initialise the client with base URL and a retry-enabled HTTP session."""
        self.base = settings.FABRIC_BASE_URL
        self.session = get_retry_session()

    def _get_token(self) -> str:
        """Acquire a bearer token for the Fabric/Power BI API scope.

        Returns:
            Access token string scoped to FABRIC_SCOPE.
        """
        if settings.USE_AZURE_IDENTITY:
            cred = DefaultAzureCredential()
            token = cred.get_token(FABRIC_SCOPE).token
            logger.info("auth_method", method="DefaultAzureCredential")
            return token
        cred = ClientSecretCredential(settings.AZURE_TENANT_ID, settings.AZURE_CLIENT_ID, settings.AZURE_CLIENT_SECRET)
        token = cred.get_token(FABRIC_SCOPE).token
        logger.info("auth_method", method="ClientSecretCredential")
        return token

    def _headers(self) -> Dict[str, str]:
        """Build standard HTTP headers including a fresh bearer token.

        Returns:
            Dict with Authorization and Content-Type headers.
        """
        return {"Authorization": f"Bearer {self._get_token()}", "Content-Type": "application/json"}

    # Capacity operations
    def find_capacity_by_name(self, display_name: str) -> Optional[Dict[str, Any]]:
        """Search for a Fabric capacity by its display name.

        Args:
            display_name: The exact capacity display name to look up.

        Returns:
            The capacity dict (containing ``id`` and ``displayName``) if found,
            or None if no match exists.
        """
        url = f"{self.base}/capacities"
        resp = self.session.get(url, headers=self._headers())
        _raise_for_status(resp)
        data = _safe_json(resp)
        items = data.get("value", []) if isinstance(data, dict) else data
        for item in items:
            if item.get("displayName") == display_name:
                return item
        return None

    def find_domain_by_name(self, display_name: str) -> Optional[Dict[str, Any]]:
        """Search for a Fabric domain by its display name.

        Queries the Fabric admin ``/domains`` endpoint which lists all domains
        visible to the authenticated principal.  Requires the caller to have
        at least Fabric Administrator or Domain Contributor permissions.

        Args:
            display_name: The exact domain display name to look up (case-sensitive).

        Returns:
            The domain dict (containing ``id`` and ``displayName``) if found,
            or None if no match exists.
        """
        url = f"{self.base}/admin/domains"
        resp = self.session.get(url, headers=self._headers())
        _raise_for_status(resp)
        data = _safe_json(resp)
        items = data.get("domains", data.get("value", [])) if isinstance(data, dict) else data
        for item in items:
            if item.get("displayName") == display_name:
                return item
        return None

    # Workspace operations
    def find_workspace_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Search for an existing workspace by its display name.

        Handles both list and OData-envelope response shapes from the API.

        Args:
            name: The exact displayName to search for.

        Returns:
            The workspace dict if found, or None.
        """
        url = f"{self.base}/workspaces"
        resp = self.session.get(url, headers=self._headers(), params={"displayName": name})
        _raise_for_status(resp)
        data = _safe_json(resp)
        # API may return list or OData envelope; normalise to a flat list
        if isinstance(data, dict) and "value" in data:
            items = data["value"]
        elif isinstance(data, list):
            items = data
        else:
            items = []
        for item in items:
            if item.get("displayName") == name:
                return item
        return None

    def create_workspace(
        self,
        display_name: str,
        description: str,
        capacity_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a new Fabric workspace.

        Passing ``capacity_id`` at creation time ensures the workspace is
        provisioned as a Fabric workspace (F-SKU) rather than defaulting to
        Power BI Pro.

        Args:
            display_name: Workspace display name.
            description: Human-readable description.
            capacity_id: Optional Fabric capacity GUID to bind at creation.

        Returns:
            API response dict containing at minimum the new workspace id.
        """
        url = f"{self.base}/workspaces"
        payload: Dict[str, Any] = {"displayName": display_name, "description": description}
        if capacity_id:
            payload["capacityId"] = capacity_id
        resp = self.session.post(url, headers=self._headers(), json=payload)
        _raise_for_status(resp)
        return _safe_json(resp)

    def update_workspace(self, workspace_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Partially update workspace properties via PATCH.

        Args:
            workspace_id: Target workspace identifier.
            payload: Fields to update (e.g. {"description": "new desc"}).

        Returns:
            Updated workspace dict from the API.
        """
        url = f"{self.base}/workspaces/{workspace_id}"
        resp = self.session.patch(url, headers=self._headers(), json=payload)
        _raise_for_status(resp)
        return _safe_json(resp)

    def assign_capacity(self, workspace_id: str, capacity_id: str) -> Dict[str, Any]:
        """Assign a Fabric capacity to the workspace.

        The API may respond with 200 OK or 202 Accepted (async) and an empty
        body; both are treated as success.

        Args:
            workspace_id: Target workspace identifier.
            capacity_id: The Fabric capacity GUID to assign.

        Returns:
            API response dict, or {} for empty-body responses.
        """
        url = f"{self.base}/workspaces/{workspace_id}/assignToCapacity"
        resp = self.session.post(url, headers=self._headers(), json={"capacityId": capacity_id})
        _raise_for_status(resp)
        return _safe_json(resp)

    def assign_domain(self, workspace_id: str, domain_id: str) -> Dict[str, Any]:
        """Assign a Fabric domain to the workspace.

        The API may respond with 200 OK or 202 Accepted (async) and an empty
        body; both are treated as success.

        Args:
            workspace_id: Target workspace identifier.
            domain_id: The domain GUID to assign.

        Returns:
            API response dict, or {} for empty-body responses.
        """
        url = f"{self.base}/workspaces/{workspace_id}/assignToDomain"
        resp = self.session.post(url, headers=self._headers(), json={"domainId": domain_id})
        _raise_for_status(resp)
        return _safe_json(resp)

    # Role assignments
    def list_role_assignments(self, workspace_id: str) -> Dict[str, Any]:
        """Retrieve all role assignments currently set on a workspace.

        Args:
            workspace_id: Target workspace identifier.

        Returns:
            API response dict, typically containing a "value" list of assignments.
        """
        url = f"{self.base}/workspaces/{workspace_id}/roleAssignments"
        resp = self.session.get(url, headers=self._headers())
        _raise_for_status(resp)
        return _safe_json(resp)

    def add_role_assignment(
        self,
        workspace_id: str,
        principal_id: str,
        role: str,
        principal_type: str = "Group",
    ) -> Dict[str, Any]:
        """Grant a principal a specific role on a workspace.

        The Fabric API requires the principal wrapped in an object with an
        explicit ``type`` field — a flat ``principalId`` is rejected with
        "The Principal field is required".

        Args:
            workspace_id: Target workspace identifier.
            principal_id: Azure AD object id of the group, user, or service principal.
            role: Fabric role name ("Admin", "Member", "Contributor", "Viewer").
            principal_type: One of "Group", "User", "ServicePrincipal",
                            "ServicePrincipalProfile". Defaults to "Group".

        Returns:
            API response dict for the created role assignment.
        """
        url = f"{self.base}/workspaces/{workspace_id}/roleAssignments"
        payload = {
            "principal": {"id": principal_id, "type": principal_type},
            "role": role,
        }
        resp = self.session.post(url, headers=self._headers(), json=payload)
        _raise_for_status(resp)
        return _safe_json(resp)

    def update_role_assignment(
        self,
        workspace_id: str,
        assignment_id: str,
        role: str,
        principal_type: str = "Group",
    ) -> Dict[str, Any]:
        """Update the role on an existing role assignment.

        Used for idempotent reconciliation when a principal's role has changed.

        Args:
            workspace_id: Target workspace identifier.
            assignment_id: Existing role assignment identifier.
            role: The new Fabric role name to apply.
            principal_type: Principal type required by the PATCH body
                            ("Group", "User", etc.). Defaults to "Group".

        Returns:
            API response dict for the updated role assignment.
        """
        url = f"{self.base}/workspaces/{workspace_id}/roleAssignments/{assignment_id}"
        payload = {"role": role, "principal": {"type": principal_type}}
        resp = self.session.patch(url, headers=self._headers(), json=payload)
        _raise_for_status(resp)
        return _safe_json(resp)
