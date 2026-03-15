from pydantic import BaseModel
from typing import Dict, Optional

class WorkspaceSpec(BaseModel):
    """Input specification for provisioning a Fabric workspace.

    Captures all configuration required to create or update a workspace,
    assign capacity and domain, and configure role-based access.
    """

    name: str
    description: Optional[str]
    capacity: Optional[str]
    domain: Optional[str]
    groups: Dict[str, str]
    log_analytics_resource_id: Optional[str] = None

class ProvisionResult(BaseModel):
    """Result returned after a workspace provisioning run.

    Summarises what was created or updated and which role assignments
    succeeded or failed.
    """

    workspace_id: str
    workspace_name: str
    capacity_assigned: Optional[str]
    domain_assigned: Optional[str]
    roles_assigned: Dict[str, str]
    roles_failed: Dict[str, str] = {}
    log_analytics_resource_id: Optional[str] = None
