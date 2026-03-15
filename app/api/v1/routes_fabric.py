from typing import Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from app.services.provisioner import Provisioner
from app.models.schemas import WorkspaceSpec
from app.logging_config import logger
from app.auth import get_current_user

router = APIRouter(prefix="/v1/fabric", tags=["fabric"])

class WorkspaceRequest(BaseModel):
    name: str = Field(..., min_length=3, max_length=128)
    description: Optional[str] = Field("", max_length=1024)
    capacity_id_or_name: Optional[str] = None
    domain_id_or_name: Optional[str] = None
    group_platform_admins: str
    group_data_product_owners: str
    group_data_engineers: str
    group_data_viewers: str
    log_analytics_resource_id: Optional[str] = None

def get_provisioner() -> Provisioner:
    """FastAPI dependency that supplies a Provisioner instance per request."""
    return Provisioner()

@router.post("/workspace")
def create_or_update_workspace(
    payload: WorkspaceRequest,
    svc: Provisioner = Depends(get_provisioner),
    _user: Dict[str, Any] = Depends(get_current_user),
):
    """Create or update a Fabric workspace from the provided request payload.

    Maps the flat HTTP request body to a WorkspaceSpec, delegates to the
    Provisioner service, and returns a JSON response with the provision result.

    Args:
        payload: Validated request body containing workspace config and group mappings.
        request: Raw FastAPI request (available for correlation id middleware etc.).
        svc: Injected Provisioner dependency.

    Returns:
        JSON body: {"status": "ok", "workspace": <ProvisionResult dict>}

    Raises:
        HTTPException 500: If provisioning raises an unexpected exception.
    """
    spec = WorkspaceSpec(
        name=payload.name,
        description=payload.description,
        capacity=payload.capacity_id_or_name,
        domain=payload.domain_id_or_name,
        groups={
            "platform_admins": payload.group_platform_admins,
            "data_product_owners": payload.group_data_product_owners,
            "data_engineers": payload.group_data_engineers,
            "data_viewers": payload.group_data_viewers
        },
        log_analytics_resource_id=payload.log_analytics_resource_id,
    )
    try:
        result = svc.provision_workspace(spec)
        return {"status": "ok", "workspace": result.model_dump()}
    except Exception as ex:
        logger.exception("provision_failed", error=str(ex), workspace=payload.name)
        raise HTTPException(status_code=500, detail="Workspace provisioning failed")

@router.get("/health")
def health():
    """Liveness probe endpoint.

    Returns:
        JSON body: {"status": "healthy"}
    """
    return {"status": "healthy"}
