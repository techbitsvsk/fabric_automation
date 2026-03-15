from typing import Dict, Any
from app.clients.fabric_client import FabricClient
from app.utils.aad_lookup import resolve_group_identifier
from app.utils.fabric_lookup import resolve_capacity_identifier, resolve_domain_identifier
from app.models.schemas import WorkspaceSpec, ProvisionResult
from app.logging_config import logger, stage
from app.exceptions import ProvisioningError, ValidationError

ROLE_MAP = {
    "platform_admins": "Admin",
    "data_product_owners": "Member",
    "data_engineers": "Contributor",
    "data_viewers": "Viewer"
}

class Provisioner:
    """Orchestrates end-to-end provisioning of a Microsoft Fabric workspace.

    Coordinates workspace create/update, capacity and domain assignment,
    role reconciliation via AAD group lookup, diagnostic settings, and
    metric alert creation.
    """

    def __init__(self, fabric_client: FabricClient | None = None):
        """Initialise with an optional pre-built client (useful for testing).

        Args:
            fabric_client: FabricClient instance; defaults to a new one if omitted.
        """
        self.fabric = fabric_client or FabricClient()

    def provision_workspace(self, spec: WorkspaceSpec) -> ProvisionResult:
        """Provision or reconcile a Fabric workspace according to the given spec.

        Execution order:
            1. Find existing workspace by name, or create it.
            2. Update description if it has changed.
            3. Assign capacity (if specified).
            4. Assign domain (if specified).
            5. Reconcile role assignments for all four group types.

        Args:
            spec: WorkspaceSpec describing desired state.

        Returns:
            ProvisionResult summarising what was applied.

        Raises:
            ProvisioningError: If the workspace id cannot be determined after
                create/find.
        """
        logger.info("========== PROVISIONING START : %s ==========", spec.name)

        # Resolve capacity and domain display names → GUIDs once up front
        stage(0, "Resolve identifiers")
        capacity_id = (
            resolve_capacity_identifier(spec.capacity, self.fabric)
            if spec.capacity else None
        )
        domain_id = (
            resolve_domain_identifier(spec.domain, self.fabric)
            if spec.domain else None
        )

        stage(1, "Workspace")
        ws = self.fabric.find_workspace_by_name(spec.name)
        is_new = ws is None
        if is_new:
            # Pass capacity_id at creation so the workspace is provisioned as
            # Fabric type (F-SKU) rather than defaulting to Power BI Pro.
            ws = self.fabric.create_workspace(
                spec.name, spec.description or "", capacity_id=capacity_id
            )
            logger.info("workspace_created", workspace=spec.name,
                        capacity=capacity_id or "none — Power BI Pro default")
        else:
            # Update description only if it has changed
            if spec.description and spec.description != ws.get("description"):
                ws = self.fabric.update_workspace(
                    ws.get("id") or ws.get("workspaceId") or ws.get("objectId"),
                    {"description": spec.description},
                )
            logger.info("workspace_exists", workspace=spec.name)

        workspace_id = ws.get("id") or ws.get("workspaceId") or ws.get("objectId")
        if not workspace_id:
            raise ProvisioningError("Unable to determine workspace id after create/find")

        stage(2, "Capacity")
        # Assign capacity — idempotent
        #    New workspaces: already bound at creation, nothing to do.
        #    Existing workspaces: only call assignToCapacity if the capacity changed.
        capacity_assigned = None
        if capacity_id:
            current_capacity = None if is_new else ws.get("capacityId")
            if is_new:
                logger.info("capacity_set_at_creation", workspace=spec.name, capacity=capacity_id)
            elif current_capacity == capacity_id:
                logger.info("capacity_unchanged", workspace=spec.name, capacity=capacity_id)
            else:
                logger.info("capacity_assigning", workspace=spec.name,
                            from_capacity=current_capacity, to_capacity=capacity_id)
                self.fabric.assign_capacity(workspace_id, capacity_id)
                logger.info("capacity_assigned", workspace=spec.name, capacity=capacity_id)
            capacity_assigned = capacity_id

        stage(3, "Domain")
        # Assign domain — idempotent: skip if already assigned to the same domain
        domain_assigned = None
        if domain_id:
            current_domain = ws.get("domainId") if not is_new else None
            if current_domain == domain_id:
                logger.info("domain_unchanged", workspace=spec.name, domain=domain_id)
            else:
                self.fabric.assign_domain(workspace_id, domain_id)
                logger.info("domain_assigned", workspace=spec.name, domain=domain_id)
            domain_assigned = domain_id

        stage(4, "Role assignments")
        existing_roles = self.fabric.list_role_assignments(workspace_id)
        # Build map of principalId -> {role, id} from current assignments
        existing_assignments: Dict[str, Any] = {}
        for r in existing_roles.get("value", []) if isinstance(existing_roles, dict) else existing_roles:
            principal = r.get("principal", {})
            pid = principal.get("id") or r.get("principalId")
            if pid:
                existing_assignments[pid] = {
                    "role": r.get("role") or r.get("workspaceRole"),
                    "id": r.get("id"),
                    "type": principal.get("type", "Group"),
                }

        roles_assigned: Dict[str, str] = {}
        roles_failed: Dict[str, str] = {}
        for key, desired_role in ROLE_MAP.items():
            group_identifier = spec.groups.get(key)
            if not group_identifier:
                reason = f"Missing group mapping for key '{key}'"
                logger.warning("role_skipped", workspace=spec.name, role=desired_role, reason=reason)
                roles_failed[key] = reason
                continue
            try:
                principal_id = resolve_group_identifier(group_identifier)
                existing = existing_assignments.get(principal_id)
                if existing is None:
                    self.fabric.add_role_assignment(workspace_id, principal_id, desired_role)
                    logger.info("role_assigned", workspace=spec.name, group=group_identifier,
                                principal=principal_id, role=desired_role)
                elif existing["role"] != desired_role:
                    self.fabric.update_role_assignment(
                        workspace_id, existing["id"], desired_role, existing["type"]
                    )
                    logger.info("role_updated", workspace=spec.name, group=group_identifier,
                                principal=principal_id, old_role=existing["role"], new_role=desired_role)
                else:
                    logger.info("role_unchanged", workspace=spec.name, group=group_identifier,
                                principal=principal_id, role=desired_role)
                roles_assigned[key] = principal_id
            except Exception as ex:
                reason = str(ex)
                logger.warning("role_failed", workspace=spec.name, key=key, group=group_identifier,
                               role=desired_role, error=reason)
                roles_failed[key] = reason

        if roles_failed:
            logger.warning("roles_partial", workspace=spec.name,
                           assigned=list(roles_assigned.keys()),
                           failed=roles_failed)
        result = ProvisionResult(
            workspace_id=workspace_id,
            workspace_name=spec.name,
            capacity_assigned=capacity_assigned,
            domain_assigned=domain_assigned,
            roles_assigned=roles_assigned,
            roles_failed=roles_failed,
            log_analytics_resource_id=spec.log_analytics_resource_id,
        )
        logger.info("========== PROVISIONING COMPLETE : %s ==========", spec.name)
        logger.info("provision_complete", workspace=spec.name, result=result.model_dump())
        return result
