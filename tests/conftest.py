import pytest
from app.clients.fabric_client import FabricClient
from app.services.provisioner import Provisioner

class DummyFabricClient(FabricClient):
    """In-memory FabricClient stub for unit tests.

    Skips network session setup and returns deterministic responses for
    all workspace and role-assignment operations.
    """

    def __init__(self):
        # Skip super().__init__() to avoid creating a real HTTP session.
        pass

    def find_capacity_by_name(self, display_name):
        """Returns a stub capacity whose id equals the display_name."""
        return {"id": display_name, "displayName": display_name}

    def find_domain_by_name(self, display_name):
        """Returns a stub domain whose id equals the display_name."""
        return {"id": display_name, "displayName": display_name}

    def find_workspace_by_name(self, name):
        """Always returns None, simulating a workspace that does not yet exist."""
        return None

    def create_workspace(self, display_name, description, capacity_id=None):
        """Returns a fixed workspace dict with id 'ws-123'."""
        return {"id": "ws-123", "displayName": display_name, "description": description}

    def update_workspace(self, workspace_id, payload):
        """Returns the payload merged with the workspace id."""
        return {"id": workspace_id, **payload}

    def assign_capacity(self, workspace_id, capacity_id):
        """No-op stub; returns a success sentinel."""
        return {"status": "ok"}

    def assign_domain(self, workspace_id, domain_id):
        """No-op stub; returns a success sentinel."""
        return {"status": "ok"}

    def list_role_assignments(self, workspace_id):
        """Returns an empty assignments list, so all roles will be created."""
        return {"value": []}

    def add_role_assignment(self, workspace_id, principal_id, role):
        """Returns a stub role assignment containing the supplied principal and role."""
        return {"id": f"ra-{principal_id}", "principalId": principal_id, "role": role}

    def update_role_assignment(self, _workspace_id, assignment_id, role):
        """Returns a stub updated role assignment with the new role."""
        return {"id": assignment_id, "role": role}


@pytest.fixture
def provisioner():
    """Pytest fixture that provides a Provisioner wired with an in-memory stub.

    Yields:
        Provisioner instance backed by DummyFabricClient.
    """
    return Provisioner(fabric_client=DummyFabricClient())
