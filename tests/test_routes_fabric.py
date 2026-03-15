import pytest
from fastapi.testclient import TestClient
from app.main import app
from app.api.v1.routes_fabric import get_provisioner
from app.models.schemas import ProvisionResult


def test_route_create_workspace():
    class FakeProv:
        def provision_workspace(self, spec):
            # Must return a ProvisionResult — the route calls result.model_dump()
            return ProvisionResult(
                workspace_id="ws-1",
                workspace_name=spec.name,
                capacity_assigned=None,
                domain_assigned=None,
                roles_assigned={},
                roles_failed={},
            )

    # FastAPI Depends() holds a reference to the original function object, so
    # monkeypatch.setattr on the module name does not work.  dependency_overrides
    # is the correct mechanism for swapping dependencies in tests.
    app.dependency_overrides[get_provisioner] = lambda: FakeProv()
    try:
        client = TestClient(app)
        payload = {
            "name": "test-ws",
            "description": "desc",
            "group_platform_admins": "admins",
            "group_data_product_owners": "owners",
            "group_data_engineers": "eng",
            "group_data_viewers": "view",
        }
        resp = client.post("/v1/fabric/workspace", json=payload)
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"
        assert body["workspace"]["workspace_name"] == "test-ws"
    finally:
        app.dependency_overrides.clear()
