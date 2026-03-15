from app.models.schemas import WorkspaceSpec

def test_provisioner_creates_workspace_and_assigns_roles(provisioner, monkeypatch):
    # provisioner.py uses `from app.utils.aad_lookup import resolve_group_identifier`,
    # so the name lives in the provisioner module's namespace — patch it there,
    # not in aad_lookup, otherwise the provisioner still calls the original.
    import app.services.provisioner as prov_module
    monkeypatch.setattr(prov_module, "resolve_group_identifier", lambda x: f"gid-{x}")

    spec = WorkspaceSpec(
        name="test-ws",
        description="desc",
        capacity="cap-1",
        domain="domain-1",
        groups={
            "platform_admins": "admins",
            "data_product_owners": "owners",
            "data_engineers": "eng",
            "data_viewers": "view"
        },
    )

    result = provisioner.provision_workspace(spec)
    assert result.workspace_name == "test-ws"
    assert result.capacity_assigned == "cap-1"
    assert result.roles_assigned["platform_admins"] == "gid-admins"
