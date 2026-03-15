import argparse
import yaml
from app.services.provisioner import Provisioner
from app.models.schemas import WorkspaceSpec
from app.logging_config import logger

def load_config(path: str) -> dict:
    """Load and parse a YAML configuration file from the given path.

    Args:
        path: Filesystem path to the YAML config file.

    Returns:
        Parsed config as a dictionary.
    """
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def main():
    """Entry point for the CLI provisioner.

    Parses the --config argument, loads the YAML workspace spec, builds
    a WorkspaceSpec, and delegates to Provisioner.provision_workspace.
    The resulting ProvisionResult is logged as structured output.
    """
    parser = argparse.ArgumentParser(description="Provision Fabric workspace")
    parser.add_argument("--config", "-c", required=True, help="Path to workspace YAML config")
    args = parser.parse_args()
    cfg = load_config(args.config)
    spec = WorkspaceSpec(
        name=cfg["workspaceName"],
        description=cfg.get("description", ""),
        capacity=cfg.get("capacity"),
        domain=cfg.get("domain"),
        groups=cfg["groups"],
        log_analytics_resource_id=cfg.get("logAnalyticsResourceId"),
    )
    prov = Provisioner()
    result = prov.provision_workspace(spec)
    logger.info("cli_provision_result", result=result.model_dump())

if __name__ == "__main__":
    main()
