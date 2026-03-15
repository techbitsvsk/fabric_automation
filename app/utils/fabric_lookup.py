import re
from typing import Optional
from app.logging_config import logger

_GUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


def resolve_capacity_identifier(identifier: str, fabric_client) -> Optional[str]:
    """Resolve a capacity display name or GUID to a capacity GUID.

    If ``identifier`` is already a valid GUID it is returned as-is without an
    API call.  Otherwise the Fabric ``/capacities`` endpoint is queried and the
    first capacity whose ``displayName`` matches exactly is returned.

    Args:
        identifier: Either a capacity GUID or a display name such as
                    ``"miniazurefabricpoc"``.
        fabric_client: An initialised :class:`FabricClient` instance used to
                       query the Fabric API when a name lookup is required.

    Returns:
        The capacity GUID string.

    Raises:
        ValueError: If the display name is not found in the tenant's capacity
                    list.
    """
    if not identifier:
        return None

    if _GUID_RE.match(identifier):
        logger.info("capacity_resolved_as_guid", capacity=identifier)
        return identifier

    logger.info("capacity_resolving_by_name", display_name=identifier)
    capacity = fabric_client.find_capacity_by_name(identifier)
    if not capacity:
        raise ValueError(
            f"Fabric capacity not found by display name: '{identifier}'. "
            "Verify the name in the Fabric Admin portal or pass the GUID directly."
        )
    guid = capacity.get("id")
    logger.info("capacity_resolved", display_name=identifier, guid=guid)
    return guid


def resolve_domain_identifier(identifier: str, fabric_client) -> Optional[str]:
    """Resolve a domain display name or GUID to a domain GUID.

    If ``identifier`` is already a valid GUID it is returned as-is without an
    API call.  Otherwise the Fabric admin ``/domains`` endpoint is queried and
    the first domain whose ``displayName`` matches exactly is returned.

    Args:
        identifier: Either a domain GUID or a display name such as
                    ``"Finance"``.
        fabric_client: An initialised :class:`FabricClient` instance used to
                       query the Fabric API when a name lookup is required.

    Returns:
        The domain GUID string.

    Raises:
        ValueError: If the display name is not found in the tenant's domain
                    list.
    """
    if not identifier:
        return None

    if _GUID_RE.match(identifier):
        logger.info("domain_resolved_as_guid", domain=identifier)
        return identifier

    logger.info("domain_resolving_by_name", display_name=identifier)
    domain = fabric_client.find_domain_by_name(identifier)
    if not domain:
        raise ValueError(
            f"Fabric domain not found by display name: '{identifier}'. "
            "Verify the name in the Fabric Admin portal or pass the GUID directly."
        )
    guid = domain.get("id")
    logger.info("domain_resolved", display_name=identifier, guid=guid)
    return guid
