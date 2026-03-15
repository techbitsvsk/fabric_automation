"""Entra ID (Azure AD) JWT authentication for FastAPI.

Exposes:
- ``oauth2_scheme``: OAuth2 Authorization Code + PKCE bearer scheme used by
  Swagger UI to render the Authorize button.
- ``get_current_user``: FastAPI dependency that validates the bearer token on
  every protected request and returns the decoded claims dict.
"""

from typing import Dict, Any

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2AuthorizationCodeBearer
from jwt import PyJWKClient, decode as jwt_decode, PyJWTError

from app.config import settings
from app.logging_config import logger

# ---------------------------------------------------------------------------
# Build URLs from the configured tenant
# ---------------------------------------------------------------------------

def _tenant_base() -> str:
    tenant = settings.AZURE_TENANT_ID or "common"
    return f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0"


# OAuth2 Authorization Code + PKCE scheme.
# FastAPI uses this to add the Authorize button to Swagger UI.
# auto_error=False so that DEV_MODE requests without a token are not rejected
# by FastAPI before the dependency function runs.
oauth2_scheme = OAuth2AuthorizationCodeBearer(
    authorizationUrl=f"{_tenant_base()}/authorize",
    tokenUrl=f"{_tenant_base()}/token",
    auto_error=False,
)

# JWKS client caches the public keys and refreshes them automatically when a
# new key id (kid) is encountered.
_jwks_client: PyJWKClient | None = None


def _get_jwks_client() -> PyJWKClient:
    """Return a cached JWKS client for the configured tenant."""
    global _jwks_client
    if _jwks_client is None:
        tenant = settings.AZURE_TENANT_ID or "common"
        jwks_uri = f"https://login.microsoftonline.com/{tenant}/discovery/v2.0/keys"
        _jwks_client = PyJWKClient(jwks_uri, cache_keys=True)
    return _jwks_client


# ---------------------------------------------------------------------------
# FastAPI dependency
# ---------------------------------------------------------------------------

def get_current_user(token: str | None = Depends(oauth2_scheme)) -> Dict[str, Any]:
    """Validate the Entra ID bearer token and return its decoded claims.

    When ``DEV_MODE=true`` in settings, validation is skipped entirely and a
    synthetic dev-user identity is returned — never enable this in production.

    Validates (production mode):
    - Signature against the tenant's JWKS public keys (RS256).
    - Audience matches ``SWAGGER_CLIENT_ID`` (or ``api://<client_id>``).
    - Issuer matches the configured tenant.
    - Token is not expired.

    Args:
        token: Raw JWT extracted from the Authorization header by FastAPI,
               or ``None`` if no Authorization header was sent.

    Returns:
        Decoded JWT claims dict (e.g. ``{"sub": "...", "name": "...", ...}``).

    Raises:
        HTTPException 401: If the token is missing, invalid, or expired
                           (only in production mode).
    """
    # ------------------------------------------------------------------
    # Dev mode — bypass all auth; log a loud warning so it's never missed
    # ------------------------------------------------------------------
    if settings.DEV_MODE:
        logger.warning("auth_dev_mode", message="JWT validation disabled — DEV_MODE=true")
        return {"sub": "dev-user", "name": "Dev User (no auth)"}

    # ------------------------------------------------------------------
    # Production — enforce token presence and validate signature/claims
    # ------------------------------------------------------------------
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    if not token:
        raise credentials_exception

    client_id = settings.SWAGGER_CLIENT_ID
    if not client_id:
        # SWAGGER_CLIENT_ID not configured — skip validation (useful for CI).
        logger.warning("swagger_auth_skipped", reason="SWAGGER_CLIENT_ID not set")
        return {}

    tenant = settings.AZURE_TENANT_ID or "common"

    try:
        signing_key = _get_jwks_client().get_signing_key_from_jwt(token)
        claims = jwt_decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            audience=[client_id, f"api://{client_id}"],
            issuer=f"https://login.microsoftonline.com/{tenant}/v2.0",
            options={"require": ["exp", "iat", "sub"]},
        )
        logger.info("auth_success", sub=claims.get("sub"), name=claims.get("name"))
        return claims
    except PyJWTError as exc:
        logger.warning("auth_failed", error=str(exc))
        raise credentials_exception
