from fastapi import FastAPI
from app.logging_config import correlation_middleware
from app.api.v1.routes_fabric import router as fabric_router
from app.config import settings

app = FastAPI(
    title="Fabric Control Plane",
    version="1.0.0",
    # Prevent FastAPI from prepending the ASGI root_path to server URLs in the
    # OpenAPI spec.  AsgiMiddleware injects root_path="/api" even when
    # routePrefix is "", which breaks the Swagger UI's fetch of /openapi.json.
    root_path_in_servers=False,
    # Tell Swagger UI where to redirect after the OAuth2 login completes.
    swagger_ui_oauth2_redirect_url="/docs/oauth2-redirect",
    swagger_ui_init_oauth={
        "clientId": settings.SWAGGER_CLIENT_ID,
        "scopes": settings.SWAGGER_SCOPES,
        # Use Authorization Code + PKCE — no client secret needed in the browser.
        "usePkceWithAuthorizationCodeGrant": True,
    },
)

app.middleware("http")(correlation_middleware)
app.include_router(fabric_router)
