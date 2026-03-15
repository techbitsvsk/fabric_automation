"""
Azure Functions v2 entry point.

Wraps the existing FastAPI app via AsgiMiddleware so that all routes,
middleware, and auth logic remain untouched.  Every HTTP method on every
path is forwarded through to FastAPI.
"""
import azure.functions as func
from app.main import app as fastapi_app

# Top-level FunctionApp — picked up automatically by the Functions host.
functions_app = func.FunctionApp()

# Instantiate once at module level — avoids repeated startup cost and ensures
# the ASGI lifespan is handled correctly across requests.
_asgi_middleware = func.AsgiMiddleware(fastapi_app)


@functions_app.route(
    route="{*route}",
    auth_level=func.AuthLevel.ANONYMOUS,
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"],
)
async def http_trigger(
    req: func.HttpRequest,
    context: func.Context,
) -> func.HttpResponse:
    """Proxy every inbound HTTP request into the FastAPI ASGI application."""
    return await _asgi_middleware.handle_async(req, context)
