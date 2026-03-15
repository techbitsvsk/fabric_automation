import logging
import os
import structlog
import uuid
from fastapi import Request

def configure_logging() -> None:
    """Configure structlog for either human-readable console output (DEV_MODE)
    or compact JSON suitable for log aggregators (production).

    Safe to call multiple times — structlog.configure() is idempotent.
    """
    dev_mode = os.getenv("DEV_MODE", "false").lower() == "true"

    # Silence uvicorn's own access logger so it doesn't duplicate lines
    logging.getLogger("uvicorn.access").handlers.clear()
    logging.basicConfig(
        level=logging.DEBUG if dev_mode else logging.INFO,
        format="%(message)s",
    )

    if dev_mode:
        renderer = structlog.dev.ConsoleRenderer(
            colors=True,
            exception_formatter=structlog.dev.plain_traceback,
        )
        time_fmt = "%H:%M:%S"
    else:
        renderer = structlog.processors.JSONRenderer()
        time_fmt = "iso"

    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt=time_fmt, utc=False),
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.format_exc_info,
            renderer,
        ],
        logger_factory=structlog.PrintLoggerFactory(),
        # False so reconfiguration (e.g. in tests) always takes effect
        cache_logger_on_first_use=False,
    )

# Auto-configure at import time so every module that does
# `from app.logging_config import logger` gets the right renderer
# regardless of import order.
configure_logging()

logger = structlog.get_logger()


def stage(number: int, title: str) -> None:
    """Print a visible stage banner to the console.

    Emits a separator line so provisioning stages are easy to scan in the
    terminal.  Example output::

        ---- STAGE 1 : Workspace ----------------------------------------

    Args:
        number: Stage number shown in the banner.
        title:  Short human-readable label for the stage.
    """
    banner = f"---- STAGE {number} : {title} "
    banner = banner.ljust(64, "-")
    logger.info(banner)

async def correlation_middleware(request: Request, call_next):
    correlation_id = request.headers.get("X-Correlation-ID", str(uuid.uuid4()))
    request.state.correlation_id = correlation_id
    logger.info("request_received", path=str(request.url), method=request.method, correlation_id=correlation_id)
    response = await call_next(request)
    response.headers["X-Correlation-ID"] = correlation_id
    logger.info("response_sent", path=str(request.url), status_code=response.status_code, correlation_id=correlation_id)
    return response
