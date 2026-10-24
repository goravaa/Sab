"""Central logging configuration and request context for the API."""

from __future__ import annotations

import logging
import os
import sys
import time
from contextvars import ContextVar
from typing import Any
from uuid import uuid4

DEFAULT_LOG_FORMAT = (
    "%(asctime)s %(levelname)-8s %(name)s "
    "[request_id=%(request_id)s] %(message)s"
)
DEFAULT_LOG_LEVEL = "INFO"
REQUEST_ID_HEADER = "x-request-id"
_SKIP_PATHS = {"/health"}
_SKIP_PREFIXES = ("/static/",)
_request_id: ContextVar[str | None] = ContextVar("request_id", default=None)
_configured = False


class RequestIdFilter(logging.Filter):
    """Attach the active request ID to each log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = _request_id.get() or "-"
        return True


def get_logger(name: str) -> logging.Logger:
    """Return the application logger for a module."""

    return logging.getLogger(name)


def current_request_id() -> str | None:
    """Return the request ID bound to the current execution context."""

    return _request_id.get()


def configure_logging() -> None:
    """Configure logging once from environment variables."""

    global _configured
    if _configured:
        return

    level_name = os.getenv("SAB_LOG_LEVEL", DEFAULT_LOG_LEVEL).upper()
    level = getattr(logging, level_name, logging.INFO)
    log_format = os.getenv("SAB_LOG_FORMAT", DEFAULT_LOG_FORMAT)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(log_format))
    handler.addFilter(RequestIdFilter())

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)

    for name in ("uvicorn", "uvicorn.error"):
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.propagate = True
        logger.setLevel(level)

    access_logger = logging.getLogger("uvicorn.access")
    access_logger.handlers.clear()
    access_logger.disabled = True

    for name in (
        "botocore",
        "urllib3",
        "s3transfer",
        "httpx",
        "httpx2",
        "httpcore",
    ):
        logging.getLogger(name).setLevel(logging.WARNING)

    _configured = True


class RequestLoggingMiddleware:
    """Bind a request ID and log the outcome of every API request."""

    def __init__(self, app: Any) -> None:
        self.app = app
        self.logger = get_logger("api.request")

    async def __call__(self, scope, receive, send) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        skip_logging = path in _SKIP_PATHS or path.startswith(_SKIP_PREFIXES)

        raw_headers = dict(scope.get("headers") or [])
        request_id = (
            raw_headers.get(REQUEST_ID_HEADER.encode(), b"")
            .decode("latin-1")
            .strip()
        )
        request_id = request_id or uuid4().hex[:12]
        token = _request_id.set(request_id)
        method = scope.get("method", "-")
        client = scope.get("client")
        client_host = client[0] if client else "-"
        status_code = 500
        start = time.perf_counter()

        async def send_with_request_id(message: dict[str, Any]) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
                headers = message.setdefault("headers", [])
                headers.append(
                    (REQUEST_ID_HEADER.encode(), request_id.encode("latin-1"))
                )
            await send(message)

        try:
            await self.app(scope, receive, send_with_request_id)
        except Exception:
            duration_ms = (time.perf_counter() - start) * 1000
            if not skip_logging:
                self.logger.exception(
                    "request failed method=%s path=%s duration_ms=%.1f client=%s",
                    method,
                    path,
                    duration_ms,
                    client_host,
                )
            raise
        else:
            duration_ms = (time.perf_counter() - start) * 1000
            if not skip_logging:
                self.logger.info(
                    "request method=%s path=%s status=%d duration_ms=%.1f client=%s",
                    method,
                    path,
                    status_code,
                    duration_ms,
                    client_host,
                )
        finally:
            _request_id.reset(token)


def install_logging(app: Any) -> None:
    """Configure logging and attach request logging to a FastAPI app."""

    configure_logging()
    app.add_middleware(RequestLoggingMiddleware)
