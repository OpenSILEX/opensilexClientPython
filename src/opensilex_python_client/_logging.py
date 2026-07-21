"""Lazy RichHandler setup for the opensilex_python_client package."""

import logging

from rich.logging import RichHandler

# Module-level logger — this is what users get via `from opensilex_python_client import logger`
logger: logging.Logger = logging.getLogger(__name__)


def _ensure_root_logger() -> logging.Logger:
    """Configure root logger with RichHandler on first call."""
    root = logging.getLogger()
    if root.handlers:
        return root
    handler = RichHandler(
        rich_tracebacks=True,
        markup=False,
        show_path=False,
        show_level=False,
    )
    handler.setFormatter(logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s"))
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)
    return root


# Ensure handler is registered immediately when this module is imported.
# This is triggered by __init__.py which imports `logger` from this module.
_ensure_root_logger()
