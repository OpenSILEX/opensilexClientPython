"""Centralized logging configuration for opensilex_python_client."""

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

from rich.console import Console
from rich.logging import RichHandler
from rich.text import Text

# Module-level logger — this is what users get via `from opensilex_python_client import logger`
logger: logging.Logger = logging.getLogger(__name__)


class _RichLevelFormatter(logging.Formatter):
    """Format log records with Rich-colored level badges and source file."""

    LEVEL_COLORS = {
        "DEBUG": "dim gray",
        "INFO": "cyan",
        "WARNING": "yellow",
        "ERROR": "red",
        "CRITICAL": "bold red",
    }

    def format(self, record: logging.LogRecord) -> str:
        levelname = record.levelname
        color = self.LEVEL_COLORS.get(levelname, "white")
        level_text = Text.assemble(
            ("[" + levelname + "]", f"bold {color}"),
        )
        ts = self.formatTime(record, self.datefmt)
        # Extract just the filename (no directory)
        src_file = os.path.basename(record.pathname)
        name = record.name
        msg = record.getMessage()
        return f"{ts}  {level_text}  {src_file}  {name}  {msg}"


def get_logger(name: str) -> logging.Logger:
    """Return a logger with the given name.

    This is the preferred way to obtain loggers across the project.
    """
    return logging.getLogger(name)


def setup_logging(
    log_dir: Optional[str] = None,
    log_file: Optional[str] = None,
    level: int = logging.DEBUG,
    enable_markup: bool = True,
) -> None:
    """Centralized logging setup.

    Call this once at application entry point to configure all logging.

    Args:
        log_dir: Directory where the log file will be created.
                 A file named ``<YYYYMMDD>_<HHMMSS>_import.log`` is generated.
        log_file: Full path to a log file (alternative to *log_dir*).
                  If both are provided, *log_file* takes precedence.
        level: Logging level for file handler (default: DEBUG).
        enable_markup: Whether to enable Rich markup in log messages.
    """
    root = logging.getLogger()
    # Avoid re-configuring
    if root.handlers:
        return

    # Console handler: WARNING only (summary lines, errors)
    console_handler = RichHandler(
        console=console,
        rich_tracebacks=True,
        markup=enable_markup,
        show_path=False,
        show_level=True,
        level=logging.WARNING,
    )
    console_handler.setFormatter(_RichLevelFormatter("%(asctime)s"))
    root.addHandler(console_handler)

    # File handler: DEBUG (all details for troubleshooting)
    if log_file:
        parent = Path(log_file).parent
        if not parent.exists():
            parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(_RichLevelFormatter("%(asctime)s"))
        root.addHandler(file_handler)
        root.setLevel(level)