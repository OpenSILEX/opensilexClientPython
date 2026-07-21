"""Shared context for variable import operations."""

import logging
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class VariablesContext:
    """Carries shared state for variable import pipeline."""

    client: Any
    config: dict[str, Any] = field(default_factory=dict)
    debug: bool = False

    def debug_log(self, msg: str) -> None:
        logger.debug(msg)

    def clean_uri(self, value: Any) -> str | None:
        if value is None or pd.isna(value):
            return None

        uri_str = str(value).strip()

        if not uri_str or uri_str == "None" or uri_str == "nan":
            return None

        if uri_str.startswith("["):
            uri_str = uri_str.strip("[]").strip("'").strip('"').strip()

        return uri_str if uri_str else None

    def row_value(self, row: pd.Series, col_map: dict[str, str | None], role: str, default: str = "") -> Any:
        col_name = col_map.get(role)
        if col_name is None:
            return default
        return row.get(col_name, default)
