"""Find target groups for variables."""

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


def find_target_groups(
    row: pd.Series,
    config: dict[str, Any],
    columns: list[str] | None = None,
) -> list[str]:
    """Find which groups a variable should belong to.

    Uses groups.group_mapping (dict: group_name → column).
    A row is assigned to a group when the cell value equals the group name.

    Args:
        row: CSV row
        config: Configuration dict with group settings
        columns: List of CSV column names (used to resolve integer-based mappings)

    Returns:
        List of group URIs for this variable
    """
    group_config = config.get("groups", {})
    available_groups = group_config.get("available_groups", {})
    default_group = group_config.get("default_group")
    group_mapping = group_config.get("group_mapping", {})

    target_groups: list[str] = []

    for group_name, col_spec in group_mapping.items():
        col_name = _resolve_group_column(col_spec, columns)
        if col_name is None:
            continue
        if col_name not in row or pd.isna(row[col_name]):
            continue
        cell = str(row[col_name]).strip()
        if not cell:
            continue
        group_names = cell.replace(",", ";").replace("|", ";").split(";")
        for gname in group_names:
            gname = gname.strip()
            if gname == group_name and group_name in available_groups:
                group_uri = available_groups[group_name]
                if group_uri not in target_groups:
                    target_groups.append(group_uri)

    if not target_groups and default_group:
        target_groups.append(default_group)

    return target_groups


def _resolve_group_column(
    col_spec: Any,
    columns: list[str] | None,
) -> str | None:
    """Resolve a group_mapping column spec to a column name.

    Supports:
      - str → column header name (returned as-is)
      - int → 1-indexed column number (resolved via columns)
    """
    if isinstance(col_spec, int):
        if columns and 1 <= col_spec <= len(columns):
            return columns[col_spec - 1]
        return None
    if isinstance(col_spec, str):
        return col_spec
    return None
