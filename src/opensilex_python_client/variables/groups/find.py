"""Find target groups for variables."""

from typing import Any

import pandas as pd


def find_target_groups(
    row: pd.Series,
    config: dict[str, Any],
    col_map: dict[str, str] | None = None,
) -> list[str]:
    """Find which groups a variable should belong to.

    Args:
        row: CSV row containing group information
        config: Configuration dictionary with group settings
        col_map: Optional resolved column mapping {role: column_name}.
                 If provided, group1/group2 are looked up via this mapping.
                 If not provided, falls back to config groups.group_columns.

    Returns:
        List of group URIs for this variable
    """
    group_config = config.get("groups", {})
    available_groups = group_config.get("available_groups", {})
    default_group = group_config.get("default_group")
    group_columns = group_config.get("group_columns", ["Group1", "Group2"])

    target_groups = []

    # Determine which columns to scan for group names
    scanned_cols: list[str] = []
    if col_map:
        for role in ("group1", "group2"):
            col_name = col_map.get(role)
            if col_name and col_name not in scanned_cols:
                scanned_cols.append(col_name)
    if not scanned_cols:
        scanned_cols = group_columns

    for col in scanned_cols:
        if col in row and pd.notna(row[col]):
            group_names = str(row[col]).replace(",", ";").replace("|", ";").split(";")
            for gname in group_names:
                gname = gname.strip()
                if gname in available_groups:
                    group_uri = available_groups[gname]
                    if group_uri not in target_groups:
                        target_groups.append(group_uri)

    if not target_groups and default_group:
        target_groups.append(default_group)

    return target_groups
