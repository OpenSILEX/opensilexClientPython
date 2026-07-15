"""Import variables from CSV file - AUTO-GENERATION from names only."""

import sys
from typing import Any

import pandas as pd

from ..file_management.read_csv import read_csv
from ..file_management.read_yaml import read_yaml
from ._components.characteristic import find_or_create_characteristic
from ._components.entity import find_or_create_entity
from ._components.method import find_or_create_method
from ._components.unit import find_or_create_unit
from .create import create_variable
from .exists import exists
from .groups.find import find_target_groups

# Default column mapping when no column_mappings section is provided
DEFAULT_COLUMN_MAPPINGS: dict[str, str] = {
    "entity_name": "Entity_name",
    "characteristic_name": "Characteristic_name",
    "method_name": "Method_name",
    "unit_name": "Unit_name",
    "variable_name": "Variable_name",
    "variable_alternative_name": "Variable_alternative_name",
    "variable_description": "Variable_description",
    "datatype_uri": "Datatype_uri",
    "time_interval": "Time_interval",
    "entity_definition": "Entity_Definition",
    "characteristic_definition": "characteristic_definition",
    "method_definition": "Method_Definition",
    "group1": "Group1",
    "group2": "Group2",
}

# Roles that are mandatory in the CSV
REQUIRED_ROLES = [
    "entity_name",
    "characteristic_name",
    "method_name",
    "unit_name",
    "variable_name",
    "datatype_uri",
]

# Roles that are optional — missing column triggers a warning
OPTIONAL_ROLES = [
    "variable_alternative_name",
    "variable_description",
    "time_interval",
    "entity_definition",
    "characteristic_definition",
    "method_definition",
    "group1",
    "group2",
]

# All known roles
ALL_ROLES = REQUIRED_ROLES + OPTIONAL_ROLES


def resolve_column_mapping(
    config: dict[str, Any], df: pd.DataFrame
) -> tuple[dict[str, str | None], list[str], list[str]]:
    """Resolve column mapping from config, falling back to defaults.

    For each role, the config value may be:
      - an int (1-indexed column number) → resolved via df.columns
      - a str (column header name) → used as-is

    Returns:
        col_map: dict {role: resolved_column_name} (None for optional+missing)
        errors: list of messages for required-role failures
        warnings: list of messages for optional-role failures
    """
    raw_mappings = config.get("csv", {}).get("column_mappings", {})

    merged: dict[str, Any] = dict(DEFAULT_COLUMN_MAPPINGS)
    if raw_mappings:
        merged.update(raw_mappings)

    col_map: dict[str, str | None] = {}
    errors: list[str] = []
    warnings: list[str] = []

    for role in ALL_ROLES:
        raw_value = merged.get(role)

        if raw_value is None:
            if role in REQUIRED_ROLES:
                errors.append(f"Required column for '{role}' is not configured")
                col_map[role] = None
            else:
                warnings.append(f"Optional column for '{role}' is not configured")
                col_map[role] = None
            continue

        resolved = _resolve_single_column(role, raw_value, df, errors, warnings)
        col_map[role] = resolved

    return col_map, errors, warnings


def _resolve_single_column(
    role: str,
    raw_value: Any,
    df: pd.DataFrame,
    errors: list[str],
    warnings: list[str],
) -> str | None:
    """Resolve a single role to a column name."""

    if isinstance(raw_value, int):
        if raw_value < 1 or raw_value > len(df.columns):
            is_req = role in REQUIRED_ROLES
            msg = (
                f"[ERROR] Column index {raw_value} for '{role}' is out of range "
                f"(1-{len(df.columns)})"
                if is_req
                else f"Column index {raw_value} for '{role}' is out of range "
                f"(1-{len(df.columns)})"
            )
            if is_req:
                errors.append(msg)
            else:
                warnings.append(msg)
            return None
        return df.columns[raw_value - 1]

    if isinstance(raw_value, str):
        if raw_value not in df.columns:
            is_req = role in REQUIRED_ROLES
            msg = (
                f"[ERROR] Column '{raw_value}' for '{role}' not found in CSV. "
                f"Available: {list(df.columns)}"
                if is_req
                else f"Column '{raw_value}' for '{role}' not found in CSV"
            )
            if is_req:
                errors.append(msg)
            else:
                warnings.append(msg)
            return None
        return raw_value

    is_req = role in REQUIRED_ROLES
    msg = (
        f"[ERROR] Invalid mapping for '{role}': expected int or str, got {type(raw_value).__name__}"
        if is_req
        else f"Invalid mapping for '{role}': expected int or str, got {type(raw_value).__name__}"
    )
    if is_req:
        errors.append(msg)
    else:
        warnings.append(msg)
    return None


def _get_safe(row: pd.Series, col_map: dict[str, str | None], role: str, default: str = "") -> Any:
    """Get value from row using column mapping, safely handling missing optional columns."""
    col_name = col_map.get(role)
    if col_name is None:
        return default
    return row.get(col_name, default)


def _clean_uri(uri_value):
    """Clean URI to ensure it's a simple string, not a list."""
    if uri_value is None or pd.isna(uri_value):
        return None

    uri_str = str(uri_value).strip()

    if not uri_str or uri_str == "None" or uri_str == "nan":
        return None

    if uri_str.startswith("["):
        uri_str = uri_str.strip("[]").strip("'").strip('"').strip()

    return uri_str if uri_str else None


def run(client, csv_path: str, config_path: str) -> dict[str, list[str]]:
    """Import variables from CSV file.

    Creates ALL components and variables automatically from NAMES only.
    """
    print("\n📄 Loading CSV and configuration...")
    df = read_csv(csv_path)
    config = read_yaml(config_path)
    print(f"✓ Loaded {len(df)} rows from CSV")

    # Resolve column mapping
    print("\n🔍 Resolving column mappings...")
    col_map, errors, warnings = resolve_column_mapping(config, df)

    for w in warnings:
        print(f"  ⚠️  {w}")

    if errors:
        print("\n❌ Column mapping errors — import aborted:")
        for e in errors:
            print(f"  ✗ {e}")
        sys.exit(1)

    print(f"  ✓ Mapped {sum(1 for v in col_map.values() if v is not None)} columns")
    mapped_cols = {k: v for k, v in col_map.items() if v is not None}

    # STEP 1: Create components FROM NAMES ONLY
    print("\n" + "=" * 80)
    print("STEP 1: CREATING COMPONENTS (AUTO-GENERATION FROM NAMES)")
    print("=" * 80)

    df_enriched = df.copy()
    df_enriched["Generated_Entity_uri"] = None
    df_enriched["Generated_Characteristic_uri"] = None
    df_enriched["Generated_Method_uri"] = None
    df_enriched["Generated_Unit_uri"] = None

    for idx, row in df.iterrows():
        entity_name = _get_safe(row, col_map, "entity_name")
        if pd.notna(entity_name):
            entity_desc = _get_safe(row, col_map, "entity_definition", "")
            uri = find_or_create_entity(client, str(entity_name), str(entity_desc))
            df_enriched.at[idx, "Generated_Entity_uri"] = _clean_uri(uri)

        char_name = _get_safe(row, col_map, "characteristic_name")
        if pd.notna(char_name):
            char_desc = _get_safe(row, col_map, "characteristic_definition", "")
            uri = find_or_create_characteristic(client, str(char_name), str(char_desc))
            df_enriched.at[idx, "Generated_Characteristic_uri"] = _clean_uri(uri)

        method_name = _get_safe(row, col_map, "method_name")
        if pd.notna(method_name):
            method_desc = _get_safe(row, col_map, "method_definition", "")
            uri = find_or_create_method(client, str(method_name), str(method_desc))
            df_enriched.at[idx, "Generated_Method_uri"] = _clean_uri(uri)

        unit_name = _get_safe(row, col_map, "unit_name")
        if pd.notna(unit_name):
            uri = find_or_create_unit(client, str(unit_name))
            df_enriched.at[idx, "Generated_Unit_uri"] = _clean_uri(uri)

    # Save enriched CSV
    output_path = csv_path.replace(".csv", "_with_uris.csv")
    df_enriched.to_csv(output_path, index=False, encoding="utf-8")
    print(f"\n✓ Enriched CSV saved: {output_path}")

    # STEP 2: Create variables
    print("\n" + "=" * 80)
    print("STEP 2: CREATING VARIABLES")
    print("=" * 80)

    grouped_variables = {}
    created = 0
    existing = 0
    failed = 0

    for idx, row in df_enriched.iterrows():
        entity_uri = _clean_uri(df_enriched.at[idx, "Generated_Entity_uri"])
        char_uri = _clean_uri(df_enriched.at[idx, "Generated_Characteristic_uri"])
        method_uri = _clean_uri(df_enriched.at[idx, "Generated_Method_uri"])
        unit_uri = _clean_uri(df_enriched.at[idx, "Generated_Unit_uri"])

        var_data = {
            "name": _get_safe(row, col_map, "variable_name"),
            "alternative_name": _get_safe(row, col_map, "variable_alternative_name", ""),
            "description": _get_safe(row, col_map, "variable_description", ""),
            "entity": entity_uri,
            "characteristic": char_uri,
            "method": method_uri,
            "unit": unit_uri,
            "datatype": _get_safe(row, col_map, "datatype_uri"),
            "time_interval": _get_safe(row, col_map, "time_interval", ""),
        }

        if not all(
            [
                var_data["name"],
                var_data["entity"],
                var_data["characteristic"],
                var_data["method"],
                var_data["unit"],
                var_data["datatype"],
            ]
        ):
            print(f"  ✗ Missing required field for variable: {var_data.get('name', 'UNKNOWN')}")
            failed += 1
            continue

        existing_uri = exists(client, name=var_data["name"])
        if existing_uri:
            variable_uri = existing_uri
            existing += 1
        else:
            variable_uri = create_variable(client, var_data)
            if variable_uri:
                created += 1
            else:
                failed += 1
                continue

        # Determine target groups
        target_groups = find_target_groups(row, config, mapped_cols)

        for group_uri in target_groups:
            if group_uri not in grouped_variables:
                grouped_variables[group_uri] = []
            grouped_variables[group_uri].append(variable_uri)

    print("\n📊 STEP 2 SUMMARY:")
    print(f"   Created: {created}, Existing: {existing}, Failed: {failed}")

    return grouped_variables
