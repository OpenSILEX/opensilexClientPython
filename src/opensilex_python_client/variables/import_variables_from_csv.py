"""Import variables from CSV file - AUTO-GENERATION from names only."""

import sys
from pathlib import Path
from typing import Any

import pandas as pd
from rich.progress import Progress, track

from .._logging import get_logger, setup_logging
from ..file_management.read_csv import read_csv
from ..file_management.read_yaml import read_yaml
from ._component_resolver import ComponentResolutionStats, find_or_create_component
from .create import VariableData, create_variable_ctx
from .ctx import VariablesContext
from .exists import exists_variable_ctx
from .groups.find import find_target_groups

logger = get_logger(__name__)

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
    "entity_uri": "Entity_uri",
    "characteristic_uri": "Characteristic_uri",
    "method_uri": "Method_uri",
    "unit_uri": "Unit_uri",
    "variable_uri": "Variable_uri"
}

REQUIRED_ROLES = [
    "entity_name",
    "characteristic_name",
    "method_name",
    "unit_name",
    "variable_name",
    "datatype_uri",
]

OPTIONAL_ROLES = [
    "variable_alternative_name",
    "variable_description",
    "time_interval",
    "entity_definition",
    "characteristic_definition",
    "method_definition",
    "entity_uri",
    "characteristic_uri",
    "method_uri",
    "unit_uri",
    "variable_uri"

]

ALL_ROLES = REQUIRED_ROLES + OPTIONAL_ROLES

COMPONENT_FIELDS = [
    ("entity", "Entity"),
    ("characteristic", "Characteristic"),
    ("method", "Method"),
    ("unit", "Unit"),
]


def _validate_variables(df: pd.DataFrame, col_map: dict[str, str | None]) -> tuple[pd.DataFrame, bool]:
    """Validate required fields and duplicates. Return filtered DF and whether all are valid."""
    ignored = []
    seen_names = {}
    valid_indices = []

    var_name_col = col_map.get("variable_name")

    for idx, row in df.iterrows():
        reasons = []

        for role in REQUIRED_ROLES:
            col = col_map.get(role)
            val = row[col] if col and col in df.columns else None
            if pd.isna(val) or str(val).strip() == "":
                reasons.append(f"Missing {role}")

        if var_name_col and var_name_col in df.columns:
            name = str(row[var_name_col]).strip()
            if name and name != "nan":
                if name in seen_names:
                    reasons.append(f"Duplicate name (first seen at row {seen_names[name] + 1})")
                else:
                    seen_names[name] = idx

        if reasons:
            ignored.append(
                {
                    "index": idx + 1,
                    "name": row[var_name_col] if var_name_col in df.columns else "N/A",
                    "reasons": ", ".join(reasons),
                }
            )
        else:
            valid_indices.append(idx)

    if ignored:
        ignored_count = len(ignored)
        created_count = len(valid_indices)
        logger.warning("Some variables will be ignored (count: %d)", ignored_count)
        for item in ignored:
            logger.debug("Ignored row %s - name=%s - reasons=%s", item["index"], item["name"], item["reasons"])
        print(
            f"\nSome variables will be ignored: {ignored_count}\n"
            f"Variables to be created: {created_count}\n"
            f"Do you want to proceed with the creation of the valid variables? (y/n): "
        )
        choice = input()
        if choice.lower() != "y":
            logger.info("Import aborted by user")
            sys.exit(0)

    return df.loc[valid_indices].reset_index(drop=True), True


def resolve_column_mapping(
    ctx: VariablesContext, df: pd.DataFrame
) -> tuple[dict[str, str | None], list[str], list[str]]:
    """Resolve column mapping from ctx.config, falling back to defaults."""
    raw_mappings = ctx.config.get("csv", {}).get("column_mappings", {})

    merged: dict[str, Any] = dict(DEFAULT_COLUMN_MAPPINGS)
    if raw_mappings:
        merged.update(raw_mappings)

    col_map: dict[str, str | None] = {}
    errors: list[str] = []
    warnings: list[str] = []

    roles_to_check: set[str] = set(merged.keys()) | set(ALL_ROLES)

    for role in roles_to_check:
        raw_value = merged.get(role)

        if raw_value is None:
            if role in REQUIRED_ROLES:
                errors.append(f"Required column for '{role}' is not configured")
                col_map[role] = None
            else:
                warnings.append(f"Optional column for '{role}' is not configured")
                col_map[role] = None
            continue

        resolved = _resolve_single_column(role, raw_value, df, errors, warnings, ctx.debug_log)
        col_map[role] = resolved

    return col_map, errors, warnings


def _resolve_single_column(
    role: str,
    raw_value: Any,
    df: pd.DataFrame,
    errors: list[str],
    warnings: list[str],
    debug_log,
) -> str | None:
    """Resolve a single role to a column name."""

    if isinstance(raw_value, int):
        if raw_value < 1 or raw_value > len(df.columns):
            is_req = role in REQUIRED_ROLES
            msg = f"Column index {raw_value} for '{role}' is out of range (1-{len(df.columns)})"
            if is_req:
                errors.append(msg)
            else:
                warnings.append(msg)
            return None
        return df.columns[raw_value - 1]

    if isinstance(raw_value, str):
        if raw_value not in df.columns:
            is_req = role in REQUIRED_ROLES
            msg = f"Column '{raw_value}' for '{role}' not found in CSV"
            if is_req:
                errors.append(msg)
            else:
                warnings.append(msg)
            return None
        return raw_value

    is_req = role in REQUIRED_ROLES
    msg = f"Invalid mapping for '{role}': expected int or str, got {type(raw_value).__name__}"
    if is_req:
        errors.append(msg)
    else:
        warnings.append(msg)
    return None


def _resolve_components(ctx: VariablesContext, df: pd.DataFrame, col_map: dict[str, str | None]) -> pd.DataFrame:
    """For each CSV row, resolve entity/char/method/unit and enrich DataFrame."""
    enriched = df.copy()
    for _comp_key, comp_title in COMPONENT_FIELDS:
        enriched[f"Final_{comp_title}_URI"] = None

    total = len(df) * len(COMPONENT_FIELDS)
    stats = ComponentResolutionStats()
    
    with Progress() as p:
        pbar = p.add_task("Resolving components...", total=total)
        for idx, row in df.iterrows():
            for comp_key, comp_title in COMPONENT_FIELDS:
                uri = ctx.clean_uri(ctx.row_value(row, col_map, f"{comp_key}_uri"))
                name = ctx.row_value(row, col_map, f"{comp_key}_name")
                desc = str(ctx.row_value(row, col_map, f"{comp_key}_definition", ""))
                if pd.notna(name):
                    resolved, comp_stats = find_or_create_component(ctx, comp_key, uri, str(name), desc)
                    stats.created += comp_stats.created
                    stats.existing += comp_stats.existing
                    stats.failed += comp_stats.failed
                    enriched.at[idx, f"Final_{comp_title}_URI"] = ctx.clean_uri(resolved)
                p.advance(pbar)
    
    logger.info("Components summary: created=%d, existing=%d, failed=%d", stats.created, stats.existing, stats.failed)
    return enriched


def _build_variable_data(
    ctx: VariablesContext, row: pd.Series, col_map: dict[str, str | None], df: pd.DataFrame, idx: int
) -> VariableData:
    """Build VariableData from an enriched row."""
    entity_uri = ctx.clean_uri(df.at[idx, "Final_Entity_URI"])
    char_uri = ctx.clean_uri(df.at[idx, "Final_Characteristic_URI"])
    method_uri = ctx.clean_uri(df.at[idx, "Final_Method_URI"])
    unit_uri = ctx.clean_uri(df.at[idx, "Final_Unit_URI"])

    return VariableData(
        uri=str(ctx.row_value(row, col_map, "variable_uri")) or None,
        name=str(ctx.row_value(row, col_map, "variable_name")),
        entity=entity_uri or "",
        characteristic=char_uri or "",
        method=method_uri or "",
        unit=unit_uri or "",
        datatype=str(ctx.row_value(row, col_map, "datatype_uri")),
        alternative_name=str(ctx.row_value(row, col_map, "variable_alternative_name", "")),
        description=str(ctx.row_value(row, col_map, "variable_description", "")),
        time_interval=str(ctx.row_value(row, col_map, "time_interval", "")),
    )


def _create_variables(
    ctx: VariablesContext, df: pd.DataFrame, col_map: dict[str, str | None]
) -> tuple[dict[str, list[str]], pd.DataFrame]:
    """Create variables from enriched rows, collect group assignments and return enriched DF."""
    grouped: dict[str, list[str]] = {}
    created = 0
    existing = 0
    failed = 0

    enriched_vars = df.copy()
    enriched_vars["Final_Variable_URI"] = None

    for idx, row in track(df.iterrows(), total=len(df), description="Creating variables"):
        var_data = _build_variable_data(ctx, row, col_map, df, idx)
        if not var_data.valid:
            logger.error("Missing required field for variable: %s", var_data.name)
            failed += 1
            continue

        existing_uri = exists_variable_ctx(ctx, name=var_data.name,uri=var_data.uri)
        if existing_uri:
            variable_uri = existing_uri
            existing += 1
        else:
            variable_uri = create_variable_ctx(ctx, var_data)
            if variable_uri:
                created += 1
            else:
                failed += 1
                continue

        enriched_vars.at[idx, "Final_Variable_URI"] = variable_uri

        for group_uri in find_target_groups(row, ctx.config, list(df.columns)):
            grouped.setdefault(group_uri, []).append(variable_uri)

    logger.info("Variable creation summary: created=%d, existing=%d, failed=%d", created, existing, failed)
    return grouped, enriched_vars


def _save_enriched_csv(df: pd.DataFrame, csv_path: str) -> None:
    output_path = csv_path.replace(".csv", "_enriched.csv")
    df.to_csv(output_path, index=False, encoding="utf-8")
    logger.info("Enriched CSV saved: %s", output_path)


def run(client: Any, csv_path: str, config_path: str, debug: bool = False) -> dict[str, list[str]]:
    """Import variables from CSV file.

    Args:
        client: OpenSILEX client instance
        csv_path: Path to the CSV file
        config_path: Path to the YAML config file
        debug: If True, print API call details (requests/responses, tracebacks)
    """
    logger.info("Loading CSV and configuration: csv=%s, config=%s", csv_path, config_path)
    print("\nLoading CSV and configuration...")
    df = read_csv(csv_path)
    config = read_yaml(config_path)

    # Set up log file in the same directory as the CSV
    setup_logging(log_dir=str(Path(csv_path).parent))

    ctx = VariablesContext(client=client, config=config, debug=debug)
    logger.info("Loaded %d rows from CSV", len(df))
    print(f"Loaded {len(df)} rows from CSV")

    print("\nResolving column mappings...")
    col_map, errors, warnings = resolve_column_mapping(ctx, df)

    for w in warnings:
        logger.warning("Optional column for '%s' is not configured", w)

    if errors:
        logger.error("Column mapping errors - import aborted: %s", errors)
        print("\nColumn mapping errors - import aborted:")
        for e in errors:
            print(f"  {e}")
        sys.exit(1)

    mapped_count = sum(1 for v in col_map.values() if v is not None)
    logger.info("Resolved column mappings: %d columns mapped", mapped_count)
    print(f"  Mapped {mapped_count} columns")

    # Validate variables (required fields & duplicates) BEFORE creating any components
    df, _ = _validate_variables(df, col_map)

    logger.info("Starting component creation step")
    print("\n" + "=" * 80)
    print("STEP 1: CREATING COMPONENTS (AUTO-GENERATION FROM NAMES)")
    print("=" * 80)

    enriched = _resolve_components(ctx, df, col_map)

    logger.info("Starting variable creation step")
    print("\n" + "=" * 80)
    print("STEP 2: CREATING VARIABLES")
    print("=" * 80)

    grouped, final_enriched = _create_variables(ctx, enriched, col_map)

    # Save the final enriched CSV including variable URIs
    _save_enriched_csv(final_enriched, csv_path)

    return grouped