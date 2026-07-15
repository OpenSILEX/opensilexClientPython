# Variable Import

Import CSV data as OpenSILEX variables with configurable column mapping, auto-generated URIs, and group management.

## Overview

The variable import script processes a CSV file to create OpenSILEX variables. For each row, it:

1. **Resolves column mapping** — matches CSV columns to semantic roles (Entity, Characteristic, Method, Unit, Variable, etc.)
2. **Creates components** — auto-generates Entity, Characteristic, Method and Unit components from their names
3. **Creates variables** — creates the variable and links it to the generated components
4. **Attaches to groups** — attaches variables to configured OpenSILEX groups

## Execution Flow

```
┌─────────────────────────┐
│  Parse CLI arguments    │
└───────┬─────────────────┘
        │
        ▼
┌─────────────────────────┐
│  Verify CSV & YAML      │
│  files existence        │
└───────┬─────────────────┘
        │
        ▼
┌─────────────────────────┐
│  Auth connect_to_opensilex│
│  (host, identifier,     │
│   password)             │
└───────┬─────────────────┘
        │
        │  (failure) → error message → exit 1
        ▼
┌──────────────────────────────────────────┐
│  Resolve column mapping                  │
│  - merge defaults with user config       │
│  - validate required columns (abort)     │
│  - warn on missing optional columns      │
└───────┬──────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────┐
│  Step 1: Create components        │
│  - find or create Entity          │
│  - find or create Characteristic   │
│  - find or create Method          │
│  - find or create Unit            │
│  - save enriched CSV with URIs    │
└───────┬───────────────────────────┘
        │
        ▼
┌───────────────────────────────────┐
│  Step 2: Create variables         │
│  - build variable data dict       │
│  - check if variable exists       │
│  - create if not                 │
│  - determine target groups        │
└───────┬───────────────────────────┘
        │
        ▼
┌───────────────────────────────────┐
│  Step 3: Attach to groups         │
│  (skip with --skip-groups)        │
│  - get existing group variables   │
│  - deduplicate and merge          │
│  - update group via API           │
└───────┬───────────────────────────┘
        │
        ▼
┌─────────────────────────┐
│  Summary + exit code 0  │
└─────────────────────────┘
```

## Column Mapping

Each CSV column is mapped to a semantic role. A role represents a part of the variable model (such as entity name, characteristic name, variable description, etc.). The mapping can be configured using either:

- **Column header name** (string): `"Entity_name"`
- **Column index** (integer, 1-indexed): `1`

### Available Roles

| Role | Required | Description | Default Column |
|------|----------|-------------|----------------|
| `entity_name` | Yes | Entity name (ex: Plant, Soil) | `Entity_name` |
| `characteristic_name` | Yes | Characteristic name (ex: Height, Temperature) | `Characteristic_name` |
| `method_name` | Yes | Method name (ex: Manual, Sensor) | `Method_name` |
| `unit_name` | Yes | Unit name (ex: Centimeter, Celsius) | `Unit_name` |
| `variable_name` | Yes | Variable name (ex: Plant_Height_cm) | `Variable_name` |
| `datatype_uri` | Yes | Datatype URI (ex: `http://www.w3.org/2001/XMLSchema#decimal`) | `Datatype_uri` |
| `variable_description` | No | Variable description | `Variable_description` |
| `variable_alternative_name` | No | Alternative variable name | `Variable_alternative_name` |
| `time_interval` | No | Time interval | `Time_interval` |
| `entity_definition` | No | Entity definition | `Entity_Definition` |
| `characteristic_definition` | No | Characteristic definition | `characteristic_definition` |
| `method_definition` | No | Method definition | `Method_Definition` |
| `group1` | No | First group membership | `Group1` |
| `group2` | No | Second group membership | `Group2` |

### Mapping Behavior

- If `csv.column_mappings` is **not** present in the YAML config, the default column names are used automatically
- Any role **not** specified in the config falls back to its default unless explicitly set to `None`
- **Required roles**: if the resolved column is not found in the CSV, the import **aborts** with an error explaining the missing column
- **Optional roles**: if the resolved column is not found, a **warning** is printed and the import continues with an empty value for that field

### Index Resolution

Column indices are 1-based:

| Role | Value | Resolves to |
|------|-------|-------------|
| `entity_name` | `1` | 1st column of the CSV |
| `characteristic_name` | `2` | 2nd column of the CSV |
| `variable_description` | `"Description"` | Column with header `Description` |

An index that is 0, negative, or exceeds the number of columns triggers an error (required) or warning (optional).

## YAML Configuration

### `csv` Section

```yaml
csv:
  # Optional: map semantic roles to CSV columns
  # Value can be an integer (1-indexed) or a string (column header name)
  column_mappings:
    entity_name: 1                    # column index
    characteristic_name: "Caract"      # column header
    method_name: 3
    unit_name: "Unite"
    variable_name: "NomVar"
    datatype_uri: "TypeDonnee"
    variable_description: "Description"
    variable_alternative_name: 7
    time_interval: 8
    group1: 9
    group2: 10
```

#### Validation

Before processing rows, the function validates every column mapping:

| Condition | Required role | Optional role |
|-----------|--------------|---------------|
| Column not found in CSV | **Error** — abort | **Warning** — continue |
| Column index out of range | **Error** — abort | **Warning** — continue |
| Value is neither int nor str | **Error** — abort | **Warning** — continue |

### `groups` Section

```yaml
groups:
  # CSV columns containing group names
  group_columns:
    - "Group1"
    - "Group2"

  # Mapping: Group name (in CSV) → Group URI (in OpenSILEX)
  available_groups:
    "Phenotyping": "http://opensilex.test/id/variablesGroup/phenotyping"
    "Environment": "http://opensilex.test/id/variablesGroup/environment"

  # Default group when no group columns are populated
  default_group: "http://opensilex.test/id/variablesGroup/default"
```

The `find_target_groups` function scans the resolved `group1` and `group2` columns (from the col_map). Each cell may contain multiple group names separated by `;`, `,`, or `|`. Unrecognized group names are silently ignored. If no groups are found and a `default_group` is configured, the default group is used.

## CSV Examples

### Standard CSV (default column names)

```csv
Entity_name,Characteristic_name,Method_name,Unit_name,Variable_name,Datatype_uri,Variable_description,Group1,Group2
Plant,Height,Manual,Centimeter,Plant_Height_cm,http://www.w3.org/2001/XMLSchema#decimal,Plant height in centimeters,Phenotyping,Environment
Soil,Temperature,Sensor,Celsius,Soil_Temp_C,http://www.w3.org/2001/XMLSchema#decimal,Soil temperature in Celsius,Environment,
```

### Custom Headers CSV (mapped via config)

```csv
Entite,Caract,Methode,Unite,NomVar,TypeDonnee,Description,Groupe1,Groupe2
Plant,Hauteur,Manuel,Centimetre,PH_cm,http://www.w3.org/2001/XMLSchema#decimal,Hauteur de la plante,Phenotyping,
```

Corresponding config:

```yaml
csv:
  column_mappings:
    entity_name: "Entite"
    characteristic_name: "Caract"
    method_name: "Methode"
    unit_name: "Unite"
    variable_name: "NomVar"
    datatype_uri: "TypeDonnee"
    variable_description: "Description"
    group1: "Groupe1"
    group2: "Groupe2"
```

### Hybrid Mapping (mix of index and header name)

```yaml
csv:
  column_mappings:
    entity_name: 1                      # 1st column → Entity_name
    characteristic_name: "Characteristic_name"  # header name
    method_name: 3                      # 3rd column
    unit_name: 4
    variable_name: "Variable_name"
    datatype_uri: "Datatype_uri"
    variable_description: "Description"  # custom header
```

## API Functions

| Function | Signature | Description |
|----------|-----------|-------------|
| `resolve_column_mapping(config, df)` | → `tuple[dict, list, list]` | Resolves each role to a column name (or `None`). Returns `(col_map, errors, warnings)` |
| `_resolve_single_column(role, raw_value, df, errors, warnings)` | → `str \| None` | Resolves a single role mapping (int or str) |
| `_get_safe(row, col_map, role, default)` | → `Any` | Reads a value from a row safely, returning `default` when the column is not mapped |
| `find_target_groups(row, config, col_map)` | → `list[str]` | Returns group URIs for a row based on configured group mapping |

## CLI Arguments

| Argument | Meaning | Default value |
|----------|---------|---------------|
| `--host` | Base URL of the OpenSILEX REST API (must include `/rest`). | `http://localhost:8666/rest` |
| `--identifier` | Email address of the OpenSILEX account. | `admin@opensilex.org` |
| `--password` | Password of the OpenSILEX account. | *None* (required) |
| `--csv` | Path to the CSV input file. | – (required) |
| `--config` | Path to the YAML configuration file. | – (required) |
| `--skip-groups` | Skip attaching variables to groups. | `False` |
| `--verbose` / `-v` | Print detailed logs. | `False` |

```bash
uv run run-variable-import \
    --host http://localhost:8666/rest \
    --identifier admin@opensilex.org \
    --password secret \
    --csv variables.csv \
    --config config.yaml \
    --verbose
```

## Download Example Files

```bash
# Downloads test_variables.csv, test_config.yaml, test_variables_with_uris.csv
uv run download-variable-config-example

# Specify destination directory
uv run download-variable-config-example --dest ./config
```

## Error Handling

| Error | Cause | Behavior |
|-------|-------|----------|
| Required column not found | CSV lacks a column needed for a required role | Import **aborts** with exit code 1 |
| Column index out of range | Integer mapping exceeds CSV column count | Import **aborts** (required) or **warns** (optional) |
| Invalid mapping type | Value is not `int` or `str` | Import **aborts** (required) or **warns** (optional) |
| Missing required field in row | A row has an empty value for a required field | Row is **skipped**, counted as failed |
| API call failure | OpenSILEX API returns error | Variable creation is **skipped**, counted as failed |