# Variable Import

Import CSV data as OpenSILEX variables with configurable column mapping, auto-generated URIs, and group management.

## Overview

The variable import script processes a CSV file to create OpenSILEX variables. For each row, it:

1. **Resolves column mapping** — matches CSV columns to semantic roles (Entity, Characteristic, Method, Unit, Variable, etc.)
2. **Validates variables** — checks for required fields and duplicate variable names. Displays a summary table of ignored variables and requests user confirmation before proceeding.
3. **Creates components** — resolves or creates Entity, Characteristic, Method and Unit components (by URI if provided, otherwise by name)
4. **Creates variables** — creates the variable and links it to the resolved components
5. **Attaches to groups** — attaches variables to configured OpenSILEX groups

## Usage

The script can be used in two ways:

### CLI (Command Line)

```bash
uv run run-variable-import \
    --host http://localhost:8666/rest \
    --identifier admin@opensilex.org \
    --password secret \
    --csv variables.csv \
    --config config.yaml \
    --verbose
```

### Programmatic API (Notebook / Script)

```python
from opensilex_python_client.auth import connect
from opensilex_python_client.variables import import_from_csv
from opensilex_python_client.variables.groups import update

# 1. Authenticate
with open('credentials.json') as f:
    credentials_dict = json.load(f)

client = connect.connect_to_opensilex(credentials_dict)

# 2. Import (creates components + variables automatically)
# Third argument 'True' enables verbose output
grouped_vars = import_from_csv.run(client, "variables.csv", "config.yaml", True)

# 3. Attach to groups
manage.attach_variables(client, grouped_vars, "config.yaml")
```

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
│  Validate Variables               │
│  - check required fields          │
│  - detect duplicate names         │
│  - prompt user for confirmation   │
└───────┬───────────────────────────┘
        │
        ▼
┌───────────────────────────────────┐
│  Step 1: Create components        │
│  - find or create Entity          │
│  - find or create Characteristic   │
│  - find or create Method          │
│  - find or create Unit            │
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
│  Step 3: Finalize and Save        │
│  - save enriched CSV with ALL URIs │
│  - attach to groups                │
└───────┬───────────────────────────┘
        │
        ▼
┌─────────────────────────┐
│  Summary + exit code 0  │
└─────────────────────────┘
```

## Component Resolution (find_or_create)

Each of the four component types (Entity, Characteristic, Method, Unit) is resolved through the same `find_or_create` pattern:

```
┌─────────────────────────────┐
│  URI provided in CSV?       │
│                             │
│  Yes → get_by_uri() API     │
│         Found → return URI  │
│         Not found → fall through
│  No  → fall through         │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│  search_by_name() API       │
│  Found → return URI         │
│  Not found → fall through   │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│  create() API               │
│  (using name + description) │
│  Return new URI             │
└─────────────────────────────┘
```

### Resolution Details

| Step | API Call | When |
|------|----------|------|
| **Look up by URI** | `get_entity(uri)`, `get_characteristic(uri)`, etc. | When the CSV provides a `{component}_uri` column with a value |
| **Search by name** | `search_entities(name=...)`, `search_characteristics(name=...)`, etc. | Fallback when URI lookup fails or no URI is provided |
| **Create** | `create_entity(body=...)`, `create_characteristic(body=...)`, etc. | Last resort when the component doesn't exist in the system |

This means you can:
- **Reference existing components by URI** — if the CSV contains `Entity_uri=http://purl.example/id/123`, the script looks it up directly
- **Reference by name only** — if only `Entity_name=Plant` is provided, the script searches for an existing component with that name and reuses it
- **Auto-create** — if neither URI nor name matches an existing component, a new one is created from the name (and description if provided)

### Duplicate Handling

Components are deduplicated automatically. If the same entity name (or URI) appears in multiple CSV rows, the component is created only once and the same URI is reused for all rows.

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
| `entity_uri` | No | Existing Entity URI to link (bypasses search-and-create) | `Entity_uri` |
| `characteristic_uri` | No | Existing Characteristic URI to link | `Characteristic_uri` |
| `method_uri` | No | Existing Method URI to link | `Method_uri` |
| `unit_uri` | No | Existing Unit URI to link | `Unit_uri` |

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

## Enriched CSV Output

After the import process, the script writes an enriched CSV file alongside the original:

- `variables.csv` → `variables_enriched.csv`

The enriched CSV contains all original columns plus five generated columns:

| Generated Column | Source |
|---|---|
| `Final_Entity_URI` | URI resolved/created for the entity |
| `Final_Characteristic_URI` | URI resolved/created for the characteristic |
| `Final_Method_URI` | URI resolved/created for the method |
| `Final_Unit_URI` | URI resolved/created for the unit |
| `Final_Variable_URI` | URI resolved/created for the variable |

This enriched file can be reused in subsequent imports to reference components and variables by their resolved URIs, avoiding redundant lookups or creations.

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
    entity_uri: 9                     # optional: reference existing Entity by URI
    group1: 10
    group2: 11
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

### `--create-groups` Flag

When `--create-groups` is passed on the CLI, groups listed in `available_groups` are automatically created if they don't exist:

```bash
uv run run-variable-import \
    --host http://localhost:8666/rest \
    --identifier admin@opensilex.org \
    --password secret \
    --csv variables.csv \
    --config config.yaml \
    --create-groups
```

## CSV Examples

### Standard CSV (default column names)

```csv
Entity_name,Characteristic_name,Method_name,Unit_name,Variable_name,Datatype_uri,Variable_description,Group1,Group2
Plant,Height,Manual,Centimeter,Plant_Height_cm,http://www.w3.org/2001/XMLSchema#decimal,Plant height in centimeters,Phenotyping,Environment
Soil,Temperature,Sensor,Celsius,Soil_Temp_C,http://www.w3.org/2001/XMLSchema#decimal,Soil temperature in Celsius,Environment,
```

### CSV with URI references (reuse existing components)

```csv
Entity_uri,Entity_name,Characteristic_uri,Characteristic_name,Method_name,Unit_name,Variable_name,Datatype_uri
http://purl.example/id/Entity/plant,Plant,http://purl.example/id/Characteristic/height,Height,Manual,Centimeter,Plant_Height_cm,http://www.w3.org/2001/XMLSchema#decimal
http://purl.example/id/Entity/soil,Soil,,Temperature,Sensor,Celsius,Soil_Temp_C,http://www.w3.org/2001/XMLSchema#decimal
```

In this example:
- Row 1: Entity and Characteristic are looked up by URI; Method and Unit are resolved by name.
- Row 2: Entity is looked up by URI; Characteristic has no URI and is resolved by name (search or create).

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
| `find_or_create_entity(client, uri, name, description)` | → `str \| None` | Look up entity by URI, then by name, or create it |
| `find_or_create_characteristic(client, uri, name, description)` | → `str \| None` | Look up characteristic by URI, then by name, or create it |
| `find_or_create_method(client, uri, name, description)` | → `str \| None` | Look up method by URI, then by name, or create it |
| `find_or_create_unit(client, uri, name, description)` | → `str \| None` | Look up unit by URI, then by name, or create it |
| `find_or_create_group(client, uri, name, description)` | → `str \| None` | Look up group by URI, then by name, or create it |
| `exists(client, name, uri)` | → `str \| None` | Check if a variable exists by name or URI |
| `create_variable(client, data)` | → `str \| None` | Create a variable from a data dict and return its URI |

## CLI Arguments

| Argument | Meaning | Default value |
|----------|---------|---------------|
| `--host` | Base URL of the OpenSILEX REST API (must include `/rest`). | `http://localhost:8666/rest` |
| `--identifier` | Email address of the OpenSILEX account. | `admin@opensilex.org` |
| `--password` | Password of the OpenSILEX account. | *None* (required) |
| `--csv` | Path to the CSV input file. | – (required) |
| `--config` | Path to the YAML configuration file. | – (required) |
| `--skip-groups` | Skip attaching variables to groups. | `False` |
| `--create-groups` | Auto-create groups listed in `available_groups` if they don't exist. | `False` |
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