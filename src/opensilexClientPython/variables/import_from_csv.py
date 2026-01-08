"""Import variables from CSV file - AUTO-GENERATION from names only."""

import pandas as pd
from typing import Dict, List
from ..file_management.read_csv import read_csv
from ..file_management.read_yaml import read_yaml
from ._components.entity import find_or_create_entity
from ._components.characteristic import find_or_create_characteristic
from ._components.method import find_or_create_method
from ._components.unit import find_or_create_unit
from .create import create_variable
from .exists import exists
from .groups.find import find_target_groups


def _clean_uri(uri_value):
    """Clean URI to ensure it's a simple string, not a list."""
    if uri_value is None or pd.isna(uri_value):
        return None
    
    # Convert to string
    uri_str = str(uri_value).strip()
    
    # If it's empty or 'None'
    if not uri_str or uri_str == 'None' or uri_str == 'nan':
        return None
    
    # If it looks like a Python list ['...'], extract the content
    if uri_str.startswith('['):
        uri_str = uri_str.strip('[]').strip("'").strip('"').strip()
    
    return uri_str if uri_str else None


def run(client, csv_path: str, config_path: str) -> Dict[str, List[str]]:
    """Import variables from CSV file.
    
    Creates ALL components and variables automatically from NAMES only.
    """
    print("\n📄 Loading CSV and configuration...")
    df = read_csv(csv_path)
    config = read_yaml(config_path)
    print(f"✓ Loaded {len(df)} rows from CSV")
    
    # STEP 1: Create components FROM NAMES ONLY
    print("\n" + "="*80)
    print("STEP 1: CREATING COMPONENTS (AUTO-GENERATION FROM NAMES)")
    print("="*80)
    
    df_enriched = df.copy()
    df_enriched['Generated_Entity_uri'] = None
    df_enriched['Generated_Characteristic_uri'] = None
    df_enriched['Generated_Method_uri'] = None
    df_enriched['Generated_Unit_uri'] = None
    
    for idx, row in df.iterrows():
        # Entity - ALWAYS create from name
        entity_name = row.get('Entity_name')
        if pd.notna(entity_name):
            uri = find_or_create_entity(client, str(entity_name), '')
            df_enriched.at[idx, 'Generated_Entity_uri'] = _clean_uri(uri)  # ← Clean!
        
        # Characteristic
        char_name = row.get('Characteristic_name')
        if pd.notna(char_name):
            uri = find_or_create_characteristic(client, str(char_name), '')
            df_enriched.at[idx, 'Generated_Characteristic_uri'] = _clean_uri(uri)  # ← Clean!
        
        # Method
        method_name = row.get('Method_name')
        if pd.notna(method_name):
            uri = find_or_create_method(client, str(method_name), '')
            df_enriched.at[idx, 'Generated_Method_uri'] = _clean_uri(uri)  # ← Clean!
        
        # Unit
        unit_name = row.get('Unit_name')
        if pd.notna(unit_name):
            uri = find_or_create_unit(client, str(unit_name))
            df_enriched.at[idx, 'Generated_Unit_uri'] = _clean_uri(uri)  # ← Clean!
    
    # Save enriched CSV
    output_path = csv_path.replace('.csv', '_with_uris.csv')
    df_enriched.to_csv(output_path, index=False, encoding='utf-8')
    print(f"\n✓ Enriched CSV saved: {output_path}")
    
    # STEP 2: Create variables
    print("\n" + "="*80)
    print("STEP 2: CREATING VARIABLES")
    print("="*80)
    
    grouped_variables = {}
    created = 0
    existing = 0
    failed = 0
    
    for idx, row in df_enriched.iterrows():
        # Get and CLEAN URIs
        entity_uri = _clean_uri(df_enriched.at[idx, 'Generated_Entity_uri'])
        char_uri = _clean_uri(df_enriched.at[idx, 'Generated_Characteristic_uri'])
        method_uri = _clean_uri(df_enriched.at[idx, 'Generated_Method_uri'])
        unit_uri = _clean_uri(df_enriched.at[idx, 'Generated_Unit_uri'])
        
        # Prepare variable data
        var_data = {
            'name': row.get('Variable_name'),
            'alternative_name': row.get('Variable_alternative_name', ''),
            'description': row.get('Variable_description', ''),
            'entity': entity_uri,
            'characteristic': char_uri,
            'method': method_uri,
            'unit': unit_uri,
            'datatype': row.get('Datatype_uri'),
            'time_interval': row.get('Time_interval', '')
        }
        
        # Check required fields
        if not all([var_data['name'], var_data['entity'], var_data['characteristic'], 
                    var_data['method'], var_data['unit'], var_data['datatype']]):
            print(f"  ✗ Missing required field for variable: {var_data.get('name', 'UNKNOWN')}")
            failed += 1
            continue
        
        # Check if variable exists
        existing_uri = exists(client, name=var_data['name'])
        if existing_uri:
            variable_uri = existing_uri
            existing += 1
        else:
            # Create new variable
            variable_uri = create_variable(client, var_data)
            if variable_uri:
                created += 1
            else:
                failed += 1
                continue
        
        # Determine target groups
        target_groups = find_target_groups(row, config)
        
        # Add variable to grouped_variables dict
        for group_uri in target_groups:
            if group_uri not in grouped_variables:
                grouped_variables[group_uri] = []
            grouped_variables[group_uri].append(variable_uri)
    
    print(f"\n📊 STEP 2 SUMMARY:")
    print(f"   Created: {created}, Existing: {existing}, Failed: {failed}")
    
    return grouped_variables
