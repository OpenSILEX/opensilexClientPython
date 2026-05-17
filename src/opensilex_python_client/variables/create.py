"""Create variables in OpenSILEX."""

from typing import Dict, Any, Optional
import pandas as pd
from opensilexClientToolsPython import VariablesApi, VariableCreationDTO


def create_variable(client, var_data: Dict[str, Any]) -> Optional[str]:
    """Create a variable in OpenSILEX.
    Args:
        client: OpenSILEX client instance
        var_data: Dictionary containing variable data
    Returns:
        Variable URI if successful, None otherwise
    """
    try:
        variables_api = VariablesApi(client)
        dto = VariableCreationDTO(
            name=var_data["name"],
            entity=var_data["entity"],
            characteristic=var_data["characteristic"],
            method=var_data["method"],
            unit=var_data["unit"],
            datatype=var_data["datatype"],
        )
        # Ajouter les champs optionnels seulement s'ils sont valides (pas NaN, pas vide)
        alt_name = var_data.get("alternative_name")
        if alt_name and pd.notna(alt_name) and str(alt_name).strip():
            dto.alternative_name = str(alt_name)
        desc = var_data.get("description")
        if desc and pd.notna(desc) and str(desc).strip():
            dto.description = str(desc)
        time_int = var_data.get("time_interval")
        if time_int and pd.notna(time_int) and str(time_int).strip():
            dto.time_interval = str(time_int)
        response = variables_api.create_variable(body=dto)
        if response:
            uri = response["result"] if isinstance(response, dict) else response
            if isinstance(uri, list) and uri:
                uri = uri[0]
            print(f"  ✓ Variable created: {var_data['name']} → {uri}")
            return str(uri)
    except Exception as e:
        print(f"  ✗ Error creating variable '{var_data['name']}': {e}")
    return None
