"""Unit creation (internal use)."""

from typing import Optional
from opensilexClientToolsPython import VariablesApi, UnitCreationDTO


def find_or_create_unit(client, name: str, description: str = "") -> Optional[str]:
    """Find or create a unit."""
    try:
        variables_api = VariablesApi(client)
        response = variables_api.search_units(name=name)

        if response and isinstance(response, dict) and "result" in response:
            for unit in response["result"]:
                unit_name = unit.get("name") if isinstance(unit, dict) else getattr(unit, "name", None)
                if unit_name == name:
                    uri = unit.get("uri") if isinstance(unit, dict) else getattr(unit, "uri", None)
                    print(f"  ✓ Unit exists: {name}")
                    return uri

        dto = UnitCreationDTO(name=name, description=description)
        response = variables_api.create_unit(body=dto)

        if response:
            uri = response["result"] if isinstance(response, dict) else response
            print(f"  ✓ Unit created: {name}")
            return str(uri)
    except Exception as e:
        print(f"  ✗ Error with unit '{name}': {e}")
    return None
