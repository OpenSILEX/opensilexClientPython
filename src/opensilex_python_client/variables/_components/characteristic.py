"""Characteristic creation (internal use)."""

from typing import Optional
from opensilexClientToolsPython import VariablesApi, CharacteristicCreationDTO


def find_or_create_characteristic(client, name: str, description: str = "") -> Optional[str]:
    """Find or create a characteristic."""
    try:
        variables_api = VariablesApi(client)
        response = variables_api.search_characteristics(name=name)

        if response and isinstance(response, dict) and "result" in response:
            for char in response["result"]:
                char_name = char.get("name") if isinstance(char, dict) else getattr(char, "name", None)
                if char_name == name:
                    uri = char.get("uri") if isinstance(char, dict) else getattr(char, "uri", None)
                    print(f"  ✓ Characteristic exists: {name}")
                    return uri

        dto = CharacteristicCreationDTO(name=name, description=description)
        response = variables_api.create_characteristic(body=dto)

        if response:
            uri = response["result"] if isinstance(response, dict) else response
            print(f"  ✓ Characteristic created: {name}")
            return str(uri)
    except Exception as e:
        print(f"  ✗ Error with characteristic '{name}': {e}")
    return None
