"""Characteristic creation (internal use)."""

from opensilexClientToolsPython import CharacteristicCreationDTO, VariablesApi


def find_or_create_characteristic(client, uri: str | None, name: str, description: str = "") -> str | None:
    """Find or create a characteristic."""
    try:
        variables_api = VariablesApi(client)

        if uri:
            response = variables_api.get_characteristic(uri)
            if response:
                res_uri = response.get("uri") if isinstance(response, dict) else getattr(response, "uri", uri)
                print(f"  ✓ Characteristic exists (by URI): {res_uri}")
                return res_uri

        response = variables_api.search_characteristics(name=name)

        if response and isinstance(response, dict) and "result" in response:
            for char in response["result"]:
                char_name = char.get("name") if isinstance(char, dict) else getattr(char, "name", None)
                if char_name == name:
                    char_uri = char.get("uri") if isinstance(char, dict) else getattr(char, "uri", None)
                    print(f"  ✓ Characteristic exists: {name}")
                    return char_uri

        dto = CharacteristicCreationDTO(name=name, description=description)
        response = variables_api.create_characteristic(body=dto)

        if response:
            res_uri = response["result"] if isinstance(response, dict) else response
            print(f"  ✓ Characteristic created: {name}")
            return str(res_uri)
    except Exception as e:
        print(f"  ✗ Error with characteristic '{name}': {e}")
    return None