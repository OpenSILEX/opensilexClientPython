"""Method creation (internal use)."""

from typing import Optional
from opensilexClientToolsPython import VariablesApi, MethodCreationDTO


def find_or_create_method(client, name: str, description: str = "") -> Optional[str]:
    """Find or create a method."""
    try:
        variables_api = VariablesApi(client)
        response = variables_api.search_methods(name=name)

        if response and isinstance(response, dict) and "result" in response:
            for method in response["result"]:
                method_name = method.get("name") if isinstance(method, dict) else getattr(method, "name", None)
                if method_name == name:
                    uri = method.get("uri") if isinstance(method, dict) else getattr(method, "uri", None)
                    print(f"  ✓ Method exists: {name}")
                    return uri

        dto = MethodCreationDTO(name=name, description=description)
        response = variables_api.create_method(body=dto)

        if response:
            uri = response["result"] if isinstance(response, dict) else response
            print(f"  ✓ Method created: {name}")
            return str(uri)
    except Exception as e:
        print(f"  ✗ Error with method '{name}': {e}")
    return None
