"""Method creation (internal use)."""

from opensilexClientToolsPython import MethodCreationDTO, VariablesApi


def find_or_create_method(client, uri: str | None, name: str, description: str = "") -> str | None:
    """Find or create a method."""
    try:
        variables_api = VariablesApi(client)

        if uri:
            response = variables_api.get_method(uri)
            if response:
                res_uri = response.get("uri") if isinstance(response, dict) else getattr(response, "uri", uri)
                print(f"  ✓ Method exists (by URI): {res_uri}")
                return res_uri

        response = variables_api.search_methods(name=name)

        if response and isinstance(response, dict) and "result" in response:
            for method in response["result"]:
                method_name = method.get("name") if isinstance(method, dict) else getattr(method, "name", None)
                if method_name == name:
                    res_uri = method.get("uri") if isinstance(method, dict) else getattr(method, "uri", None)
                    print(f"  ✓ Method exists: {name}")
                    return res_uri

        dto = MethodCreationDTO(name=name, description=description)
        response = variables_api.create_method(body=dto)

        if response:
            res_uri = response["result"] if isinstance(response, dict) else response
            print(f"  ✓ Method created: {name}")
            return str(res_uri)
    except Exception as e:
        print(f"  ✗ Error with method '{name}': {e}")
    return None