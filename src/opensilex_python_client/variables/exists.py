"""Check if variable exists in OpenSILEX."""


from opensilexClientToolsPython import VariablesApi


def exists(client, name: str = None, uri: str = None) -> str | None:
    """Check if a variable exists by name or URI.

    Args:
        client: OpenSILEX client instance
        name: Variable name to search for
        uri: Variable URI to search for

    Returns:
        Variable URI if found, None otherwise
    """
    try:
        variables_api = VariablesApi(client)

        if uri:
            # Search by URI
            response = variables_api.get_variable(uri)
            if response:
                print(f"  ✓ Variable exists: {uri}")
                return uri

        if name:
            # Search by name
            response = variables_api.search_variables(name=name)
            if response and isinstance(response, dict) and "result" in response:
                for var in response["result"]:
                    var_name = var.get("name") if isinstance(var, dict) else getattr(var, "name", None)
                    if var_name == name:
                        var_uri = var.get("uri") if isinstance(var, dict) else getattr(var, "uri", None)
                        print(f"  ✓ Variable exists: {name} → {var_uri}")
                        return var_uri

    except Exception as e:
        print(f"  ✗ Error searching variable: {e}")

    return None
