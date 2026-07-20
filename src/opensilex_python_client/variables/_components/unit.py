"""Unit creation (internal use)."""


from opensilexClientToolsPython import UnitCreationDTO, VariablesApi


def find_or_create_unit(client, uri: str | None, name: str, description: str = "") -> str | None:
    """Find or create a unit."""
    try:
        variables_api = VariablesApi(client)

        if uri:
            response = variables_api.get_unit(uri)
            if response:
                res_uri = response.get("uri") if isinstance(response, dict) else getattr(response, "uri", uri)
                print(f"  ✓ Unit exists (by URI): {res_uri}")
                return res_uri

        response = variables_api.search_units(name=name)

        if response and isinstance(response, dict) and "result" in response:
            for unit in response["result"]:
                unit_name = unit.get("name") if isinstance(unit, dict) else getattr(unit, "name", None)
                if unit_name == name:
                    res_uri = unit.get("uri") if isinstance(unit, dict) else getattr(unit, "uri", None)
                    print(f"  ✓ Unit exists: {name}")
                    return res_uri

        dto = UnitCreationDTO(name=name, description=description)
        response = variables_api.create_unit(body=dto)

        if response:
            res_uri = response["result"] if isinstance(response, dict) else response
            print(f"  ✓ Unit created: {name}")
            return str(res_uri)
    except Exception as e:
        print(f"  ✗ Error with unit '{name}': {e}")
    return None