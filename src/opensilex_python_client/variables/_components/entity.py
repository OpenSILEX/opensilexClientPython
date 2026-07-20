"""Entity creation (internal use)."""


from opensilexClientToolsPython import EntityCreationDTO, VariablesApi


def find_or_create_entity(client, uri: str | None, name: str, description: str = "") -> str | None:
    """Find or create an entity."""
    try:
        variables_api = VariablesApi(client)

        if uri:
            response = variables_api.get_entity(uri)
            if response:
                res_uri = response.get("uri") if isinstance(response, dict) else getattr(response, "uri", uri)
                print(f"  ✓ Entity exists (by URI): {res_uri}")
                return res_uri

        response = variables_api.search_entities(name=name)

        if response and isinstance(response, dict) and "result" in response:
            for entity in response["result"]:
                entity_name = entity.get("name") if isinstance(entity, dict) else getattr(entity, "name", None)
                if entity_name == name:
                    res_uri = entity.get("uri") if isinstance(entity, dict) else getattr(entity, "uri", None)
                    print(f"  ✓ Entity exists: {name}")
                    return res_uri

        dto = EntityCreationDTO(name=name, description=description)
        response = variables_api.create_entity(body=dto)

        if response:
            res_uri = response["result"] if isinstance(response, dict) else response
            print(f"  ✓ Entity created: {name}")
            return str(res_uri)
    except Exception as e:
        print(f"  ✗ Error with entity '{name}': {e}")
    return None