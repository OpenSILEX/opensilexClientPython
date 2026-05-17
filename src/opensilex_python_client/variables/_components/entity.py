"""Entity creation (internal use)."""

from typing import Optional
from opensilexClientToolsPython import VariablesApi, EntityCreationDTO


def find_or_create_entity(client, name: str, description: str = "") -> Optional[str]:
    """Find or create an entity."""
    try:
        variables_api = VariablesApi(client)
        response = variables_api.search_entities(name=name)

        if response and isinstance(response, dict) and "result" in response:
            for entity in response["result"]:
                entity_name = entity.get("name") if isinstance(entity, dict) else getattr(entity, "name", None)
                if entity_name == name:
                    uri = entity.get("uri") if isinstance(entity, dict) else getattr(entity, "uri", None)
                    print(f"  ✓ Entity exists: {name}")
                    return uri

        dto = EntityCreationDTO(name=name, description=description)
        response = variables_api.create_entity(body=dto)

        if response:
            uri = response["result"] if isinstance(response, dict) else response
            print(f"  ✓ Entity created: {name}")
            return str(uri)
    except Exception as e:
        print(f"  ✗ Error with entity '{name}': {e}")
    return None
