 
"""Manage variable groups in OpenSILEX."""

from opensilexClientToolsPython import VariablesApi, VariablesGroupCreationDTO


def find_or_create_group(client, name: str, description: str = "") -> str | None:
    """Find a group by name or create it if it doesn't exist.

    Args:
        client: OpenSILEX client instance
        name: Name of the group to find or create
        description: Optional description for the group

    Returns:
        The URI of the group, or None if creation failed.
    """
    variables_api = VariablesApi(client)

    try:
        # Search for existing group by name
        groups = variables_api.search_variables_groups()
        if groups:
            for group in groups:
                group_data = group if isinstance(group, dict) else group.to_dict()
                if group_data.get("name") == name:
                    return group_data.get("uri")

        # Group not found, ask user for confirmation to create it
        confirm = input(f"Group '{name}' not found. Create it? (y/n): ").strip().lower()
        if confirm != 'y':
            print(f"  ℹ Group creation cancelled for '{name}'.")
            return None

        group_dto = VariablesGroupCreationDTO(name=name, description=description, variables=[])
        response = variables_api.create_variables_group(body=group_dto)

        if response and hasattr(response, "uri"):
            return response.uri
        elif isinstance(response, dict):
            return response.get("uri")

    except Exception as e:
        print(f"  ⚠️  Error managing group '{name}': {e}")

    return None
