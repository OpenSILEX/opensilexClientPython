"""Update and attach variables to groups."""

from typing import Dict, List
from opensilexClientToolsPython import VariablesApi, VariablesGroupUpdateDTO


def attach_to_groups(client, grouped_variables: Dict[str, List[str]], config_path: str) -> None:
    """Attach variables to their target groups in OpenSILEX.

    Args:
        client: OpenSILEX client instance
        grouped_variables: Dictionary mapping group URIs to variable URI lists
        config_path: Path to configuration (for reference)
    """
    print("\n" + "=" * 80)
    print("STEP 3: ATTACHING TO GROUPS")
    print("=" * 80)

    variables_api = VariablesApi(client)

    for group_uri, new_variables in grouped_variables.items():
        try:
            # Get current group details
            response = variables_api.get_variables_group(group_uri)

            if not response:
                print(f"  ✗ Cannot retrieve group: {group_uri}")
                continue

            group_data = response if isinstance(response, dict) else response.to_dict()

            # Extract group name (with fallback if None)
            group_name = group_data.get("name") or group_uri.split("/")[-1]

            # Extract existing variables
            existing_vars = []
            vars_data = group_data.get("variables", [])
            for var in vars_data:
                if isinstance(var, str):
                    existing_vars.append(var)
                elif isinstance(var, dict):
                    existing_vars.append(var.get("uri", ""))
                elif hasattr(var, "uri"):
                    existing_vars.append(var.uri)

            # Deduplicate: merge existing + new
            unique_new_vars = [v for v in new_variables if v not in existing_vars]
            all_vars = list(set(existing_vars + unique_new_vars))

            if not unique_new_vars:
                print(f"  ℹ No new variables for group: {group_name}")
                continue

            # Update group
            update_dto = VariablesGroupUpdateDTO(
                uri=group_uri, name=group_name, description=group_data.get("description", ""), variables=all_vars
            )

            variables_api.update_variables_group(body=update_dto)
            print(f"  ✓ Group updated: {group_name} ({len(all_vars)} variables, +{len(unique_new_vars)} new)")

        except Exception as e:
            print(f"  ✗ Error updating group {group_uri}: {e}")

    print(f"\n✅ STEP 3 COMPLETE")
