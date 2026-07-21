"""Manage variable groups in OpenSILEX."""

from opensilexClientToolsPython import VariablesApi, VariablesGroupCreationDTO, VariablesGroupUpdateDTO, OntologyApi
import ast


def _dbg(debug, msg):
    if debug:
        print(f"  [DEBUG] {msg}")


def create_group(uri: str, name: str, variables_api: VariablesApi, description: str = "", debug: bool = False):
    _dbg(debug, f"Creating group(uri={uri!r},name={name!r}, description={description!r})")
    confirm = input(f"Group '{name}' not found. Create it? (y/n): ").strip().lower()
    if confirm != "y":
        print(f"  ℹ Group creation cancelled for '{name}'.")
        return None

    group_dto = VariablesGroupCreationDTO(uri=uri, name=name, description=description, variables=[])
    response = variables_api.create_variables_group(body=group_dto)
    _dbg(debug, f"  response: {response}")

    if response and hasattr(response, "uri"):
        print(f"  ℹ Group  '{name}' created'.")

        return response.uri
    elif isinstance(response, dict):
        print(f"  ℹ Group  '{name}' created'.")

        return response.get("uri")


def find_or_create_group(client, uri: str | None, name: str, description: str = "", debug: bool = False) -> str | None:
    """Find a group by URI or name, or create it if it doesn't exist.

    Args:
        client: OpenSILEX client instance
        uri: Optional URI of the group to look up
        name: Name of the group to find or create
        description: Optional description for the group
        debug: If True, print API call details

    Returns:
        The URI of the group, or None if creation failed.
    """
    variables_api = VariablesApi(client)

    try:
        if uri:
            _dbg(debug, f"get_variables_group(uri={uri!r})")
            response = variables_api.get_variables_group(uri)
            _dbg(debug, f"  response: {response}")
            if response:
                group_data = response if isinstance(response, dict) else response.to_dict()
                return group_data["result"].uri
    except Exception as e:
        print(e)
        try:
            return create_group(uri=uri, name=name, description=description, variables_api=variables_api, debug=debug)
        except Exception as e:
            print(f"  ⚠️  Error managing group '{name}': {e}")
            if debug:
                import traceback

                traceback.print_exc()
        return None
    try:
        _dbg(debug, "search_variables_groups()")
        groups = variables_api.search_variables_groups(name=name)
        _dbg(debug, f"  response: {groups}")
        if groups:
            for group in groups:
                group_data = group if isinstance(group, dict) else group.to_dict()
                return group_data.get("uri")
    except Exception as e:
        print(f"  ⚠️  Error managing group '{name}': {e}")
        if debug:
            import traceback

            traceback.print_exc()

    try:
        return create_group(uri=uri, name=name, description=description, debug=debug)
    except Exception as e:
        print(f"  ⚠️  Error managing group '{name}': {e}")
        if debug:
            import traceback

            traceback.print_exc()
    return None


def attach_variables(client, grouped_variables: dict[str, list[str]], config_path: str) -> None:
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
    ontology_api = OntologyApi(client)
    namespaces = {}
    try:
        namespaces_response = ontology_api.get_name_space()
        namespaces =  ast.literal_eval(namespaces_response["result"])
    except Exception as e :
        print(f"  ✗ Can't find namespace {e}")

    for group_uri, new_variables in grouped_variables.items():
        try:
            # Get current group details
            response = variables_api.get_variables_group(group_uri)

            if not response:
                print(f"  ✗ Cannot retrieve group: {group_uri}")
                continue

            group_data = response["result"] if isinstance(response, dict) else response.to_dict()

            # Extract group name (with fallback if None)
            group_name = group_data.name or group_uri.split("/")[-1]

            # Extract existing variables
            existing_vars = []
            vars_data = group_data.variables or []
            for var in vars_data:
                if isinstance(var, str):
                    existing_vars.append(var)
                elif isinstance(var, dict):
                    existing_vars.append(var.get("uri", ""))
                elif hasattr(var, "uri"):
                    existing_vars.append(var.uri)

            # Replace namespaces in new variables if they match known namespace URIs
            final_new_vars = []
            for v_uri in new_variables:
                replaced = False
                for ns_key, ns_uri in namespaces.items():
                    if ns_key in v_uri:
                        print(ns_uri, "in", v_uri)
                        final_new_vars.append(v_uri.replace(ns_key + ":", ns_uri ))
                        replaced = True
                        break
                if not replaced:
                    final_new_vars.append(v_uri)

            # Deduplicate: merge existing + expanded new
            unique_new_vars = [v for v in final_new_vars if v not in existing_vars]
            all_vars = existing_vars + unique_new_vars

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

    print("\n✅ STEP 3 COMPLETE")
