"""Manage variable groups in OpenSILEX."""

import ast

from opensilexClientToolsPython import OntologyApi, VariablesApi, VariablesGroupCreationDTO, VariablesGroupUpdateDTO

from ..._logging import get_logger

logger = get_logger(__name__)


def _dbg(debug, msg):
    if debug:
        logger.debug(msg)


def expand_namespaces(var_uris: list[str], namespaces: dict[str, str]) -> list[str]:
    """Replace namespace prefixes in variable URIs with full namespace URIs.

    Args:
        var_uris: List of variable URIs that may contain namespace prefixes (e.g. "sixtine:xxx")
        namespaces: Dictionary mapping namespace prefixes to their full URIs

    Returns:
        List of variable URIs with all known namespace prefixes expanded to full URIs.
    """
    expanded = []
    for v_uri in var_uris:
        replaced = False
        for ns_key, ns_uri in namespaces.items():
            if ns_key in v_uri:
                expanded.append(v_uri.replace(ns_key + ":", ns_uri))
                replaced = True
                break
        if not replaced:
            expanded.append(v_uri)
    return expanded


def create_group(uri: str, name: str, variables_api: VariablesApi, description: str = "", debug: bool = False):
    logger.info("Creating group: uri=%s, name=%s", uri, name)
    _dbg(debug, f"Creating group(uri={uri!r},name={name!r}, description={description!r})")
    confirm = input(f"Group '{name}' not found. Create it? (y/n): ").strip().lower()
    if confirm != "y":
        logger.info("Group creation cancelled for '%s'", name)
        return None

    group_dto = VariablesGroupCreationDTO(uri=uri, name=name, description=description, variables=[])
    response = variables_api.create_variables_group(body=group_dto)
    _dbg(debug, f"  response: {response}")

    if response and hasattr(response, "uri"):
        logger.info("Group '%s' created", name)
        return response.uri
    elif isinstance(response, dict):
        logger.info("Group '%s' created", name)
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
        logger.error("Error in %s: %s", find_or_create_group.__name__, e)
        try:
            return create_group(uri=uri, name=name, description=description, variables_api=variables_api, debug=debug)
        except Exception as e:
            logger.warning("Error managing group '%s': %s", name, e)
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
        logger.error("Error in %s: %s", find_or_create_group.__name__, e)
        logger.warning("Error managing group '%s': %s", name, e)
        if debug:
            import traceback

            traceback.print_exc()

    try:
        return create_group(uri=uri, name=name, description=description, debug=debug, variables_api=variables_api)
    except Exception as e:
        logger.error("Error in %s: %s", find_or_create_group.__name__, e)
        logger.warning("Error managing group '%s': %s", name, e)
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
    logger.info("Attaching variables to groups")
    logger.info("=" * 80)
    logger.info("STEP 3: ATTACHING TO GROUPS")
    logger.info("=" * 80)

    variables_api = VariablesApi(client)
    ontology_api = OntologyApi(client)
    namespaces = {}
    try:
        namespaces_response = ontology_api.get_name_space()
        namespaces = ast.literal_eval(namespaces_response["result"])
    except Exception as e:
        logger.error("Can't find namespace: %s", e)

    for group_uri, new_variables in grouped_variables.items():
        try:
            response = variables_api.get_variables_group(group_uri)

            if not response:
                logger.error("Cannot retrieve group: %s", group_uri)
                continue

            group_data = response["result"] if isinstance(response, dict) else response.to_dict()

            group_name = group_data.name or group_uri.split("/")[-1]

            existing_vars = []
            vars_data = group_data.variables or []
            for var in vars_data:
                if isinstance(var, str):
                    existing_vars.append(var)
                elif isinstance(var, dict):
                    existing_vars.append(var.get("uri", ""))
                elif hasattr(var, "uri"):
                    existing_vars.append(var.uri)

            existing_vars = expand_namespaces(existing_vars, namespaces)
            final_new_vars = expand_namespaces(new_variables, namespaces)

            unique_new_vars = [v for v in final_new_vars if v not in existing_vars]
            all_vars = existing_vars + unique_new_vars

            if not unique_new_vars:
                logger.info("No new variables for group: %s", group_name)
                continue

            update_dto = VariablesGroupUpdateDTO(
                uri=group_uri, name=group_name, description=group_data.description, variables=all_vars
            )

            variables_api.update_variables_group(body=update_dto)
            logger.info("Group updated: %s (%d variables, +%d new)", group_name, len(all_vars), len(unique_new_vars))

        except Exception as e:
            logger.error("Error updating group %s: %s", group_uri, e)

    logger.info("STEP 3 COMPLETE")
