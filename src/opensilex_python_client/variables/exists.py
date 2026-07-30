"""Check if variable exists in OpenSILEX."""

from typing import Any

from opensilexClientToolsPython import VariablesApi
from opensilexClientToolsPython.rest import ApiURINotFoundException

from .._logging import get_logger
from .ctx import VariablesContext

logger = get_logger(__name__)


def exists_variable_ctx(ctx: VariablesContext, name: str | None = None, uri: str | None = None) -> str | None:
    """Check if variable exists using context.

    Args:
        ctx: Variables context
        name: Variable name to search for
        uri: Variable URI to lookup

    Returns:
        Variable URI if found, None otherwise
    """
    logger.info("Checking variable existence: name=%s, uri=%s", name, uri)
    try:
        api = VariablesApi(ctx.client)
        if uri:
            ctx.debug_log(f"get_variable(uri={uri!r})")
            try:
                response = api.get_variable(uri)
                ctx.debug_log(f"  response: {response}")
                if response:
                    logger.info("Variable exists: %s", uri)
                    return uri
            except ApiURINotFoundException:
                logger.debug("Variable not found by URI: %s", uri)

        if name:
            ctx.debug_log(f"search_variables(name={name!r})")
            response = api.search_variables(name=name)
            ctx.debug_log(f"  response: {response}")
            if response and isinstance(response, dict) and "result" in response:
                for var in response["result"]:
                    var_name = var.get("name") if isinstance(var, dict) else getattr(var, "name", None)
                    if var_name == name:
                        var_uri = var.get("uri") if isinstance(var, dict) else getattr(var, "uri", None)
                        logger.info("Variable exists: %s -> %s", name, var_uri)
                        return var_uri
    except Exception as e:
        logger.error("Error searching variable: %s", e)
        if ctx.debug:
            import traceback

            traceback.print_exc()
    return None


def exists(client: Any, name: str | None = None, uri: str | None = None, debug: bool = False) -> str | None:
    """Check if a variable exists by name or URI.

    Backward-compatible public API.

    Args:
        client: OpenSILEX client instance
        name: Variable name to search for
        uri: Variable URI to lookup
        debug: If True, print API call details

    Returns:
        Variable URI if found, None otherwise
    """
    ctx = VariablesContext(client=client, debug=debug)
    return exists_variable_ctx(ctx, name=name, uri=uri)
