"""Create variables in OpenSILEX."""

from dataclasses import dataclass
from typing import Any

from opensilexClientToolsPython import VariableCreationDTO, VariablesApi

from .._logging import get_logger
from .ctx import VariablesContext

logger = get_logger(__name__)


@dataclass
class VariableData:
    uri: str
    name: str
    entity: str
    characteristic: str
    method: str
    unit: str
    datatype: str
    alternative_name: str = ""
    description: str = ""
    time_interval: str = ""

    @property
    def valid(self) -> bool:
        return all([self.name, self.entity, self.characteristic, self.method, self.unit, self.datatype])

    def to_dto(self) -> VariableCreationDTO:
        dto = VariableCreationDTO(
            uri=self.uri,
            name=self.name,
            entity=self.entity,
            characteristic=self.characteristic,
            method=self.method,
            unit=self.unit,
            datatype=self.datatype,
        )
        if str(self.alternative_name).strip():
            dto.alternative_name = str(self.alternative_name)
        if str(self.description).strip():
            dto.description = str(self.description)
        if str(self.time_interval).strip():
            dto.time_interval = str(self.time_interval)
        return dto


def create_variable_ctx(ctx: VariablesContext, var_data: VariableData) -> str | None:
    """Create a variable using context."""
    try:
        api = VariablesApi(ctx.client)
        dto = var_data.to_dto()
        ctx.debug_log(f"create_variable(name={var_data.name!r}, entity={var_data.entity!r})")
        response = api.create_variable(body=dto)
        ctx.debug_log(f"  response: {response}")
        if response:
            uri = response["result"] if isinstance(response, dict) else response
            if isinstance(uri, list) and uri:
                uri = uri[0]
            logger.info("Variable created: %s -> %s", var_data.name, uri)
            return str(uri)
    except Exception as e:
        logger.error("Error creating variable '%s': %s", var_data.name, e)
        if ctx.debug:
            import traceback

            traceback.print_exc()
    return None


def create_variable(client: Any, var_data: dict[str, Any] | VariableData, debug: bool = False) -> str | None:
    """Create a variable in OpenSILEX.

    Backward-compatible public API.

    Args:
        client: OpenSILEX client instance
        var_data: Dictionary or VariableData containing variable data
        debug: If True, print API call details

    Returns:
        Variable URI if successful, None otherwise
    """
    ctx = VariablesContext(client=client, debug=debug)
    if isinstance(var_data, dict):
        var_data = VariableData(**var_data)
    return create_variable_ctx(ctx, var_data)
