"""Create variables in OpenSILEX."""

from dataclasses import dataclass
from typing import Any

import pandas as pd
from opensilexClientToolsPython import VariableCreationDTO, VariablesApi

from .ctx import VariablesContext


@dataclass
class VariableData:
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
        print([self.name, self.entity, self.characteristic,
                    self.method, self.unit, self.datatype])
        return all([self.name, self.entity, self.characteristic,
                    self.method, self.unit, self.datatype])

    def to_dto(self) -> VariableCreationDTO:
        dto = VariableCreationDTO(
            name=self.name, entity=self.entity,
            characteristic=self.characteristic,
            method=self.method, unit=self.unit,
            datatype=self.datatype
        )
        if self.alternative_name and pd.notna(self.alternative_name) and str(self.alternative_name).strip():
            dto.alternative_name = str(self.alternative_name)
        if self.description and pd.notna(self.description) and str(self.description).strip():
            dto.description = str(self.description)
        if self.time_interval and pd.notna(self.time_interval) and str(self.time_interval).strip():
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
            print(f"  ✓ Variable created: {var_data.name} → {uri}")
            return str(uri)
    except Exception as e:
        print(f"  ✗ Error creating variable '{var_data.name}': {e}")
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
