"""Generic component resolver for entity/characteristic/method/unit."""

import traceback
from dataclasses import dataclass
from typing import Any

from opensilexClientToolsPython import (
    CharacteristicCreationDTO,
    EntityCreationDTO,
    MethodCreationDTO,
    UnitCreationDTO,
    VariablesApi,
)

from .._logging import get_logger
from .ctx import VariablesContext

logger = get_logger(__name__)


@dataclass
class ComponentConfig:
    dto_class: type
    get_api: str
    search_api: str
    create_api: str
    plural: str
    label: str


@dataclass
class ComponentResolutionStats:
    created: int = 0
    existing: int = 0
    failed: int = 0


COMPONENTS: dict[str, ComponentConfig] = {
    "entity": ComponentConfig(EntityCreationDTO, "get_entity", "search_entities", "create_entity", "result", "Entity"),
    "characteristic": ComponentConfig(
        CharacteristicCreationDTO,
        "get_characteristic",
        "search_characteristics",
        "create_characteristic",
        "result",
        "Characteristic",
    ),
    "method": ComponentConfig(MethodCreationDTO, "get_method", "search_methods", "create_method", "result", "Method"),
    "unit": ComponentConfig(UnitCreationDTO, "get_unit", "search_units", "create_unit", "result", "Unit"),
}


def _extract_uri(response: Any) -> str | None:
    if isinstance(response, dict):
        return response.get("uri")
    return getattr(response, "uri", response)


def _get_name(item: Any) -> str | None:
    if isinstance(item, dict):
        return item.get("name")
    return getattr(item, "name", None)


def _get_uri(item: Any) -> str | None:
    if isinstance(item, dict):
        return item.get("uri")
    return getattr(item, "uri", None)


def _create(
    uri: str,
    name: str,
    description: str,
    ctx: VariablesContext,
    api: VariablesApi,
    cfg: ComponentConfig,
    stats: ComponentResolutionStats,
) -> tuple[str | None, ComponentResolutionStats]:
    logger.debug("Creating component %s: %s (%s)", cfg.label, name, uri)
    try:
        ctx.debug_log(f"{cfg.create_api}(uri={uri!r}, name={name!r}, description={description!r})")
        dto = cfg.dto_class(uri=uri, name=name, description=description)
        response = getattr(api, cfg.create_api)(body=dto)
        ctx.debug_log(f"  response: {response}")
        if response:
            res_uri = response.get("result", response) if isinstance(response, dict) else response
            logger.debug("Component %s created: %s", cfg.label, name)
            stats.created += 1
            return str(res_uri), stats

        stats.failed += 1
        return None, stats
    except Exception as e:
        logger.error("Error creating %s '%s': %s", cfg.label, name, e)
        if ctx.debug:
            traceback.print_exc()
        stats.failed += 1
        return None, stats


def find_or_create_component(
    ctx: VariablesContext,
    component: str,
    uri: str | None,
    name: str,
    description: str = "",
) -> tuple[str | None, ComponentResolutionStats]:
    """Find or create a component (entity/characteristic/method/unit).

    Strategy: 1. Lookup by URI -> 2. Search by name -> 3. Create.
    
    Returns:
        Tuple of (component_uri, ComponentResolutionStats)
    """

    cfg = COMPONENTS[component]
    api = VariablesApi(ctx.client)
    stats = ComponentResolutionStats()

    if uri:
        try:
            ctx.debug_log(f"{cfg.get_api}(uri={uri!r})")
            response = getattr(api, cfg.get_api)(uri)
            ctx.debug_log(f"  response: {response}")
            if response:
                res_uri = _extract_uri(response["result"])
                logger.debug("Component %s exists (by URI): %s", cfg.label, res_uri)
                stats.existing += 1
                return res_uri, stats
        except Exception:
            if ctx.debug:
                traceback.print_exc()
            return _create(uri=uri, name=name, description=description, api=api, cfg=cfg, ctx=ctx, stats=stats)

    ctx.debug_log(f"{cfg.search_api}(name={name!r})")
    response = getattr(api, cfg.search_api)(name=name)
    ctx.debug_log(f"  response: {response}")
    if response and isinstance(response, dict) and "result" in response:
        for item in response["result"]:
            item_name = _get_name(item)
            if item_name == name:
                res_uri = _get_uri(item)
                logger.debug("Component %s exists: %s", cfg.label, name)
                stats.existing += 1
                return res_uri, stats
    return _create(uri=uri, name=name, description=description, api=api, cfg=cfg, ctx=ctx, stats=stats)