"""Generic YAML file reader."""

from typing import Any

import yaml

from .._logging import get_logger

logger = get_logger(__name__)


def read_yaml(yaml_path: str) -> dict[str, Any]:
    """Load YAML configuration file.
    Generic function to read any YAML file.
    Args:
        yaml_path: Path to the YAML file
    Returns:
        Dictionary with YAML content
    Example:
        >>> config = read_yaml("config.yaml")
        >>> print(config.keys())
    """
    try:
        with open(yaml_path, encoding="utf-8") as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error("Failed to read YAML %s: %s", yaml_path, e)
        raise
