"""Generic YAML file reader."""

from typing import Any

import yaml


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
    with open(yaml_path, encoding="utf-8") as f:
        return yaml.safe_load(f)
