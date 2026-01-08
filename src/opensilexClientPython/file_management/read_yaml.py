"""Generic YAML file reader."""

import yaml
from typing import Dict, Any


def read_yaml(yaml_path: str) -> Dict[str, Any]:
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
    with open(yaml_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)