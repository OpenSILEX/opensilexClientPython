"""Generic CSV file reader."""

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def read_csv(csv_path: str) -> pd.DataFrame:
    """Load CSV file.
    Generic function to read any CSV file.
    Args:
        csv_path: Path to the CSV file
    Returns:
        DataFrame with CSV data
    Example:
        >>> df = read_csv("data.csv")
        >>> print(len(df))
    """
    try:
        return pd.read_csv(csv_path, encoding="utf-8")
    except Exception as e:
        logger.error("Failed to read CSV %s: %s", csv_path, e)
        raise
