"""Generic CSV file reader."""

import pandas as pd


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
    return pd.read_csv(csv_path, encoding='utf-8')
