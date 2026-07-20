"""Tests for file management utilities."""

import pandas as pd
import pytest
import yaml

from opensilex_python_client.file_management.read_csv import read_csv
from opensilex_python_client.file_management.read_yaml import read_yaml


def test_read_csv(tmp_path):
    # Create a sample CSV file
    csv_file = tmp_path / "test.csv"
    df_input = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    df_input.to_csv(csv_file, index=False, encoding="utf-8")

    # Test read_csv
    df_output = read_csv(str(csv_file))
    pd.testing.assert_frame_equal(df_input, df_output)


def test_read_csv_not_found():
    with pytest.raises(FileNotFoundError):
        read_csv("non_existent_file.csv")


def test_read_yaml(tmp_path):
    # Create a sample YAML file
    yaml_file = tmp_path / "test.yaml"
    data = {"key": "value", "nested": {"a": 1}}
    with open(yaml_file, "w", encoding="utf-8") as f:
        yaml.dump(data, f)

    # Test read_yaml
    output = read_yaml(str(yaml_file))
    assert output == data


def test_read_yaml_not_found():
    with pytest.raises(FileNotFoundError):
        read_yaml("non_existent_file.yaml")
