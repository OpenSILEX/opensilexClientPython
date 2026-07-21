"""Integration test for the import_variables_from_csv.run function."""

import sys
from unittest.mock import patch

import pandas as pd
import pytest

from opensilex_python_client.variables.import_variables_from_csv import run


def _component_side_effect(ctx, component, uri, name, description=""):
    uris = {
        "entity": "http://test/entity",
        "characteristic": "http://test/char",
        "method": "http://test/method",
        "unit": "http://test/unit",
    }
    return uris.get(component, f"http://test/{component}")


def test_run_import_success(tmp_path, mock_client, default_config):
    csv_file = tmp_path / "test_import.csv"
    df = pd.DataFrame([
        {
            "Entity_name": "Plant",
            "Characteristic_name": "Height",
            "Method_name": "Manual",
            "Unit_name": "Centimeter",
            "Variable_name": "Plant_Height_cm",
            "Datatype_uri": "http://www.w3.org/2001/XMLSchema#decimal",
            "Phenotyping": "Phenotyping",
        }
    ])
    df.to_csv(csv_file, index=False, encoding="utf-8")

    yaml_file = tmp_path / "test_config.yaml"
    import yaml
    with open(yaml_file, "w") as f:
        yaml.dump(default_config, f)

    with patch("opensilex_python_client.variables.import_variables_from_csv.find_or_create_component") as mock_comp, \
         patch("opensilex_python_client.variables.import_variables_from_csv.exists_variable_ctx") as mock_exists, \
         patch("opensilex_python_client.variables.import_variables_from_csv.create_variable_ctx") as mock_create, \
         patch("rich.console.Console.input", return_value="y"):

        mock_comp.side_effect = _component_side_effect
        mock_exists.return_value = None
        mock_create.return_value = "http://test/var/1"

        result = run(mock_client, str(csv_file), str(yaml_file))

        assert isinstance(result, dict)
        pheno_uri = default_config["groups"]["available_groups"]["Phenotyping"]
        assert pheno_uri in result
        assert result[pheno_uri] == ["http://test/var/1"]

        # Check 4 component calls (one per type)
        assert mock_comp.call_count == 4

        enriched_csv = str(csv_file).replace(".csv", "_enriched.csv")
        df_enriched = pd.read_csv(enriched_csv)
        assert df_enriched.shape[0] == 1
        assert "Final_Variable_URI" in df_enriched.columns
        assert df_enriched.at[0, "Final_Variable_URI"] == "http://test/var/1"


def test_run_import_column_error(tmp_path, mock_client, caplog):
    csv_file = tmp_path / "test_err.csv"
    pd.DataFrame({"wrong": [1]}).to_csv(csv_file, index=False)

    yaml_file = tmp_path / "test_err.yaml"
    with open(yaml_file, "w") as f:
        f.write("csv: {column_mappings: {entity_name: 'missing'}}")

    with pytest.raises(SystemExit):
        run(mock_client, str(csv_file), str(yaml_file))