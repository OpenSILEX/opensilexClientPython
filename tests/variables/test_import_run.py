"""Integration test for the import_variables_from_csv.run function."""

from unittest.mock import patch

import pandas as pd

from opensilex_python_client.variables.import_variables_from_csv import run


def test_run_import_success(tmp_path, mock_client, default_config):
    # Prepare CSV
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

    # Prepare YAML
    yaml_file = tmp_path / "test_config.yaml"
    import yaml
    with open(yaml_file, "w") as f:
        yaml.dump(default_config, f)

    # Mock internal components to avoid deep API mocking
    with patch("opensilex_python_client.variables.import_variables_from_csv.find_or_create_entity") as mock_ent, \
         patch("opensilex_python_client.variables.import_variables_from_csv.find_or_create_characteristic") as mock_char, \
         patch("opensilex_python_client.variables.import_variables_from_csv.find_or_create_method") as mock_met, \
         patch("opensilex_python_client.variables.import_variables_from_csv.find_or_create_unit") as mock_unit, \
         patch("opensilex_python_client.variables.import_variables_from_csv.exists") as mock_exists, \
         patch("opensilex_python_client.variables.import_variables_from_csv.create_variable") as mock_create:

        mock_ent.return_value = "http://test/entity"
        mock_char.return_value = "http://test/char"
        mock_met.return_value = "http://test/method"
        mock_unit.return_value = "http://test/unit"
        mock_exists.return_value = None # force creation
        mock_create.return_value = "http://test/var/1"

        result = run(mock_client, str(csv_file), str(yaml_file))

        # Check if grouped_variables is returned
        assert isinstance(result, dict)
        # Phenotyping group should have the variable
        pheno_uri = default_config["groups"]["available_groups"]["Phenotyping"]
        assert pheno_uri in result
        assert result[pheno_uri] == ["http://test/var/1"]

        # Check if enriched CSV was created
        enriched_csv = str(csv_file).replace(".csv", "_with_uris.csv")
        assert pd.read_csv(enriched_csv).shape[0] == 1


def test_run_import_column_error(tmp_path, mock_client):
    csv_file = tmp_path / "test_err.csv"
    pd.DataFrame({"wrong": [1]}).to_csv(csv_file, index=False)

    yaml_file = tmp_path / "test_err.yaml"
    with open(yaml_file, "w") as f:
        f.write("csv: {column_mappings: {entity_name: 'missing'}}")

    with patch("opensilex_python_client.variables.import_variables_from_csv.sys.exit") as mock_exit:
        run(mock_client, str(csv_file), str(yaml_file))
        mock_exit.assert_called_once_with(1)
