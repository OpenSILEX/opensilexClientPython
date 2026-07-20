"""Tests for variable creation."""

from unittest.mock import patch

from opensilex_python_client.variables.create import create_variable


def test_create_variable_success(mock_client):
    with patch("opensilex_python_client.variables.create.VariablesApi") as MockVariablesApi:
        mock_api = MockVariablesApi.return_value
        var_uri = "http://test/var/1"
        mock_api.create_variable.return_value = var_uri

        var_data = {
            "name": "TestVar",
            "entity": "http://test/entity",
            "characteristic": "http://test/char",
            "method": "http://test/method",
            "unit": "http://test/unit",
            "datatype": "http://test/datatype"
        }

        result = create_variable(mock_client, var_data)
        assert result == var_uri
        mock_api.create_variable.assert_called_once()


def test_create_variable_failure(mock_client):
    with patch("opensilex_python_client.variables.create.VariablesApi") as MockVariablesApi:
        mock_api = MockVariablesApi.return_value
        mock_api.create_variable.return_value = None

        var_data = {
            "name": "TestVar",
            "entity": "http://test/entity",
            "characteristic": "http://test/char",
            "method": "http://test/method",
            "unit": "http://test/unit",
            "datatype": "http://test/datatype"
        }

        result = create_variable(mock_client, var_data)
        assert result is None
