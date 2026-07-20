"""Tests for variable existence checks."""

from unittest.mock import patch

from opensilex_python_client.variables.exists import exists


def test_exists_by_uri(mock_client):
    with patch("opensilex_python_client.variables.exists.VariablesApi") as MockVariablesApi:
        mock_api = MockVariablesApi.return_value
        uri = "http://test/var/1"
        mock_api.get_variable.return_value = {"uri": uri, "name": "Var1"}

        result = exists(mock_client, uri=uri)
        assert result == uri
        mock_api.get_variable.assert_called_once_with(uri)


def test_exists_by_uri_not_found(mock_client):
    with patch("opensilex_python_client.variables.exists.VariablesApi") as MockVariablesApi:
        mock_api = MockVariablesApi.return_value
        mock_api.get_variable.return_value = None

        result = exists(mock_client, uri="http://test/notfound")
        assert result is None


def test_exists_by_name_found(mock_client):
    with patch("opensilex_python_client.variables.exists.VariablesApi") as MockVariablesApi:
        mock_api = MockVariablesApi.return_value
        name = "MyVar"
        uri = "http://test/var/myvar"
        # Simulate response as a dict with 'result' list
        mock_api.search_variables.return_value = {
            "result": [{"name": name, "uri": uri}]
        }

        result = exists(mock_client, name=name)
        assert result == uri
        mock_api.search_variables.assert_called_once_with(name=name)


def test_exists_by_name_not_found(mock_client):
    with patch("opensilex_python_client.variables.exists.VariablesApi") as MockVariablesApi:
        mock_api = MockVariablesApi.return_value
        mock_api.search_variables.return_value = {"result": []}

        result = exists(mock_client, name="UnknownVar")
        assert result is None


def test_exists_api_error(mock_client):
    with patch("opensilex_python_client.variables.exists.VariablesApi") as MockVariablesApi:
        mock_api = MockVariablesApi.return_value
        mock_api.get_variable.side_effect = Exception("API Error")

        result = exists(mock_client, uri="http://test/error")
        assert result is None
