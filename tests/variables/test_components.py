"""Tests for find_or_create_* component functions."""

from unittest.mock import patch

from opensilex_python_client.variables._components.characteristic import find_or_create_characteristic
from opensilex_python_client.variables._components.entity import find_or_create_entity
from opensilex_python_client.variables._components.method import find_or_create_method
from opensilex_python_client.variables._components.unit import find_or_create_unit


def test_find_or_create_unit_exists(mock_client):
    with patch("opensilex_python_client.variables._components.unit.VariablesApi") as MockVariablesApi:
        mock_api = MockVariablesApi.return_value
        name = "Meter"
        uri = "http://test/unit/meter"
        mock_api.search_units.return_value = {"result": [{"name": name, "uri": uri}]}

        result = find_or_create_unit(mock_client, None, name)
        assert result == uri
        mock_api.search_units.assert_called_once_with(name=name)


def test_find_or_create_unit_creates(mock_client):
    with patch("opensilex_python_client.variables._components.unit.VariablesApi") as MockVariablesApi:
        mock_api = MockVariablesApi.return_value
        name = "NewUnit"
        uri = "http://test/unit/newunit"
        mock_api.search_units.return_value = {"result": []}
        mock_api.create_unit.return_value = uri

        result = find_or_create_unit(mock_client, None, name)
        assert result == uri
        mock_api.create_unit.assert_called_once()


def test_find_or_create_entity_exists(mock_client):
    with patch("opensilex_python_client.variables._components.entity.VariablesApi") as MockVariablesApi:
        mock_api = MockVariablesApi.return_value
        name = "Plant"
        uri = "http://test/entity/plant"
        mock_api.search_entities.return_value = {"result": [{"name": name, "uri": uri}]}

        result = find_or_create_entity(mock_client, None, name)
        assert result == uri


def test_find_or_create_entity_creates(mock_client):
    with patch("opensilex_python_client.variables._components.entity.VariablesApi") as MockVariablesApi:
        mock_api = MockVariablesApi.return_value
        name = "NewEntity"
        uri = "http://test/entity/new"
        mock_api.search_entities.return_value = {"result": []}
        mock_api.create_entity.return_value = uri

        result = find_or_create_entity(mock_client, None, name)
        assert result == uri


def test_find_or_create_characteristic_exists(mock_client):
    with patch("opensilex_python_client.variables._components.characteristic.VariablesApi") as MockVariablesApi:
        mock_api = MockVariablesApi.return_value
        name = "Height"
        uri = "http://test/char/height"
        mock_api.search_characteristics.return_value = {"result": [{"name": name, "uri": uri}]}

        result = find_or_create_characteristic(mock_client, None, name)
        assert result == uri


def test_find_or_create_characteristic_creates(mock_client):
    with patch("opensilex_python_client.variables._components.characteristic.VariablesApi") as MockVariablesApi:
        mock_api = MockVariablesApi.return_value
        name = "NewChar"
        uri = "http://test/char/new"
        mock_api.search_characteristics.return_value = {"result": []}
        mock_api.create_characteristic.return_value = uri

        result = find_or_create_characteristic(mock_client, None, name)
        assert result == uri


def test_find_or_create_method_exists(mock_client):
    with patch("opensilex_python_client.variables._components.method.VariablesApi") as MockVariablesApi:
        mock_api = MockVariablesApi.return_value
        name = "Manual"
        uri = "http://test/method/manual"
        mock_api.search_methods.return_value = {"result": [{"name": name, "uri": uri}]}

        result = find_or_create_method(mock_client, None, name)
        assert result == uri


def test_find_or_create_method_creates(mock_client):
    with patch("opensilex_python_client.variables._components.method.VariablesApi") as MockVariablesApi:
        mock_api = MockVariablesApi.return_value
        name = "NewMethod"
        uri = "http://test/method/new"
        mock_api.search_methods.return_value = {"result": []}
        mock_api.create_method.return_value = uri

        result = find_or_create_method(mock_client, None, name)
        assert result == uri