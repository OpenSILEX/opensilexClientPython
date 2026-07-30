"""Tests for find_or_create_component generic resolver."""

from unittest.mock import patch

from opensilex_python_client.variables._component_resolver import (
    ComponentResolutionStats,
    find_or_create_component,
)
from opensilex_python_client.variables.ctx import VariablesContext


def test_find_or_create_unit_exists(mock_client):
    with patch("opensilex_python_client.variables._component_resolver.VariablesApi") as MockVariablesApi:
        mock_api = MockVariablesApi.return_value
        name = "Meter"
        uri = "http://test/unit/meter"
        mock_api.search_units.return_value = {"result": [{"name": name, "uri": uri}]}

        ctx = VariablesContext(client=mock_client)
        result, stats = find_or_create_component(ctx, "unit", None, name)
        assert result == uri
        assert stats.existing == 1
        assert stats.created == 0
        assert stats.failed == 0
        mock_api.search_units.assert_called_once_with(name=name)


def test_find_or_create_unit_creates(mock_client):
    with patch("opensilex_python_client.variables._component_resolver.VariablesApi") as MockVariablesApi:
        mock_api = MockVariablesApi.return_value
        name = "NewUnit"
        uri = "http://test/unit/newunit"
        mock_api.search_units.return_value = {"result": []}
        mock_api.create_unit.return_value = uri

        ctx = VariablesContext(client=mock_client)
        result, stats = find_or_create_component(ctx, "unit", None, name)
        assert result == uri
        assert stats.created == 1
        assert stats.existing == 0
        assert stats.failed == 0
        mock_api.create_unit.assert_called_once()


def test_find_or_create_entity_exists(mock_client):
    with patch("opensilex_python_client.variables._component_resolver.VariablesApi") as MockVariablesApi:
        mock_api = MockVariablesApi.return_value
        name = "Plant"
        uri = "http://test/entity/plant"
        mock_api.search_entities.return_value = {"result": [{"name": name, "uri": uri}]}

        ctx = VariablesContext(client=mock_client)
        result, stats = find_or_create_component(ctx, "entity", None, name)
        assert result == uri
        assert stats.existing == 1


def test_find_or_create_entity_creates(mock_client):
    with patch("opensilex_python_client.variables._component_resolver.VariablesApi") as MockVariablesApi:
        mock_api = MockVariablesApi.return_value
        name = "NewEntity"
        uri = "http://test/entity/new"
        mock_api.search_entities.return_value = {"result": []}
        mock_api.create_entity.return_value = uri

        ctx = VariablesContext(client=mock_client)
        result, stats = find_or_create_component(ctx, "entity", None, name)
        assert result == uri
        assert stats.created == 1


def test_find_or_create_characteristic_exists(mock_client):
    with patch("opensilex_python_client.variables._component_resolver.VariablesApi") as MockVariablesApi:
        mock_api = MockVariablesApi.return_value
        name = "Height"
        uri = "http://test/char/height"
        mock_api.search_characteristics.return_value = {"result": [{"name": name, "uri": uri}]}

        ctx = VariablesContext(client=mock_client)
        result, stats = find_or_create_component(ctx, "characteristic", None, name)
        assert result == uri
        assert stats.existing == 1


def test_find_or_create_characteristic_creates(mock_client):
    with patch("opensilex_python_client.variables._component_resolver.VariablesApi") as MockVariablesApi:
        mock_api = MockVariablesApi.return_value
        name = "NewChar"
        uri = "http://test/char/new"
        mock_api.search_characteristics.return_value = {"result": []}
        mock_api.create_characteristic.return_value = uri

        ctx = VariablesContext(client=mock_client)
        result, stats = find_or_create_component(ctx, "characteristic", None, name)
        assert result == uri
        assert stats.created == 1


def test_find_or_create_method_exists(mock_client):
    with patch("opensilex_python_client.variables._component_resolver.VariablesApi") as MockVariablesApi:
        mock_api = MockVariablesApi.return_value
        name = "Manual"
        uri = "http://test/method/manual"
        mock_api.search_methods.return_value = {"result": [{"name": name, "uri": uri}]}

        ctx = VariablesContext(client=mock_client)
        result, stats = find_or_create_component(ctx, "method", None, name)
        assert result == uri
        assert stats.existing == 1


def test_find_or_create_method_creates(mock_client):
    with patch("opensilex_python_client.variables._component_resolver.VariablesApi") as MockVariablesApi:
        mock_api = MockVariablesApi.return_value
        name = "NewMethod"
        uri = "http://test/method/new"
        mock_api.search_methods.return_value = {"result": []}
        mock_api.create_method.return_value = uri

        ctx = VariablesContext(client=mock_client)
        result, stats = find_or_create_component(ctx, "method", None, name)
        assert result == uri
        assert stats.created == 1


def test_find_or_create_unit_by_uri(mock_client):
    with patch("opensilex_python_client.variables._component_resolver.VariablesApi") as MockVariablesApi:
        mock_api = MockVariablesApi.return_value
        uri = "http://test/unit/meter"
        mock_api.get_unit.return_value = {"result": {"uri": uri, "name": "Meter"}}

        ctx = VariablesContext(client=mock_client)
        result, stats = find_or_create_component(ctx, "unit", uri, "Meter")
        assert result == uri
        assert stats.existing == 1
        assert stats.created == 0
        assert stats.failed == 0
        mock_api.get_unit.assert_called_once_with(uri)
        mock_api.search_units.assert_not_called()
        mock_api.create_unit.assert_not_called()


def test_find_or_create_unit_creation_failure(mock_client):
    with patch("opensilex_python_client.variables._component_resolver.VariablesApi") as MockVariablesApi:
        mock_api = MockVariablesApi.return_value
        name = "FailingUnit"
        uri = "http://test/unit/failing"
        mock_api.get_unit.side_effect = Exception("URI not found")
        mock_api.search_units.return_value = {"result": []}
        mock_api.create_unit.side_effect = Exception("Creation failed")

        ctx = VariablesContext(client=mock_client)
        result, stats = find_or_create_component(ctx, "unit", uri, name)
        assert result is None
        assert stats.failed == 1
        assert stats.created == 0
        assert stats.existing == 0
        mock_api.create_unit.assert_called_once()