"""Tests for variable group management."""

from unittest.mock import MagicMock, patch

from opensilex_python_client.variables.groups.find import find_target_groups
from opensilex_python_client.variables.groups.manage import find_or_create_group
from opensilex_python_client.variables.groups.update import attach_to_groups


def test_find_target_groups(sample_df, default_config):
    row = sample_df.iloc[0]
    groups = find_target_groups(row, default_config, list(sample_df.columns))

    assert "http://opensilex.test/id/variablesGroup/phenotyping" in groups
    assert "http://opensilex.test/id/variablesGroup/environment" in groups
    assert len(groups) == 2


def test_find_or_create_group_exists(mock_client):
    with patch("opensilex_python_client.variables.groups.manage.VariablesApi") as MockVariablesApi:
        mock_api = MockVariablesApi.return_value
        name = "MyGroup"
        uri = "http://test/group/mygroup"
        mock_api.search_variables_groups.return_value = [{"name": name, "uri": uri}]

        result = find_or_create_group(mock_client, None, name)
        assert result == uri


def test_find_or_create_group_creates(mock_client):
    with patch("opensilex_python_client.variables.groups.manage.VariablesApi") as MockVariablesApi, \
         patch("builtins.input", return_value="y"):
        mock_api = MockVariablesApi.return_value
        name = "NewGroup"
        uri = "http://test/group/new"
        mock_api.search_variables_groups.return_value = []

        mock_response = MagicMock()
        mock_response.uri = uri
        mock_api.create_variables_group.return_value = mock_response

        result = find_or_create_group(mock_client, None, name)
        assert result == uri


def test_attach_to_groups(mock_client):
    with patch("opensilex_python_client.variables.groups.update.VariablesApi") as MockVariablesApi:
        mock_api = MockVariablesApi.return_value

        grouped_vars = {
            "http://test/group/1": ["http://test/var/1", "http://test/var/2"]
        }
        config = {"groups": {"group_mapping": {}}}

        mock_group = {
            "name": "Test Group",
            "description": "Test Desc",
            "variables": ["http://test/var/0"]
        }
        mock_api.get_variables_group.return_value = mock_group

        attach_to_groups(mock_client, grouped_vars, config)
        assert mock_api.update_variables_group.called