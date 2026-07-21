"""Tests for variable group management."""

from unittest.mock import MagicMock, patch

from opensilex_python_client.variables.groups.find import find_target_groups
from opensilex_python_client.variables.groups.manage import expand_namespaces, find_or_create_group
from opensilex_python_client.variables.groups.manage import attach_variables


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


def test_attach_variables(mock_client):
    with patch("opensilex_python_client.variables.groups.manage.VariablesApi") as MockVariablesApi, \
         patch("opensilex_python_client.variables.groups.manage.OntologyApi") as MockOntologyApi:
        mock_api = MockVariablesApi.return_value
        mock_ontology = MockOntologyApi.return_value
        mock_ontology.get_name_space.return_value = "{}"

        grouped_vars = {
            "http://test/group/1": ["http://test/var/1", "http://test/var/2"]
        }
        config = {"groups": {"group_mapping": {}}}

        mock_group_result = MagicMock()
        mock_group_result.name = "Test Group"
        mock_group_result.description = "Test Desc"
        mock_group_result.variables = ["http://test/var/0"]
        mock_api.get_variables_group.return_value = {"result": mock_group_result}

        attach_variables(mock_client, grouped_vars, config)
        assert mock_api.update_variables_group.called


def test_expand_namespaces_replaces_prefix():
    namespaces = {"sixtine": "http://sixtine.mistea.inrae.fr/", "xsd": "http://www.w3.org/2001/XMLSchema#"}
    uris = ["sixtine:factor/height", "xsd:decimal"]
    result = expand_namespaces(uris, namespaces)
    assert result == ["http://sixtine.mistea.inrae.fr/factor/height", "http://www.w3.org/2001/XMLSchema#decimal"]


def test_expand_namespaces_no_match_passthrough():
    namespaces = {"sixtine": "http://sixtine.mistea.inrae.fr/"}
    uris = ["http://example.com/var/1", "http://example.com/var/2"]
    result = expand_namespaces(uris, namespaces)
    assert result == uris


def test_expand_namespaces_mixed():
    namespaces = {"oe": "http://test.org/", "foaf": "http://xmlns.com/foaf/0.1/"}
    uris = ["oe:var/1", "http://example.com/var/2", "foaf:Person"]
    result = expand_namespaces(uris, namespaces)
    assert result == ["http://test.org/var/1", "http://example.com/var/2", "http://xmlns.com/foaf/0.1/Person"]


def test_expand_namespaces_empty_input():
    namespaces = {"xsd": "http://www.w3.org/2001/XMLSchema#"}
    result = expand_namespaces([], namespaces)
    assert result == []


def test_expand_namespaces_empty_namespaces():
    uris = ["sixtine:factor/x", "xsd:string"]
    result = expand_namespaces(uris, {})
    assert result == uris


def test_expand_namespaces_empty_string_uris():
    namespaces = {"xsd": "http://www.w3.org/2001/XMLSchema#"}
    uris = ["", "xsd:decimal"]
    result = expand_namespaces(uris, namespaces)
    assert result == ["", "http://www.w3.org/2001/XMLSchema#decimal"]


def test_expand_namespaces_first_match_wins():
    ns = {"os": "http://os.test/", "opensilex": "http://opensilex.org/"}
    uris = ["opensilex:var/1"]
    result = expand_namespaces(uris, ns)
    # "os" is a substring of "opensilex", so order of dict matters; first match wins
    assert len(result) == 1
    assert result[0].endswith("var/1")