"""Unit tests for resolve_column_mapping and helpers."""

from opensilex_python_client.variables.ctx import VariablesContext
from opensilex_python_client.variables.import_variables_from_csv import (
    ALL_ROLES,
    DEFAULT_COLUMN_MAPPINGS,
    OPTIONAL_ROLES,
    REQUIRED_ROLES,
    resolve_column_mapping,
)


def _ctx_for(config):
    return VariablesContext(client=None, config=config, debug=False)


class TestDefaultMappings:
    def test_all_roles_defined(self):
        assert set(ALL_ROLES) == set(REQUIRED_ROLES) | set(OPTIONAL_ROLES)

    def test_all_roles_in_defaults(self):
        for role in ALL_ROLES:
            assert role in DEFAULT_COLUMN_MAPPINGS

    def test_required_roles_subset(self):
        for role in REQUIRED_ROLES:
            assert role in ALL_ROLES


class TestResolveColumnMappingDefaults:
    def test_empty_config_uses_defaults(self, sample_df, empty_config):
        col_map, errors, warnings = resolve_column_mapping(_ctx_for(empty_config), sample_df)
        assert not errors
        assert col_map["entity_name"] == "Entity_name"
        assert col_map["variable_name"] == "Variable_name"

    def test_config_without_column_mappings_uses_defaults(self, sample_df, default_config):
        col_map, errors, warnings = resolve_column_mapping(_ctx_for(default_config), sample_df)
        assert not errors
        assert col_map["entity_name"] == "Entity_name"
        assert col_map["datatype_uri"] == "Datatype_uri"


class TestResolveColumnMappingIndexBased:
    def test_index_mapping_resolves(self, sample_df, index_based_config):
        col_map, errors, warnings = resolve_column_mapping(_ctx_for(index_based_config), sample_df)
        assert not errors
        assert col_map["entity_name"] == sample_df.columns[0]
        assert col_map["datatype_uri"] == sample_df.columns[5]

    def test_index_out_of_range_error(self, sample_df):
        config = {
            "csv": {
                "column_mappings": {
                    "entity_name": 99,
                }
            }
        }
        col_map, errors, warnings = resolve_column_mapping(_ctx_for(config), sample_df)
        assert len(errors) > 0
        assert "out of range" in errors[0].lower()

    def test_index_zero_error(self, sample_df):
        config = {
            "csv": {
                "column_mappings": {
                    "entity_name": 0,
                }
            }
        }
        col_map, errors, warnings = resolve_column_mapping(_ctx_for(config), sample_df)
        assert len(errors) > 0

    def test_negative_index_error(self, sample_df):
        config = {
            "csv": {
                "column_mappings": {
                    "entity_name": -1,
                }
            }
        }
        col_map, errors, warnings = resolve_column_mapping(_ctx_for(config), sample_df)
        assert len(errors) > 0


class TestResolveColumnMappingStringBased:
    def test_string_mapping_resolves(self, custom_header_df, custom_header_config):
        col_map, errors, warnings = resolve_column_mapping(_ctx_for(custom_header_config), custom_header_df)
        assert not errors
        assert col_map["entity_name"] == "Entite"
        assert col_map["variable_name"] == "NomVar"
        assert col_map["variable_description"] == "Description"

    def test_missing_required_column_error(self, sample_df):
        config = {
            "csv": {
                "column_mappings": {
                    "entity_name": "NonExistentColumn",
                }
            }
        }
        col_map, errors, warnings = resolve_column_mapping(_ctx_for(config), sample_df)
        assert len(errors) > 0
        assert "NonExistentColumn" in errors[0]

    def test_missing_optional_column_warning(self, sample_df):
        config = {
            "csv": {
                "column_mappings": {
                    "variable_description": "NonExistentColumn",
                }
            }
        }
        col_map, errors, warnings = resolve_column_mapping(_ctx_for(config), sample_df)
        assert not errors
        assert len(warnings) > 0
        assert col_map["variable_description"] is None


class TestResolveColumnMappingHybrid:
    def test_hybrid_mapping(self, sample_df, hybrid_config):
        col_map, errors, warnings = resolve_column_mapping(_ctx_for(hybrid_config), sample_df)
        assert not errors
        assert col_map["entity_name"] == sample_df.columns[0]
        assert col_map["characteristic_name"] == "Characteristic_name"
        assert col_map["method_name"] == sample_df.columns[2]


class TestResolveColumnMappingTypes:
    def test_invalid_type_error(self, sample_df):
        config = {
            "csv": {
                "column_mappings": {
                    "entity_name": 3.14,
                }
            }
        }
        col_map, errors, warnings = resolve_column_mapping(_ctx_for(config), sample_df)
        assert len(errors) > 0
        assert "expected int or str" in errors[0]

    def test_list_type_error(self, sample_df):
        config = {
            "csv": {
                "column_mappings": {
                    "entity_name": [1, 2],
                }
            }
        }
        col_map, errors, warnings = resolve_column_mapping(_ctx_for(config), sample_df)
        assert len(errors) > 0


class TestResolveColumnMappingMinimality:
    def test_minimal_df_only_warns_optional(self, minimal_df, empty_config):
        col_map, errors, warnings = resolve_column_mapping(_ctx_for(empty_config), minimal_df)
        assert not errors
        for role in REQUIRED_ROLES:
            assert col_map[role] is not None
        for role in OPTIONAL_ROLES:
            if role in ("entity_definition", "characteristic_definition", "method_definition"):
                assert col_map[role] is None
            assert role not in [w.split("'")[1] for w in errors]


class TestCleanURI:
    """Tests for VariablesContext.clean_uri (moved from _clean_uri)."""

    def test_none(self):
        from opensilex_python_client.variables.ctx import VariablesContext

        assert VariablesContext(client=None).clean_uri(None) is None

    def test_nan(self):
        from opensilex_python_client.variables.ctx import VariablesContext

        assert VariablesContext(client=None).clean_uri(float("nan")) is None

    def test_string(self):
        from opensilex_python_client.variables.ctx import VariablesContext

        assert VariablesContext(client=None).clean_uri("http://example.com") == "http://example.com"

    def test_list_string(self):
        from opensilex_python_client.variables.ctx import VariablesContext

        assert VariablesContext(client=None).clean_uri("['http://example.com']") == "http://example.com"

    def test_empty_string(self):
        from opensilex_python_client.variables.ctx import VariablesContext

        assert VariablesContext(client=None).clean_uri("") is None

    def test_whitespace(self):
        from opensilex_python_client.variables.ctx import VariablesContext

        assert VariablesContext(client=None).clean_uri("  ") is None
