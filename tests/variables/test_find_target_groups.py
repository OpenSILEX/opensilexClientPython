"""Unit tests for find_target_groups with group_mapping."""

import pandas as pd

from opensilex_python_client.variables.groups.find import find_target_groups


class TestFindTargetGroupsDefault:
    def test_single_group(self, sample_df, default_config):
        row = sample_df.iloc[0]
        groups = find_target_groups(row, default_config, list(sample_df.columns))
        assert "http://opensilex.test/id/variablesGroup/phenotyping" in groups
        assert "http://opensilex.test/id/variablesGroup/environment" in groups

    def test_second_row_one_group(self, sample_df, default_config):
        row = sample_df.iloc[1]
        groups = find_target_groups(row, default_config, list(sample_df.columns))
        assert "http://opensilex.test/id/variablesGroup/environment" in groups
        assert "http://opensilex.test/id/variablesGroup/phenotyping" not in groups

    def test_no_groups_falls_back_to_default(self, minimal_df, default_config):
        row = minimal_df.iloc[0]
        groups = find_target_groups(row, default_config, list(minimal_df.columns))
        assert groups == ["http://opensilex.test/id/variablesGroup/default"]


class TestFindTargetGroupsCustomHeaders:
    def test_custom_header_groups(self, custom_header_df, custom_header_config):
        row = custom_header_df.iloc[0]
        groups = find_target_groups(row, custom_header_config, list(custom_header_df.columns))
        assert "http://opensilex.test/id/variablesGroup/phenotyping" in groups
        assert "http://opensilex.test/id/variablesGroup/environment" in groups

    def test_none_columns_falls_back_to_mapping(self, sample_df, default_config):
        row = sample_df.iloc[0]
        groups = find_target_groups(row, default_config, None)
        assert "http://opensilex.test/id/variablesGroup/phenotyping" in groups


class TestFindTargetGroupsMultiValue:
    def test_semicolon_separated(self):
        row = pd.Series({"Group1": "Phenotyping;Environment", "Group2": ""})
        config = {
            "groups": {
                "group_mapping": {
                    "Phenotyping": "Group1",
                    "Environment": "Group1",
                },
                "available_groups": {
                    "Phenotyping": "http://test/id/phenotyping",
                    "Environment": "http://test/id/environment",
                },
            }
        }
        groups = find_target_groups(row, config, list(row.index))
        assert "http://test/id/phenotyping" in groups
        assert "http://test/id/environment" in groups

    def test_comma_separated(self):
        row = pd.Series({"Group1": "Phenotyping,Environment", "Group2": ""})
        config = {
            "groups": {
                "group_mapping": {
                    "Phenotyping": "Group1",
                    "Environment": "Group1",
                },
                "available_groups": {
                    "Phenotyping": "http://test/id/phenotyping",
                    "Environment": "http://test/id/environment",
                },
            }
        }
        groups = find_target_groups(row, config, list(row.index))
        assert "http://test/id/phenotyping" in groups
        assert "http://test/id/environment" in groups

    def test_pipe_separated(self):
        row = pd.Series({"Group1": "Phenotyping|Environment", "Group2": ""})
        config = {
            "groups": {
                "group_mapping": {
                    "Phenotyping": "Group1",
                    "Environment": "Group1",
                },
                "available_groups": {
                    "Phenotyping": "http://test/id/phenotyping",
                    "Environment": "http://test/id/environment",
                },
            }
        }
        groups = find_target_groups(row, config, list(row.index))
        assert "http://test/id/phenotyping" in groups
        assert "http://test/id/environment" in groups


class TestFindTargetGroupsWithColumnIndex:
    def test_integer_column_mapping(self):
        row = pd.Series({"ColA": "Phenotyping", "ColB": ""})
        config = {
            "groups": {
                "group_mapping": {
                    "Phenotyping": 1,
                },
                "available_groups": {
                    "Phenotyping": "http://test/id/phenotyping",
                },
            }
        }
        groups = find_target_groups(row, config, ["ColA", "ColB"])
        assert "http://test/id/phenotyping" in groups


class TestFindTargetGroupsUnknownGroup:
    def test_unrecognized_group_ignored(self):
        row = pd.Series({"Group1": "UnknownGroup", "Group2": ""})
        config = {
            "groups": {
                "group_mapping": {
                    "Phenotyping": "Group1",
                },
                "available_groups": {
                    "Phenotyping": "http://test/id/phenotyping",
                },
                "default_group": "http://test/id/default",
            }
        }
        groups = find_target_groups(row, config, list(row.index))
        assert groups == ["http://test/id/default"]


class TestFindTargetGroupsNoDefault:
    def test_no_default_returns_empty(self):
        row = pd.Series({"Group1": None, "Group2": None})
        config = {
            "groups": {
                "group_mapping": {
                    "Phenotyping": "Group1",
                },
                "available_groups": {},
            }
        }
        groups = find_target_groups(row, config, list(row.index))
        assert groups == []
