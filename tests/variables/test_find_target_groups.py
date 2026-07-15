"""Unit tests for find_target_groups with column mapping."""

import pandas as pd

from opensilex_python_client.variables.groups.find import find_target_groups


class TestFindTargetGroupsDefault:
    def test_single_group(self, sample_df, default_config):
        row = sample_df.iloc[0]
        groups = find_target_groups(row, default_config, None)
        assert "http://opensilex.test/id/variablesGroup/phenotyping" in groups
        assert "http://opensilex.test/id/variablesGroup/environment" in groups

    def test_second_row_one_group(self, sample_df, default_config):
        row = sample_df.iloc[1]
        groups = find_target_groups(row, default_config, None)
        assert "http://opensilex.test/id/variablesGroup/environment" in groups
        assert "http://opensilex.test/id/variablesGroup/phenotyping" not in groups

    def test_no_groups_falls_back_to_default(self, minimal_df, default_config):
        row = minimal_df.iloc[0]
        groups = find_target_groups(row, default_config, None)
        assert groups == ["http://opensilex.test/id/variablesGroup/default"]


class TestFindTargetGroupsWithColMap:
    def test_custom_header_groups(self, custom_header_df, custom_header_config):
        col_map = {
            "group1": "Groupe1",
            "group2": "Groupe2",
        }
        row = custom_header_df.iloc[0]
        groups = find_target_groups(row, custom_header_config, col_map)
        assert "http://opensilex.test/id/variablesGroup/phenotyping" in groups
        assert "http://opensilex.test/id/variablesGroup/environment" in groups

    def test_empty_col_map_falls_back_to_config(self, sample_df, default_config):
        row = sample_df.iloc[0]
        groups = find_target_groups(row, default_config, {})
        assert "http://opensilex.test/id/variablesGroup/phenotyping" in groups

    def test_none_col_map_falls_back_to_config(self, sample_df, default_config):
        row = sample_df.iloc[0]
        groups = find_target_groups(row, default_config, None)
        assert "http://opensilex.test/id/variablesGroup/phenotyping" in groups


class TestFindTargetGroupsMultiValue:
    def test_semicolon_separated(self):
        row = pd.Series({"Group1": "Phenotyping;Environment", "Group2": ""})
        config = {
            "groups": {
                "group_columns": ["Group1", "Group2"],
                "available_groups": {
                    "Phenotyping": "http://test/id/phenotyping",
                    "Environment": "http://test/id/environment",
                },
            }
        }
        groups = find_target_groups(row, config, None)
        assert "http://test/id/phenotyping" in groups
        assert "http://test/id/environment" in groups

    def test_comma_separated(self):
        row = pd.Series({"Group1": "Phenotyping,Environment", "Group2": ""})
        config = {
            "groups": {
                "group_columns": ["Group1", "Group2"],
                "available_groups": {
                    "Phenotyping": "http://test/id/phenotyping",
                    "Environment": "http://test/id/environment",
                },
            }
        }
        groups = find_target_groups(row, config, None)
        assert "http://test/id/phenotyping" in groups
        assert "http://test/id/environment" in groups

    def test_pipe_separated(self):
        row = pd.Series({"Group1": "Phenotyping|Environment", "Group2": ""})
        config = {
            "groups": {
                "group_columns": ["Group1", "Group2"],
                "available_groups": {
                    "Phenotyping": "http://test/id/phenotyping",
                    "Environment": "http://test/id/environment",
                },
            }
        }
        groups = find_target_groups(row, config, None)
        assert "http://test/id/phenotyping" in groups
        assert "http://test/id/environment" in groups


class TestFindTargetGroupsUnknownGroup:
    def test_unrecognized_group_ignored(self):
        row = pd.Series({"Group1": "UnknownGroup", "Group2": ""})
        config = {
            "groups": {
                "group_columns": ["Group1", "Group2"],
                "available_groups": {
                    "Phenotyping": "http://test/id/phenotyping",
                },
                "default_group": "http://test/id/default",
            }
        }
        groups = find_target_groups(row, config, None)
        assert groups == ["http://test/id/default"]


class TestFindTargetGroupsNoDefault:
    def test_no_default_returns_empty(self):
        row = pd.Series({"Group1": None, "Group2": None})
        config = {
            "groups": {
                "group_columns": ["Group1", "Group2"],
                "available_groups": {},
            }
        }
        groups = find_target_groups(row, config, None)
        assert groups == []
