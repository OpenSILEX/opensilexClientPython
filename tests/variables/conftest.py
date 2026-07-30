"""Fixtures for column mapping and group resolution tests."""

from unittest.mock import MagicMock

import pandas as pd
import pytest


@pytest.fixture
def mock_client():
    """Mock of opensilexClientToolsPython.ApiClient."""
    client = MagicMock()
    client.default_headers = {"Authorization": "Bearer token"}
    return client


@pytest.fixture
def mock_variables_api(mock_client):
    """Mock of opensilexClientToolsPython.VariablesApi."""
    api = MagicMock()
    return api


@pytest.fixture
def sample_df():
    """Standard CSV matching default column names."""
    return pd.DataFrame(
        [
            {
                "Entity_name": "Plant",
                "Characteristic_name": "Height",
                "Method_name": "Manual",
                "Unit_name": "Centimeter",
                "Variable_name": "Plant_Height_cm",
                "Datatype_uri": "http://www.w3.org/2001/XMLSchema#decimal",
                "Variable_description": "Plant height",
                "Variable_alternative_name": "PH_cm",
                "Time_interval": "",
                "Phenotyping": "Phenotyping",
                "Environment": "Environment",
                "Group2": "",
            },
            {
                "Entity_name": "Soil",
                "Characteristic_name": "Temperature",
                "Method_name": "Sensor",
                "Unit_name": "Celsius",
                "Variable_name": "Soil_Temp_C",
                "Datatype_uri": "http://www.w3.org/2001/XMLSchema#decimal",
                "Variable_description": "Soil temperature",
                "Variable_alternative_name": "ST_C",
                "Time_interval": "",
                "Phenotyping": "",
                "Environment": "Environment",
                "Group2": "",
            },
        ]
    )


@pytest.fixture
def custom_header_df():
    """CSV with custom headers (different from defaults)."""
    return pd.DataFrame(
        [
            {
                "Entite": "Plant",
                "Caract": "Height",
                "Methode": "Manual",
                "Unite": "Centimeter",
                "NomVar": "Plant_Height_cm",
                "TypeDonnee": "http://www.w3.org/2001/XMLSchema#decimal",
                "Description": "Plant height",
                "NomAlt": "PH_cm",
                "Interval": "",
                "Phenotyping": "Phenotyping",
                "Environment": "Environment",
                "Groupe2": "",
            },
        ]
    )


@pytest.fixture
def minimal_df():
    """CSV with only required columns."""
    return pd.DataFrame(
        [
            {
                "Entity_name": "Plant",
                "Characteristic_name": "Height",
                "Method_name": "Manual",
                "Unit_name": "Centimeter",
                "Variable_name": "Plant_Height_cm",
                "Datatype_uri": "http://www.w3.org/2001/XMLSchema#decimal",
            },
        ]
    )


@pytest.fixture
def empty_config():
    return {}


@pytest.fixture
def default_config():
    """Config with no column_mappings — relies on defaults."""
    return {
        "csv": {},
        "groups": {
            "group_mapping": {
                "Phenotyping": "Phenotyping",
                "Environment": "Environment",
            },
            "available_groups": {
                "Phenotyping": "http://opensilex.test/id/variablesGroup/phenotyping",
                "Environment": "http://opensilex.test/id/variablesGroup/environment",
            },
            "default_group": "http://opensilex.test/id/variablesGroup/default",
        },
    }


@pytest.fixture
def index_based_config():
    """Config using 1-indexed column numbers."""
    return {
        "csv": {
            "column_mappings": {
                "entity_name": 1,
                "characteristic_name": 2,
                "method_name": 3,
                "unit_name": 4,
                "variable_name": 5,
                "datatype_uri": 6,
            }
        },
        "groups": {
            "group_mapping": {
                "Phenotyping": 10,
                "Environment": 11,
            },
            "available_groups": {},
        },
    }


@pytest.fixture
def custom_header_config():
    """Config mapping custom column headers to roles."""
    return {
        "csv": {
            "column_mappings": {
                "entity_name": "Entite",
                "characteristic_name": "Caract",
                "method_name": "Methode",
                "unit_name": "Unite",
                "variable_name": "NomVar",
                "datatype_uri": "TypeDonnee",
                "variable_description": "Description",
                "variable_alternative_name": "NomAlt",
            }
        },
        "groups": {
            "group_mapping": {
                "Phenotyping": "Phenotyping",
                "Environment": "Environment",
            },
            "available_groups": {
                "Phenotyping": "http://opensilex.test/id/variablesGroup/phenotyping",
                "Environment": "http://opensilex.test/id/variablesGroup/environment",
            },
            "default_group": "http://opensilex.test/id/variablesGroup/default",
        },
    }


@pytest.fixture
def hybrid_config():
    """Config mixing integers and strings."""
    return {
        "csv": {
            "column_mappings": {
                "entity_name": 1,
                "characteristic_name": "Characteristic_name",
                "method_name": 3,
                "unit_name": 4,
                "variable_name": "Variable_name",
                "datatype_uri": "Datatype_uri",
            }
        },
        "groups": {
            "group_mapping": {
                "Phenotyping": 10,
                "Environment": 11,
            },
            "available_groups": {},
        },
    }
