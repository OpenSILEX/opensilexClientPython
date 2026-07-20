"""Tests for OpenSILEX connection."""

from unittest.mock import patch

from opensilex_python_client.auth.connect import connect_to_opensilex


def test_connect_to_opensilex_success():
    connection_info = {
        "host": "http://test.com",
        "identifier": "user",
        "password": "password"
    }

    with patch("opensilexClientToolsPython.ApiClient") as MockApiClient:
        mock_instance = MockApiClient.return_value
        mock_instance.default_headers = {"Authorization": "Bearer token"}

        client = connect_to_opensilex(connection_info)

        assert client is not None
        mock_instance.connect_to_opensilex_ws.assert_called_once_with(
            identifier="user",
            password="password",
            host="http://test.com"
        )


def test_connect_to_opensilex_missing_params():
    connection_info = {
        "host": "",
        "identifier": "user",
        "password": "password"
    }

    with patch("opensilex_python_client.auth.connect.sys.exit") as mock_exit:
        connect_to_opensilex(connection_info)
        mock_exit.assert_called_once_with(1)


def test_connect_to_opensilex_failure():
    connection_info = {
        "host": "http://test.com",
        "identifier": "user",
        "password": "password"
    }

    with patch("opensilexClientToolsPython.ApiClient") as MockApiClient:
        mock_instance = MockApiClient.return_value
        mock_instance.connect_to_opensilex_ws.side_effect = Exception("Connection failed")

        client = connect_to_opensilex(connection_info)
        assert client is None
