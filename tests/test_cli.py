"""Tests for CLI entry points."""

from unittest.mock import MagicMock, patch

from opensilex_python_client.cli.download_variable_config_example import main as download_main
from opensilex_python_client.cli.import_variables import main as import_main

"""Tests for CLI entry points."""


def test_import_variables_main_no_args():
    with patch("sys.argv", ["import-variables"]):
        with patch("opensilex_python_client.cli.import_variables._parse_args") as mock_args:
            # Mocking _parse_args to return a namespace with required fields
            mock_args.return_value = MagicMock(
                connection_info={"host": "h", "identifier": "i", "password": "p"},
                csv_path="test.csv",
                yaml_config_path="test.yaml",
                verbose=False,
                attach_variables_to_group=True,
                skip_groups=False,
                create_groups=False,
            )


"""Tests for CLI entry points."""


def test_import_variables_main_no_args():
    with patch("sys.argv", ["import-variables"]):
        with patch("opensilex_python_client.cli.import_variables._parse_args") as mock_args:
            # Mocking _parse_args to return a namespace with required fields
            mock_args.return_value = MagicMock(
                connection_info={"host": "h", "identifier": "i", "password": "p"},
                csv_path="test.csv",
                yaml_config_path="test.yaml",
                verbose=False,
                attach_variables_to_group=True,
                skip_groups=False,
                create_groups=False,
            )
            with (
                patch("opensilex_python_client.cli.import_variables.connect.connect_to_opensilex") as mock_conn,
                patch("opensilex_python_client.variables.import_from_csv.run") as mock_run,
            ):
                mock_conn.return_value = MagicMock()
                import_main()

                mock_conn.assert_called_once()
                mock_run.assert_called_once()


def test_download_config_main():
    with patch("sys.argv", ["download-variable-config-example"]):
        with patch("opensilex_python_client.cli.download_variable_config_example._parse_args") as mock_args:
            mock_args.return_value = MagicMock(dest=".")
            # Mock the actual function that does the download
            with patch(
                "opensilex_python_client.cli.download_variable_config_example.download_variables_config"
            ) as mock_dl:
                download_main()
                mock_dl.assert_called_once_with(".")


def test_download_config_main():
    with patch("sys.argv", ["download-variable-config-example"]):
        with patch("opensilex_python_client.cli.download_variable_config_example._parse_args") as mock_args:
            mock_args.return_value = MagicMock(dest=".")
            # Mock the actual function that does the download
            with patch(
                "opensilex_python_client.cli.download_variable_config_example.download_variables_config"
            ) as mock_dl:
                download_main()
                mock_dl.assert_called_once_with(".")


def test_download_config_main():
    with patch("sys.argv", ["download-variable-config-example"]):
        with patch("opensilex_python_client.cli.download_variable_config_example._parse_args") as mock_args:
            mock_args.return_value = MagicMock(dest=".")
            # Mock the actual function that does the download
            with patch(
                "opensilex_python_client.cli.download_variable_config_example.download_variables_config"
            ) as mock_dl:
                download_main()
                mock_dl.assert_called_once_with(".")
