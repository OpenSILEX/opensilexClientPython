"""Tests for the logging infrastructure."""

import logging


class TestPackageLogger:
    """Test that the package root exports a working logger."""

    def test_logger_is_exported(self):
        """The package exports a logger instance."""
        from opensilex_python_client import logger

        assert logger is not None
        assert isinstance(logger, logging.Logger)

    def test_logger_name(self):
        """Logger is named for the _logging module."""
        from opensilex_python_client import logger

        assert logger.name == "opensilex_python_client._logging"

    def test_root_logger_has_handler(self):
        """Importing the logger automatically configures the root logger."""
        from opensilex_python_client import logger  # noqa: F401

        root = logging.getLogger()
        assert len(root.handlers) >= 1


class TestModuleLoggers:
    """Each module exposes a module-level logger with correct __name__."""

    def test_auth_module(self):
        from opensilex_python_client.auth import connect

        assert hasattr(connect, "logger")
        assert connect.logger.name == "opensilex_python_client.auth.connect"

    def test_create_module(self):
        from opensilex_python_client.variables import create

        assert hasattr(create, "logger")
        assert create.logger.name == "opensilex_python_client.variables.create"

    def test_ctx_module(self):
        from opensilex_python_client.variables import ctx

        assert hasattr(ctx, "logger")
        assert ctx.logger.name == "opensilex_python_client.variables.ctx"

    def test_exists_module(self):
        from opensilex_python_client.variables.exists import logger

        assert logger.name == "opensilex_python_client.variables.exists"

    def test_import_csv_module(self):
        from opensilex_python_client.variables import import_variables_from_csv

        assert hasattr(import_variables_from_csv, "logger")
        assert import_variables_from_csv.logger.name == "opensilex_python_client.variables.import_variables_from_csv"

    def test_download_config_example_module(self):
        from opensilex_python_client.variables import download_config_example

        assert hasattr(download_config_example, "logger")
        assert download_config_example.logger.name == "opensilex_python_client.variables.download_config_example"

    def test_component_resolver_module(self):
        from opensilex_python_client.variables import _component_resolver

        assert hasattr(_component_resolver, "logger")
        assert _component_resolver.logger.name == "opensilex_python_client.variables._component_resolver"

    def test_groups_find_module(self):
        from opensilex_python_client.variables.groups import find

        assert hasattr(find, "logger")
        assert find.logger.name == "opensilex_python_client.variables.groups.find"

    def test_groups_manage_module(self):
        from opensilex_python_client.variables.groups import manage

        assert hasattr(manage, "logger")
        assert manage.logger.name == "opensilex_python_client.variables.groups.manage"

    def test_cli_import_module(self):
        from opensilex_python_client.cli import import_variables

        assert hasattr(import_variables, "logger")
        assert import_variables.logger.name == "opensilex_python_client.cli.import_variables"

    def test_cli_download_config_module(self):
        from opensilex_python_client.cli import download_variable_config_example

        assert hasattr(download_variable_config_example, "logger")
        assert (
            download_variable_config_example.logger.name
            == "opensilex_python_client.cli.download_variable_config_example"
        )

    def test_file_management_read_csv(self):
        from opensilex_python_client.file_management import read_csv

        assert hasattr(read_csv, "logger")
        assert read_csv.logger.name == "opensilex_python_client.file_management.read_csv"

    def test_file_management_read_yaml(self):
        from opensilex_python_client.file_management import read_yaml

        assert hasattr(read_yaml, "logger")
        assert read_yaml.logger.name == "opensilex_python_client.file_management.read_yaml"


class TestLoggingBehavior:
    """Test that logger calls actually emit through the handler."""

    def test_info_message_emits(self, caplog):
        """logger.info messages appear in the handler."""
        from opensilex_python_client import logger

        with caplog.at_level(logging.INFO):
            logger.info("test info message")
        assert "test info message" in caplog.text

    def test_debug_message_requires_debug_level(self, caplog):
        """logger.debug messages are filtered unless level is DEBUG."""
        from opensilex_python_client import logger

        with caplog.at_level(logging.WARNING):
            logger.debug("test debug message")
        assert "test debug message" not in caplog.text

    def test_error_message_emits(self, caplog):
        """logger.error messages appear in the handler."""
        from opensilex_python_client import logger

        with caplog.at_level(logging.ERROR):
            logger.error("test error message")
        assert "test error message" in caplog.text
