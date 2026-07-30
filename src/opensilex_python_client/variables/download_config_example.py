import importlib.resources as pkg_resources
import shutil
from pathlib import Path

from .._logging import get_logger

logger = get_logger(__name__)

EXAMPLE_FILES = [
    "test_variables.csv",
    "test_config.yaml",
    "test_variables_with_uris.csv",
]


def download_variables_config(dest: str = "."):
    """
    Download example configuration files to the specified destination directory.

    This function copies bundled example files from the `opensilex_python_client`
    package to a local directory. These files serve as templates for structuring
    variable import data for OpenSILEX.

    Files copied:
        - test_variables.csv              : Example variable definitions (no URIs)
        - test_config.yaml                : Example YAML configuration file
        - test_variables_with_uris.csv    : Example variable definitions (with URIs)

    :param dest: Path to the destination directory where the example files will be
                 saved. Accepts both relative and absolute paths. The directory is
                 created automatically (including parent directories) if it does not
                 already exist. Defaults to the current working directory (".").
    :type dest: str

    :raises OSError: If the destination directory cannot be created due to permission
                     issues or an invalid path.

    :example:
        >>> download_variables_config()                        # saves to current dir
        >>> download_variables_config("./config/opensilex")   # saves to a subdirectory
        >>> download_variables_config("/tmp/silex_examples")  # saves to an absolute path
    """
    dest_path = Path(dest)
    dest_path.mkdir(parents=True, exist_ok=True)

    package_dir = pkg_resources.files("opensilex_python_client.data_examples.variables")

    failed = []
    for filename in EXAMPLE_FILES:
        try:
            src = package_dir.joinpath(filename)
            shutil.copy(src, dest_path / filename)
            logger.info("Copied %s to %s", filename, dest_path / filename)
        except Exception as e:
            logger.error("Failed to copy %s: %s", filename, e)
            failed.append(filename)

    if failed:
        logger.warning("%d file(s) could not be copied: %s", len(failed), failed)
    else:
        logger.info("All %d files saved to %s", len(EXAMPLE_FILES), dest_path)
