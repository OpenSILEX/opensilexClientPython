import argparse
import importlib.resources as pkg_resources
import shutil
from pathlib import Path

EXAMPLE_FILES = [
    "test_variables.csv",
    "test_config.yaml",
    "test_variables_with_uris.csv",
]


def _parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Télécharge les fichiers d'exemples de configuration nécessaires à l'import "
            "de variables dans OpenSILEX.\n\n"
            "Les fichiers suivants seront copiés dans le répertoire de destination :\n"
            + "\n".join(f"  - {f}" for f in EXAMPLE_FILES)
            + "\n\nCes fichiers servent de modèles pour structurer vos données avant import."
        ),
        epilog=(
            "Exemples d'utilisation :\n"
            "  %(prog)s\n"
            "  %(prog)s --dest ./config/opensilex\n"
            "  %(prog)s --dest /home/user/project/variables\n\n"
            "Le répertoire de destination sera créé automatiquement s'il n'existe pas."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--dest",
        default=".",
        metavar="RÉPERTOIRE",
        help=(
            "Chemin vers le répertoire de destination où les fichiers d'exemples seront copiés.\n"
            "Peut être un chemin relatif ou absolu. Le répertoire sera créé automatiquement "
            "s'il n'existe pas déjà.\n"
            "(défaut : répertoire courant '.')"
        ),
    )

    return parser.parse_args()


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
            print(f"✅ Saved {filename} to {dest_path / filename}")
        except Exception as e:
            print(f"❌ Failed to copy {filename}: {e}")
            failed.append(filename)

    if failed:
        print(f"\n⚠️  {len(failed)} file(s) could not be copied: {failed}")
    else:
        print(f"\n✅ All {len(EXAMPLE_FILES)} files saved to {dest_path}")
