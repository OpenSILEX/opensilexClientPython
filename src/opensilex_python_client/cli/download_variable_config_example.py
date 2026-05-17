# mypackage/cli.py
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
        description="Télécharge les exemples de configuration pour l'import de variables pour OpenSILEX.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--dest",
        default=".",
        help="Répertoire de destination pour les fichiers téléchargés",
    )

    return parser.parse_args()


def download_examples(dest: str = "."):
    """
    The function `download_examples` downloads example files to a specified destination directory,
    handling any errors that may occur during the process.

    :param dest: The `dest` parameter in the `download_examples` function is a string that represents
    the destination directory where the example files will be downloaded. If no destination is provided,
    it defaults to the current directory (".") where the function is being called, defaults to .
    :type dest: str (optional)
    """
    # Python 3.9+
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



def main():
    args = _parse_args()
    download_examples(args.dest)
