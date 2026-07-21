import argparse
import logging

from rich.console import Console

from opensilex_python_client.variables.download_config_example import EXAMPLE_FILES, download_variables_config

logger = logging.getLogger(__name__)
_console = Console()


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


def main():
    logger.info("Downloading example configuration files for OpenSILEX variables")
    _console.print("Téléchargement des fichiers d'exemples de configuration pour les variables OpenSILEX...")
    args = _parse_args()
    download_variables_config(args.dest)


if __name__ == "__main__":
    main()
