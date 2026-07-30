#!/usr/bin/env python3
"""
CLI pour l'import de variables OpenSILEX depuis un fichier CSV.
"""

import argparse
import os
import sys

from rich.console import Console

from opensilex_python_client._logging import get_logger
from opensilex_python_client.auth import connect
from opensilex_python_client.variables import import_from_csv
from opensilex_python_client.variables.groups import manage
from opensilex_python_client.variables.groups.manage import find_or_create_group

logger = get_logger(__name__)
_console = Console()


def _existing_file(value: str) -> str:
    """argparse type: vérifie que le fichier existe."""
    if not os.path.isfile(value):
        raise argparse.ArgumentTypeError(f"Fichier introuvable : {value}")
    return value


def _parse_args():
    parser = argparse.ArgumentParser(
        prog="opensilex-import",
        description=(
            "Importe des variables dans OpenSILEX depuis un fichier CSV\n"
            "en s'appuyant sur un fichier de configuration YAML."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Exemples d'utilisation :\n"
            "  %(prog)s --host http://localhost:8666/rest \\\n"
            "           --identifier admin@opensilex.org --password admin \\\n"
            "           --csv variables.csv --config config.yaml\n\n"
            "  # Import sans rattachement aux groupes, mode verbeux :\n"
            "  %(prog)s --csv variables.csv --config config.yaml \\\n"
            "           --skip-groups --verbose\n\n"
            "  # Utilisation des variables d'environnement :\n"
            "  export OPENSILEX_HOST=http://prod-server/rest\n"
            "  export OPENSILEX_IDENTIFIER=admin@opensilex.org\n"
            "  export OPENSILEX_PASSWORD=secret\n"
            "  %(prog)s --csv variables.csv --config config.yaml\n"
        ),
    )

    # --- Connexion ---
    conn_group = parser.add_argument_group(
        "Connexion",
        description=(
            "Paramètres de connexion à l'API OpenSILEX. "
            "Chaque valeur peut être définie via une variable d'environnement "
            "(OPENSILEX_HOST, OPENSILEX_IDENTIFIER, OPENSILEX_PASSWORD) "
            "ou directement en argument."
        ),
    )
    conn_group.add_argument(
        "--host",
        default=os.environ.get("OPENSILEX_HOST", "http://localhost:8666/rest"),
        metavar="URL",
        help=(
            "URL de base de l'API OpenSILEX. "
            "Doit inclure le chemin /rest. "
            "Peut aussi être définie via la variable d'environnement OPENSILEX_HOST. "
            "(défaut : http://localhost:8666/rest)"
        ),
    )
    conn_group.add_argument(
        "--identifier",
        default=os.environ.get("IDENTIFIER", "admin@opensilex.org"),
        metavar="EMAIL",
        help=(
            "Identifiant (adresse e-mail) du compte OpenSILEX. "
            "Peut aussi être définie via OPENSILEX_IDENTIFIER. "
            "(défaut : admin@opensilex.org)"
        ),
    )
    conn_group.add_argument(
        "--password",
        default=os.environ.get("OPENSILEX_PASSWORD"),
        metavar="MOT_DE_PASSE",
        help=(
            "Mot de passe du compte OpenSILEX. "
            "Peut aussi être définie via OPENSILEX_PASSWORD. "
            "⚠️  Évitez de passer le mot de passe en clair dans le shell ; "
            "préférez la variable d'environnement."
        ),
    )

    # --- Fichiers ---
    file_group = parser.add_argument_group(
        "Fichiers",
        description="Chemins vers les fichiers d'entrée requis pour l'import.",
    )
    file_group.add_argument(
        "--csv",
        dest="csv_path",
        required=True,
        metavar="FICHIER.csv",
        type=_existing_file,
        help=(
            "Chemin vers le fichier CSV contenant les variables à importer. "
            "Le fichier doit exister et être lisible. "
            "Consultez la documentation pour le format attendu des colonnes."
        ),
    )
    file_group.add_argument(
        "--config",
        dest="yaml_config_path",
        required=True,
        metavar="FICHIER.yaml",
        type=_existing_file,
        help=(
            "Chemin vers le fichier de configuration YAML. "
            "Définit les mappings de colonnes, les groupes cibles, "
            "et les options d'import avancées."
        ),
    )

    # --- Options ---
    options_group = parser.add_argument_group("Options")
    options_group.add_argument(
        "--create-groups",
        action="store_true",
        default=False,
        help=("Si activé, crée les groupes définis dans le fichier YAML s'ils n'existent pas encore dans OpenSILEX."),
    )
    options_group.add_argument(
        "--attach-variables-to-group",
        action="store_true",
        default=True,
        help=("Si activé, rattache les variables importées aux groupes définis dans le fichier YAML. (Défaut : True)"),
    )
    options_group.add_argument(
        "--skip-groups",
        action="store_true",
        default=False,
        help=(
            "Si activé, les variables importées ne seront pas rattachées "
            "aux groupes définis dans le fichier YAML. "
            "Utile pour un import rapide ou pour tester sans modifier les groupes."
        ),
    )
    options_group.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        default=False,
        help=(
            "Affiche les détails de chaque étape : "
            "connexion, fichiers utilisés, progression de l'import, "
            "et rattachement aux groupes."
        ),
    )

    args = parser.parse_args()

    # Validation post-parse : mot de passe obligatoire
    if not args.password:
        parser.error(
            "Le mot de passe est requis. Utilisez --password ou la variable d'environnement OPENSILEX_PASSWORD."
        )

    return args


def main():
    """
    Effectue la vérification des fichiers, l'authentification,
    l'import des variables et le rattachement aux groupes dans OpenSILEX.
    """
    args = _parse_args()

    if args.verbose:
        logger.debug("Connection: host=%s, identifier=%s", args.host, args.identifier)
        logger.debug("Files: csv=%s, config=%s", args.csv_path, args.yaml_config_path)
        _console.print(f"📡 Connexion à : {args.host}")
        _console.print(f"👤 Identifiant : {args.identifier}")
        _console.print(f"📄 CSV         : {args.csv_path}")
        _console.print(f"⚙️  Config YAML : {args.yaml_config_path}")

    # 1. Authentification
    client = connect.connect_to_opensilex(
        {
            "host": args.host,
            "identifier": args.identifier,
            "password": args.password,
        }
    )
    if client is None:
        logger.error("Connection to OpenSILEX failed")
        _console.print("❌ Échec de la connexion à OpenSILEX", file=sys.stderr)
        sys.exit(1)

    # 2. Import des variables
    if args.verbose:
        logger.debug("Starting import of variables from CSV")
        _console.print("\n🔄 Import des variables en cours...")
    grouped_vars = import_from_csv.run(client, args.csv_path, args.yaml_config_path, debug=args.verbose)

    # Note: If --create-groups is enabled, we should ensure groups exist.
    # This logic might be inside attach_variables or we might need to call a helper.
    if args.create_groups:
        if args.verbose:
            logger.debug("Checking/creating groups from config file")
            _console.print("⚙️  Vérification/Création des groupes du fichier config...")
        # Implementation for create_groups logic based on YAML config
        # we'll need to extract groups from YAML and call find_or_create_group
        from opensilex_python_client.file_management.read_yaml import read_yaml

        config = read_yaml(args.yaml_config_path)
        group_config = config.get("groups", {})
        available_groups = group_config.get("available_groups", {})

        for group_name, group_uri in available_groups.items():
            find_or_create_group(client, group_uri if group_uri else None, group_name)

    # 3. Rattachement aux groupes
    if args.attach_variables_to_group and not args.skip_groups:
        if args.verbose:
            logger.debug("Attaching variables to groups")
            _console.print("🔗 Attaching variables to groups...")
        manage.attach_variables(client, grouped_vars, args.yaml_config_path)
    elif args.skip_groups:
        _console.print("⏭️  Attaching variables to groups ignored (--skip-groups)")
    else:
        _console.print("ℹ️  Attaching variables to disabled (--attach-variables-to-group False)")

    _console.print("✅ Import completed successfully !")
    logger.info("Import completed successfully")


if __name__ == "__main__":
    main()
