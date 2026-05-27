#!/usr/bin/env python3
"""
CLI pour l'import de variables OpenSILEX depuis un fichier CSV.
"""

import argparse
import os
import sys

from opensilex_python_client.auth import connect
from opensilex_python_client.variables import import_from_csv
from opensilex_python_client.variables.groups import update


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
        print(f"📡 Connexion à : {args.host}")
        print(f"👤 Identifiant : {args.identifier}")
        print(f"📄 CSV         : {args.csv_path}")
        print(f"⚙️  Config YAML : {args.yaml_config_path}")

    # 1. Authentification
    client = connect.connect_to_opensilex(
        {
            "host": args.host,
            "identifier": args.identifier,
            "password": args.password,
        }
    )
    if client is None:
        print("❌ Échec de la connexion à OpenSILEX", file=sys.stderr)
        sys.exit(1)

    # 2. Import des variables
    if args.verbose:
        print("\n🔄 Import des variables en cours...")
    grouped_vars = import_from_csv.run(client, args.csv_path, args.yaml_config_path)

    # 3. Rattachement aux groupes
    if not args.skip_groups:
        if args.verbose:
            print("🔗 Rattachement aux groupes...")
        update.attach_to_groups(client, grouped_vars, args.yaml_config_path)
    else:
        print("⏭️  Rattachement aux groupes ignoré (--skip-groups)")

    print("✅ Import terminé avec succès !")


if __name__ == "__main__":
    main()
