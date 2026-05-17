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


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Importe des variables dans OpenSILEX depuis un fichier CSV.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # --- Connexion ---
    conn_group = parser.add_argument_group("Connexion")
    conn_group.add_argument(
        "--host",
        default="http://localhost:8666/rest",
        required=True,
        help="URL de l'API OpenSILEX",
    )
    conn_group.add_argument(
        "--identifier",
        default="admin@opensilex.org",
        required=True,
        help="Identifiant de connexion",
    )
    conn_group.add_argument(
        "--password",
        default="admin",
        required=True,
        help="Mot de passe",
    )

    # --- Fichiers ---
    file_group = parser.add_argument_group("Fichiers")
    file_group.add_argument(
        "--csv",
        dest="csv_path",
        required=True,
        help="Chemin vers le fichier CSV des variables",
    )
    file_group.add_argument(
        "--config",
        dest="yaml_config_path",
        required=True,
        help="Chemin vers le fichier de configuration YAML",
    )

    # --- Options ---
    parser.add_argument(
        "--skip-groups",
        action="store_true",
        help="Ne pas rattacher les variables aux groupes après l'import",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Afficher les détails de l'import",
    )

    return parser.parse_args()


def main():
    """
    The main function performs file verification, authentication, variable import, and group attachment
    in an OpenSILEX system.
    """
    args = _parse_args()

    # Vérification des fichiers
    for label, path in [("CSV", args.csv_path), ("YAML", args.yaml_config_path)]:
        if not os.path.isfile(path):
            print(f"❌ Fichier {label} introuvable : {path}", file=sys.stderr)
            sys.exit(1)

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
