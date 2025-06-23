import pandas as pd
from opensilex_client.opensilex_client import OpensilexClient
import traceback

# 1. Initialisation du client OpenSILEX
opensilex_client = OpensilexClient(
    host="http://138.102.159.37:8095/rest",
    identifier="admin@opensilex.org",
    password="admin",
    verbose=False
)

# 2. Chemins vers les fichiers
csv_path = "/home/jemaa/MAU17/Variables _env.csv"
yaml_path = "/home/jemaa/Bureau/opensilexClientPython/variables_import_params.yaml"

# 3. Diagnostic brut du fichier CSV
print(f"🔍 Lecture brute du fichier : {csv_path}")
try:
    with open(csv_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        print(f" Le fichier contient {len(lines)} ligne(s).")
        if len(lines) == 0:
            print(" Le fichier est vide.")
            exit(1)
        else:
            print(" Aperçu des 5 premières lignes :")
            for i, line in enumerate(lines[:5]):
                print(f"Ligne {i+1}: {repr(line)}")
except FileNotFoundError:
    print(f" Fichier non trouvé à l'emplacement : {csv_path}")
    exit(1)
except Exception as e:
    print(f" Erreur lors de l'ouverture du fichier : {str(e)}")
    exit(1)

# 4. Chargement tolérant du fichier CSV avec Pandas
try:
    df = pd.read_csv(csv_path, engine="python", dtype=str, on_bad_lines='skip')
    df = df.where(pd.notnull(df), None)  # Remplace NaN par None
    print(" CSV chargé avec succès. Aperçu :")
    print(df.head())
except Exception as e:
    print(" Erreur lors du chargement du fichier CSV :")
    traceback.print_exc()
    exit(1)

# 5. Import des variables dans OpenSILEX
try:
    response = opensilex_client.variable.sheet_create_if_not_exists(
        path_or_url_to_sheet=csv_path,
        yaml_path=yaml_path
    )
    print(" Variables importées avec succès !")
    print(response)
except Exception as e:
    print(" Erreur lors de l'import des variables :")
    traceback.print_exc()