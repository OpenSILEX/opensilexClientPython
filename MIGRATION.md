# Guide de Migration - opensilexClientPython v2.0

Ce document explique les changements entre l'ancienne version (scripts standalone) et la nouvelle version (package Python complet).

---

## Vue d'Ensemble des Changements

### De Scripts à Package Python

**AVANT (v1) :** Scripts standalone dans `scripts/`
```
scripts/
├── variables_import.py       # Script principal
└── import_functions.py       # Toutes les fonctions
```

**APRÈS (v2) :** Package Python modulaire
```
src/opensilexClientPython/
├── auth/
│   └── connect.py
├── file_management/
│   ├── read_csv.py
│   └── read_yaml.py
└── variables/
    ├── import_from_csv.py
    ├── create.py
    ├── exists.py
    ├── _components/
    └── groups/
```

---

## Correspondance des Fonctions

### 1. Connexion au Client

**AVANT :**
```python
from import_functions import get_client

client = get_client(
    "http://localhost:8081/rest",
    "guest@opensilex.org", 
    "guest",
    False  # verbose
)
```

**APRÈS :**
```python
from opensilexClientPython.auth import connect

client = connect.connect_to_opensilex({
    "host": "http://localhost:8081/rest",
    "identifier": "guest@opensilex.org",
    "password": "guest"
})
```

---

### 2. Lecture des Fichiers

**AVANT :**
```python
from import_functions import read_csv, load_config

df = read_csv(CSV_PATH)
config = load_config(CONFIG_PATH)
```

**APRÈS :**
```python
from opensilexClientPython.file_management import read_csv, read_yaml

df = read_csv(csv_path)
config = read_yaml(config_path)
```

**Changement :** `load_config()` → `read_yaml()`

---

### 3. Création des Composants

**AVANT :**
```python
from import_functions import create_components_from_csv

df_with_uris = create_components_from_csv(client, df, CSV_PATH)
```

**APRÈS :**
```python
# Intégré automatiquement dans import_from_csv.run()
# Plus besoin d'appeler séparément !
```

---

### 4. Import des Variables

**AVANT :**
```python
from import_functions import import_variables

grouped_variables = import_variables(client, df_with_uris, config)
```

**APRÈS :**
```python
from opensilexClientPython.variables import import_from_csv

# Fait TOUT : composants + variables
grouped_vars = import_from_csv.run(client, csv_path, config_path)
```

---

### 5. Attachement aux Groupes

**AVANT :**
```python
from import_functions import attach_to_groups

attach_to_groups(client, grouped_variables, config)
```

**APRÈS :**
```python
from opensilexClientPython.variables.groups import update

update.attach_to_groups(client, grouped_vars, config_path)
```

---

##  Migration Complète du Script

### Script Principal - AVANT (v1)

**Fichier : `scripts/variables_import.py`**
```python
import os
from import_functions import *

# Configuration en dur dans le script
CSV_PATH = "/path/to/your/variables.csv"
CONFIG_PATH = "/path/to/your/groups_config.yaml"
OPENSILEX_HOST = "http://your-instance:8081/rest"
OPENSILEX_IDENTIFIER = "guest@opensilex.org"
OPENSILEX_PASSWORD = "guest"
OPENSILEX_VERBOSE = False

if __name__ == "__main__":
    # Vérifier les fichiers
    if not os.path.exists(CONFIG_PATH):
        print(f"✗ Config missing: {CONFIG_PATH}")
        exit(1)
    
    if not os.path.exists(CSV_PATH):
        print(f"✗ CSV missing: {CSV_PATH}")
        exit(1)
    
    # Initialiser client et charger données
    client = get_client(OPENSILEX_HOST, OPENSILEX_IDENTIFIER, 
                       OPENSILEX_PASSWORD, OPENSILEX_VERBOSE)
    config = load_config(CONFIG_PATH)
    df = read_csv(CSV_PATH)
    
    # STEP 1: Créer composants
    df_with_uris = create_components_from_csv(client, df, CSV_PATH)
    
    # STEP 2: Importer variables
    grouped_variables = import_variables(client, df_with_uris, config)
    
    # STEP 3: Attacher aux groupes
    total_vars = sum(len(set(vars)) for vars in grouped_variables.values())
    if total_vars > 0:
        attach_to_groups(client, grouped_variables, config)
```

---

### Script Principal - APRÈS (v2)

**Fichier : `examples/import_variables_example.py`**
```python
from opensilexClientPython.auth import connect
from opensilexClientPython.variables import import_from_csv
from opensilexClientPython.variables.groups import update

# 1. Authentification
client = connect.connect_to_opensilex({
    "host": "http://your-instance:8081/rest",
    "identifier": "guest@opensilex.org",
    "password": "guest"
})

# 2. Chemins des fichiers
csv_path = "/path/to/your/variables.csv"
config_path = "/path/to/your/groups_config.yaml"

# 3. Import variables (composants + variables)
grouped_vars = import_from_csv.run(client, csv_path, config_path)

# 4. Attacher aux groupes
update.attach_to_groups(client, grouped_vars, config_path)
```

---

## Nouvelles Fonctionnalités

### 1. Auto-génération des URIs

**AVANT :** Les URIs devaient être calculées et remplies manuellement

**MAINTENANT :** Auto-génération complète à partir des noms uniquement !
```csv
Entity_name,Characteristic_name,Method_name,Unit_name,Variable_name
Plant,Height,Manual,Centimeter,Plant_Height_cm
```

Les colonnes `*_uri` sont **complètement ignorées**.

---

### 2. Package Python Installable

**AVANT :** Scripts à copier manuellement

**MAINTENANT :** Package installable avec Poetry
```bash
# Installation
poetry install

# Utilisation
poetry run python examples/import_variables_example.py
```

---

### 3. Documentation Auto-générée
```bash
# Générer la documentation
poetry run pdoc src/opensilexClientPython -o docs/

# Ouvrir
xdg-open docs/index.html
```

---

### 4. Jupyter Notebook
```bash
# Lancer le notebook d'exemple
poetry run jupyter notebook examples/import_variables_example.ipynb
```

---

### 5. Architecture Modulaire Réutilisable

**AVANT :** Tout dans un seul fichier `import_functions.py`

**MAINTENANT :** Modules séparés et réutilisables
```python
# Utiliser seulement la lecture CSV
from opensilexClientPython.file_management import read_csv
df = read_csv("data.csv")

# Créer une variable individuellement
from opensilexClientPython.variables import create_variable
var_uri = create_variable(client, var_data)

# Vérifier si une variable existe
from opensilexClientPython.variables import exists
uri = exists(client, name="my_variable")
```

---

## Tableau Comparatif Complet

| Aspect | Ancienne Version (v1) | Nouvelle Version (v2) |
|--------|----------------------|----------------------|
| **Structure** | Scripts standalone | Package Python |
| **Installation** | Copier les scripts | `poetry install` |
| **Configuration** | Hard-codée dans le script | Paramètres de fonction |
| **get_client()** | 4 paramètres séparés | Dict de config |
| **load_config()** | `load_config()` | `read_yaml()` |
| **Composants** | Fonction séparée | Intégré dans `run()` |
| **URIs** | Calculées manuellement | Auto-générées |
| **Imports** | `from import_functions import *` | Imports modulaires explicites |
| **Documentation** | README.md seulement | Documentation API complète |
| **Exemples** | 1 script | Script + Notebook |
| **Tests** | Aucun | Framework de tests |
| **Réutilisabilité** | Monolithique | Modulaire |

---

## Checklist de Migration

### Étape 1 : Installer le nouveau package
```bash
cd opensilexClientPython
poetry install
```

---

### Étape 2 : Adapter le script

**Remplacer :**
```python
from import_functions import *
client = get_client(host, id, pwd, verbose)
config = load_config(path)
df_uris = create_components_from_csv(client, df, path)
grouped = import_variables(client, df_uris, config)
attach_to_groups(client, grouped, config)
```

**Par :**
```python
from opensilexClientPython.auth import connect
from opensilexClientPython.variables import import_from_csv
from opensilexClientPython.variables.groups import update

client = connect.connect_to_opensilex({...})
grouped = import_from_csv.run(client, csv_path, config_path)
update.attach_to_groups(client, grouped, config_path)
```

---

### Étape 3 : Simplifier le CSV

Vous pouvez maintenant **retirer toutes les colonnes `*_uri`** !

Gardez seulement :
- `Entity_name`
- `Characteristic_name`
- `Method_name`
- `Unit_name`
- `Variable_name`
- `Datatype_uri`
- `Group1`, `Group2` (optionnel)

---

### Étape 4 : Tester
```bash
poetry run python examples/import_variables_example.py
```

---

## Points d'Attention

### 1. Import des Modules

**AVANT :** Import global avec `*`
```python
from import_functions import *
```

**MAINTENANT :** Imports explicites
```python
from opensilexClientPython.auth import connect
from opensilexClientPython.variables import import_from_csv
```

---

### 2. Paramètres du Client

**AVANT :** 4 paramètres séparés
```python
client = get_client(host, identifier, password, verbose)
```

**MAINTENANT :** Dictionnaire
```python
client = connect.connect_to_opensilex({
    "host": host,
    "identifier": identifier,
    "password": password
})
```

---

### 3. Retour de Fonction

**AVANT :** `import_variables()` retournait directement le résultat

**MAINTENANT :** `run()` retourne `grouped_variables` qui doit être passé à `attach_to_groups()`
```python
# v2
grouped_vars = import_from_csv.run(client, csv_path, config_path)
update.attach_to_groups(client, grouped_vars, config_path)
```