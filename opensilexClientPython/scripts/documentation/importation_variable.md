# Importation des Variables dans les groupes 

## Description
Ce script Python importe des variables depuis un fichier CSV on les rattacher aux groupes.

## Installation
```bash
pip install pandas requests
```

## Utilisation
1. Modifiez le script avec vos paramètres
2. Préparez votre fichier CSV
3. Lancez : `importation_des_variables.py`

## Configuration du script
Modifiez ces lignes dans la fonction main() :

```python
csv_file_path = "/chemin/vers/votre/fichier.csv"

client = OpenSilexDirectAPI(
    host="http://localhost/rest",
    username="guest@opensilex.org",
    password="guest"
)
```

## Format du fichier CSV

### Colonnes obligatoires :
- Variable_name 
- Entity_uri
- Entity_name
- Characteristic_uri
- Characteristic_name
- Method_uri
- Method_name
- Unit_uri
- Unit_name
- Datatype_uri

### Colonnes optionnelles :
- Variable_uri
- Variable_description
- Variable_alternative_name
- Group1
- Group2
 

## Groupes disponibles (ça depend de l'instance)
- Variables Environnementales M3P
- Variables Phénotypiques M3P
- Variables 4P
- Variables SweetPotato

## Résolution des problèmes

**Erreur d'authentification**
- Vérifiez votre nom d'utilisateur et mot de passe
- Vérifiez l'URL du serveur

**Erreur de CSV**
- Vérifiez que toutes les colonnes obligatoires sont présentes
- Vérifiez l'encodage du fichier (UTF-8)

**Variable non créée**
- Vérifiez que les URIs existent dans OpenSILEX

## Ce que fait le script
1. Se connecte à OpenSILEX
2. Lit le fichier CSV
3. Crée les variables une par une
4. Les ajoute aux groupes spécifiés
5. Affiche un rapport final

## Exemple de sortie
```
Authentification réussie
Fichier lu avec succès
25 variables trouvées

1/25 - Temperature_Air
Variable créée avec succès

RÉSUMÉ :
Variables créées: 20
Variables existantes: 5
Variables échouées: 0

Import terminé avec succès!
```