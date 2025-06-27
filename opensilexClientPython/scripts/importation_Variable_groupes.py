import pandas as pd
import requests
import json
from typing import List, Dict, Optional
import urllib.parse
import os

class OpenSILEXDirectAPI:
    """
    Client OpenSILEX qui contourne le bug du client Python officiel
    """
    
    def __init__(self, host: str = "http://localhost/rest", 
                 username: str = "guest@opensilex.org", 
                 password: str = "guest"):
        self.host = host.rstrip('/rest').rstrip('/')
        self.base_url = f"{self.host}/rest"
        self.username = username
        self.password = password
        self.token = None
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
        # Authentification automatique
        self.authenticate()
    
    def authenticate(self) -> bool:
        """Authentification via API REST"""
        try:
            auth_data = {
                "identifier": self.username,
                "password": self.password
            }
            
            response = requests.post(
                f"{self.base_url}/security/authenticate", 
                json=auth_data, 
                headers=self.headers
            )
            
            if response.status_code == 200:
                result = response.json()
                self.token = result['result']['token']
                self.headers['Authorization'] = f'Bearer {self.token}'
                print(" Authentification réussie")
                return True
            else:
                print(f" Échec authentification: {response.status_code}")
                return False
                
        except Exception as e:
            print(f" Erreur authentification: {e}")
            return False
    
    def create_variable(self, variable_data: Dict) -> Optional[str]:
        """Crée une nouvelle variable"""
        try:
            print(f"Tentative création: {variable_data.get('name', 'Unknown')}")
            print(f"   Données: {json.dumps(variable_data, indent=2)}")
            
            response = requests.post(
                f"{self.base_url}/core/variables",
                headers=self.headers,
                json=variable_data
            )
            
            if response.status_code == 201:
                result = response.json()
                print(f"Succès: {result['result']}")
                return result['result']
            else:
                print(f"    Erreur {response.status_code}")
                print(f"   Réponse: {response.text}")
                return None
                
        except Exception as e:
            print(f"    Exception: {e}")
            return None
    
    def find_variable_by_name(self, variable_name: str) -> Optional[Dict]:
        """Trouve une variable par son nom exact"""
        try:
            params = {'name': variable_name}
            response = requests.get(
                f"{self.base_url}/core/variables",
                headers=self.headers,
                params=params
            )
            
            if response.status_code == 200:
                variables_data = response.json()
                if 'result' in variables_data:
                    for variable in variables_data['result']:
                        if variable.get('name') == variable_name:
                            return variable
            return None
                
        except Exception as e:
            print(f" Erreur recherche variable: {e}")
            return None
    
    def get_group_details(self, group_uri: str) -> Optional[Dict]:
        """Récupère les détails d'un groupe"""
        try:
            encoded_uri = urllib.parse.quote(group_uri, safe='')
            response = requests.get(
                f"{self.base_url}/core/variables_group/{encoded_uri}",
                headers=self.headers
            )
            
            if response.status_code == 200:
                return response.json()['result']
            else:
                print(f" Erreur récupération groupe: {response.status_code}")
                return None
                
        except Exception as e:
            print(f" Erreur détails groupe: {e}")
            return None
    
    def update_variable_group(self, group_uri: str, variable_uris: List[str]) -> bool:
        """Met à jour un groupe avec des variables"""
        try:
            # Récupérer les détails actuels du groupe
            current_group = self.get_group_details(group_uri)
            if not current_group:
                print(f" Impossible de récupérer le groupe {group_uri}")
                return False
            
            # Préparer les données de mise à jour
            update_data = {
                "uri": group_uri,
                "name": current_group.get("name", ""),
                "description": current_group.get("description", ""),
                "variables": variable_uris
            }
            
            # Ajouter d'autres champs si ils existent
            for field in ['publisher', 'publicationDate', 'lastUpdatedDate']:
                if field in current_group:
                    update_data[field] = current_group[field]
            
            response = requests.put(
                f"{self.base_url}/core/variables_group",
                headers=self.headers,
                json=update_data
            )
            
            if response.status_code == 200:
                return True
            else:
                print(f" Erreur update groupe: {response.status_code}")
                return False
                
        except Exception as e:
            print(f" Erreur API update: {e}")
            return False


class VariableImporter:
    """
    Importeur spécialisé pour les variables avec le format CSV fourni
    """
    
    def __init__(self, csv_file_path: str, opensilex_client: OpenSILEXDirectAPI):
        self.csv_file_path = csv_file_path
        self.client = opensilex_client
        
        # Groupes disponibles - on utilisera celui spécifié ou "Variables Environnementales M3P"
        self.available_groups = {
            "Variables Environnementales M3P": "opensilex-sandbox:id/variablesGroup/variables_environnementales_m3p",
            "Variables Phénotypiques M3P": "opensilex-sandbox:id/variablesGroup/variables_phnotypiques_m3p",
        }
        
        # Groupe par défaut (il faut indiquer le groupe concerné)
        self.default_group = "ex : Variables Environnementales M3P"
    
    def read_csv(self) -> pd.DataFrame:
        """Lit le fichier CSV"""
        try:
            print(f" Lecture du fichier CSV: {self.csv_file_path}")
            
            # Le fichier est en UTF-8 d'après les métadonnées
            df = pd.read_csv(self.csv_file_path, encoding='utf-8')
            print(f" Fichier lu avec succès")
            print(f" {len(df)} variables trouvées")
            print(f" Colonnes: {list(df.columns)}")
            
            return df
            
        except Exception as e:
            print(f" Erreur lecture CSV: {e}")
            return pd.DataFrame()
    
    def validate_csv_structure(self, df: pd.DataFrame) -> bool:
        """Valide que le CSV a le format attendu"""
        required_columns = [
            'Variable_name', 'Variable_description', 'Entity_uri', 'Entity_name',
            'Characteristic_uri', 'Characteristic_name', 'Method_uri', 'Method_name',
            'Unit_uri', 'Unit_name', 'Datatype_uri'
        ]
        
        print("\n🔍 Validation de la structure CSV :")
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f" Colonnes manquantes: {missing_columns}")
            return False
        
        print(" Toutes les colonnes requises sont présentes")
        
        # Vérifier qu'il y a des données
        if df['Variable_name'].isnull().any():
            print(" Certaines variables n'ont pas de nom")
            return False
        
        print(" Structure CSV valide")
        return True
    
    def prepare_variable_data(self, row: pd.Series) -> Dict:
        """Prépare les données d'une variable pour l'API OpenSILEX"""
        
        # Données de base avec les URIs complètes
        variable_data = {
            "name": str(row['Variable_name']).strip(),
            "description": str(row['Variable_description']).strip() if pd.notna(row['Variable_description']) else f"Variable {row['Variable_name']}",
            "entity": str(row['Entity_uri']).strip(),
            "characteristic": str(row['Characteristic_uri']).strip(),
            "method": str(row['Method_uri']).strip(),
            "unit": str(row['Unit_uri']).strip(),
            "datatype": str(row['Datatype_uri']).strip()
        }
        
        # Ajouter l'URI de la variable si spécifiée
        if 'Variable_uri' in row and pd.notna(row['Variable_uri']) and str(row['Variable_uri']).strip():
            variable_data['uri'] = str(row['Variable_uri']).strip()
        
        # Ajouter le nom alternatif si spécifié
        if 'Variable_alternative_name' in row and pd.notna(row['Variable_alternative_name']) and str(row['Variable_alternative_name']).strip():
            variable_data['alternative_name'] = str(row['Variable_alternative_name']).strip()
        
        return variable_data
    
    def determine_target_group(self, row: pd.Series) -> str:
        """Détermine le groupe cible pour une variable"""
        # Priorité: Group1, puis Group2, puis défaut
        
        if 'Group1' in row and pd.notna(row['Group1']) and str(row['Group1']).strip():
            group1 = str(row['Group1']).strip()
            if group1 in self.available_groups:
                return group1
        
        if 'Group2' in row and pd.notna(row['Group2']) and str(row['Group2']).strip():
            group2 = str(row['Group2']).strip()
            if group2 in self.available_groups:
                return group2
        
        # Par défaut
        return self.default_group
    
    def import_variables(self) -> Dict[str, List[str]]:
        """Importe toutes les variables """
        print("\n Démarrage de l'import des variables ")
        
        # Lire le CSV
        df = self.read_csv()
        if df.empty:
            return {}
        
        # Valider la structure
        if not self.validate_csv_structure(df):
            return {}
        
        # Statistiques
        results = {group: [] for group in self.available_groups.keys()}
        stats = {
            'created': 0,
            'existing': 0,
            'failed': 0,
            'failed_names': []
        }
        
        print(f"\n Import de {len(df)} variables :")
        
        for index, row in df.iterrows():
            variable_name = str(row['Variable_name']).strip()
            print(f"\n   {index + 1}/{len(df)} - {variable_name}")
            
            # Déterminer le groupe cible
            target_group = self.determine_target_group(row)
            print(f"    Groupe cible: {target_group}")
            
            # Vérifier si la variable existe déjà
            existing_var = self.client.find_variable_by_name(variable_name)
            
            if existing_var:
                print(f"    Variable existe déjà: {existing_var['uri']}")
                results[target_group].append(existing_var['uri'])
                stats['existing'] += 1
                continue
            
            # Préparer les données de la variable
            try:
                variable_data = self.prepare_variable_data(row)
                
                # Créer la variable
                created_uri = self.client.create_variable(variable_data)
                
                if created_uri:
                    print(f"    Variable créée: {created_uri}")
                    results[target_group].append(created_uri)
                    stats['created'] += 1
                else:
                    print(f"    Échec création")
                    stats['failed'] += 1
                    stats['failed_names'].append(variable_name)
                    
            except Exception as e:
                print(f"    Erreur: {e}")
                stats['failed'] += 1
                stats['failed_names'].append(variable_name)
        
        # Résumé de l'import
        print(f"\n RÉSUMÉ DE L'IMPORT:")
        print(f"    Variables créées: {stats['created']}")
        print(f"    Variables existantes: {stats['existing']}")
        print(f"    Variables échouées: {stats['failed']}")
        
        if stats['failed_names']:
            print(f"    Variables échouées: {stats['failed_names']}")
        
        # Afficher la répartition par groupe
        print(f"\n RÉPARTITION PAR GROUPE:")
        for group_name, variable_uris in results.items():
            if variable_uris:
                print(f"   {group_name}: {len(variable_uris)} variables")
        
        return results
    
    def attach_to_groups(self, grouped_variables: Dict[str, List[str]]) -> bool:
        """Attache les variables aux groupes appropriés"""
        overall_success = True
        
        print(f"\n Attachement des variables aux groupes:")
        
        for group_name, variable_uris in grouped_variables.items():
            if not variable_uris:
                continue  # Passer les groupes vides
            
            group_uri = self.available_groups[group_name]
            print(f"\n Groupe: {group_name} ({len(variable_uris)} variables)")
            
            # Récupérer les variables déjà dans le groupe
            current_group = self.client.get_group_details(group_uri)
            existing_vars = []
            
            if current_group and 'variables' in current_group:
                existing_vars = [var['uri'] if isinstance(var, dict) else var 
                               for var in current_group['variables']]
                print(f"    Variables déjà présentes: {len(existing_vars)}")
            
            # Combiner sans doublons
            all_variable_uris = list(set(existing_vars + variable_uris))
            print(f"    Total après ajout: {len(all_variable_uris)}")
            
            # Mettre à jour le groupe
            success = self.client.update_variable_group(group_uri, all_variable_uris)
            
            if success:
                print(f"    Succès pour {group_name}")
                
                # Vérifier le résultat
                updated_group = self.client.get_group_details(group_uri)
                if updated_group and 'variables' in updated_group:
                    final_count = len(updated_group['variables'])
                    print(f"    Le groupe contient maintenant {final_count} variables")
            else:
                print(f"    Échec pour {group_name}")
                overall_success = False
        
        return overall_success


def main():
    """Fonction principale pour l'import """
    print(" Import de variables CSV vers OpenSILEX")
    print("=" * 60)
    
    # Utiliser directement votre chemin
    csv_file_path = "/home/variables"
    print(f" Fichier CSV: {csv_file_path}")
    
    # Vérifier que le fichier existe
    if not os.path.exists(csv_file_path):
        print(f" Fichier non trouvé: {csv_file_path}")
        return
    
    # Initialiser le client OpenSILEX
    print("\n Connexion à OpenSILEX...")
    client = OpenSILEXDirectAPI()
    
    if not client.token:
        print(" Impossible de se connecter à OpenSILEX")
        return
    
    # Initialiser l'importeur
    importer = VariableImporter(csv_file_path, client)
    
    # Importer les variables
    grouped_variables = importer.import_variables()
    
    # Vérifier qu'on a des variables à attacher
    total_variables = sum(len(vars) for vars in grouped_variables.values())
    
    if total_variables > 0:
        print(f"\n {total_variables} variables au total à attacher aux groupes")
        
        # Attacher aux groupes appropriés
        success = importer.attach_to_groups(grouped_variables)
        
        if success:
            print("\n Import terminé avec succès!")
        else:
            print("\n  Import terminé avec quelques erreurs d'attachement")
    else:
        print("\n Aucune variable à attacher")
    
    print("\n📋 GROUPES OPENSILEX DISPONIBLES:")
    for group_name, group_uri in importer.available_groups.items():
        group_details = client.get_group_details(group_uri)
        if group_details and 'variables' in group_details:
            var_count = len(group_details['variables'])
            print(f"   {group_name}: {var_count} variables")
        else:
            print(f"   {group_name}: 0 variables")


if __name__ == "__main__":
    main()