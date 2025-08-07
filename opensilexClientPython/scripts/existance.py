#!/usr/bin/env python3
"""
Script pour vérifier si une variable existe dans OpenSILEX
en utilisant la combinaison Entity + Characteristic + Method + Unit
avec correspondance EXACTE
"""

import requests
import json
import sys
import pandas as pd
import csv
from typing import Optional, Dict, Any, List
from urllib.parse import quote

class OpenSILEXVariableChecker:
    def __init__(self, base_url: str, token: Optional[str] = None):
        """
        Initialise le vérificateur de variables OpenSILEX
        
        Args:
            base_url: URL de base de l'instance OpenSILEX
            token: Token d'authentification (optionnel)
        """
        self.base_url = base_url.rstrip('/')
        self.token = token
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        if token:
            self.headers['Authorization'] = f'Bearer {token}'
    
    def authenticate(self, username: str, password: str) -> bool:
        """Authentification auprès d'OpenSILEX"""
        auth_url = f"{self.base_url}/security/authenticate"
        auth_data = {
            "identifier": username,
            "password": password
        }
        
        try:
            response = requests.post(auth_url, json=auth_data)
            if response.status_code == 200:
                result = response.json()
                self.token = result.get('result', {}).get('token')
                if self.token:
                    self.headers['Authorization'] = f'Bearer {self.token}'
                    print("✅ Authentification réussie")
                    return True
            print(f"❌ Échec de l'authentification: {response.status_code}")
            return False
        except requests.RequestException as e:
            print(f"❌ Erreur de connexion lors de l'authentification: {e}")
            return False
    
    def search_variables_by_criteria(self, entity_uri: str = None, characteristic_uri: str = None, 
                                   method_uri: str = None, unit_uri: str = None,
                                   entity_name: str = None, characteristic_name: str = None,
                                   method_name: str = None, unit_name: str = None) -> List[Dict]:
        """
        Recherche des variables par critères (URI ou noms)
        """
        search_url = f"{self.base_url}/core/variables"
        params = {}
        
        # Utilise les URI en priorité, sinon les noms
        if entity_uri:
            params['entity'] = entity_uri
        if characteristic_uri:
            params['characteristic'] = characteristic_uri
        if method_uri:
            params['method'] = method_uri
        if unit_uri:
            params['unit'] = unit_uri
        
        try:
            response = requests.get(search_url, headers=self.headers, params=params)
            if response.status_code == 200:
                result = response.json()
                variables = result.get('result', [])
                
                # Filtrage supplémentaire par noms si pas d'URI
                if not any([entity_uri, characteristic_uri, method_uri, unit_uri]) and variables:
                    filtered = []
                    for var in variables:
                        match = True
                        if entity_name and entity_name.lower() not in str(var.get('entity', {}).get('name', '')).lower():
                            match = False
                        if characteristic_name and characteristic_name.lower() not in str(var.get('characteristic', {}).get('name', '')).lower():
                            match = False
                        if method_name and method_name.lower() not in str(var.get('method', {}).get('name', '')).lower():
                            match = False
                        if unit_name and unit_name.lower() not in str(var.get('unit', {}).get('name', '')).lower():
                            match = False
                        if match:
                            filtered.append(var)
                    return filtered
                
                return variables
            else:
                print(f"❌ Erreur lors de la recherche: {response.status_code}")
                return []
        except requests.RequestException as e:
            print(f"❌ Erreur de connexion: {e}")
            return []
    
    def search_variable_by_name(self, variable_name: str) -> List[Dict]:
        """Recherche des variables par nom"""
        search_url = f"{self.base_url}/core/variables"
        params = {'name': variable_name}
        
        try:
            response = requests.get(search_url, headers=self.headers, params=params)
            if response.status_code == 200:
                result = response.json()
                return result.get('result', [])
            return []
        except requests.RequestException as e:
            print(f"❌ Erreur de connexion: {e}")
            return []
    
    def check_variables_from_csv(self, csv_file_path: str, output_file: Optional[str] = None) -> Dict[str, Any]:
        """
        Vérifie l'existence de variables depuis un fichier CSV
        en utilisant Entity + Characteristic + Method + Unit avec correspondance EXACTE
        """
        results = {
            'found': [],
            'not_found': [],
            'errors': [],
            'total': 0,
            'found_count': 0,
            'not_found_count': 0,
            'error_count': 0
        }
        
        try:
            # Lecture du fichier CSV
            df = pd.read_csv(csv_file_path)
            
            # Vérification des colonnes nécessaires
            required_cols = ['Entity_name', 'Characteristic_name', 'Method_name', 'Unit_name']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                print(f"❌ Colonnes manquantes dans le fichier CSV: {missing_cols}")
                return results
            
            # Filtrer les lignes avec des données valides
            df_valid = df.dropna(subset=required_cols)
            
            results['total'] = len(df_valid)
            print(f"📂 Lecture de {results['total']} variables depuis {csv_file_path}")
            print("🔍 Vérification en cours (correspondance EXACTE requise)...\n")
            
            for index, row in df_valid.iterrows():
                entity_name = str(row['Entity_name']).strip()
                characteristic_name = str(row['Characteristic_name']).strip()
                method_name = str(row['Method_name']).strip()
                unit_name = str(row['Unit_name']).strip()
                variable_name = str(row.get('Variable_name', 'Sans nom'))
                
                print(f"[{index + 1}/{results['total']}] Vérification: {variable_name}")
                print(f"    Entity: {entity_name}")
                print(f"    Characteristic: {characteristic_name}")
                print(f"    Method: {method_name}")
                print(f"    Unit: {unit_name}")
                
                try:
                    row_data = {
                        'csv_index': index,
                        'variable_name': variable_name,
                        'entity_name': entity_name,
                        'characteristic_name': characteristic_name,
                        'method_name': method_name,
                        'unit_name': unit_name,
                        'entity_uri': str(row.get('Entity_uri', '')),
                        'characteristic_uri': str(row.get('Characteristic_uri', '')),
                        'method_uri': str(row.get('Method_uri', '')),
                        'unit_uri': str(row.get('Unit_uri', ''))
                    }
                    
                    # Recherche par URIs si disponibles, sinon par noms
                    matching_variables = []
                    
                    # Première tentative avec les URIs
                    if (row_data['entity_uri'] and row_data['characteristic_uri'] and 
                        row_data['method_uri'] and row_data['unit_uri']):
                        matching_variables = self.search_variables_by_criteria(
                            entity_uri=row_data['entity_uri'],
                            characteristic_uri=row_data['characteristic_uri'],
                            method_uri=row_data['method_uri'],
                            unit_uri=row_data['unit_uri']
                        )
                    
                    # Deuxième tentative par nom de variable
                    if not matching_variables and variable_name != 'Sans nom':
                        matching_variables = self.search_variable_by_name(variable_name)
                    
                    # Troisième tentative : recherche générale et filtrage exact
                    if not matching_variables:
                        print(f"   🔍 Recherche avec correspondance EXACTE des composants...")
                        all_variables = self.search_variables_by_criteria()
                        matching_variables = self._filter_variables_by_components(
                            all_variables, entity_name, characteristic_name, method_name, unit_name
                        )
                    
                    if matching_variables:
                        # Avec correspondance exacte, tous les résultats sont équivalents
                        # On prend le premier (ils ont tous un score de 1.0)
                        best_match = matching_variables[0]
                        
                        results['found'].append({
                            'csv_data': row_data,
                            'opensilex_data': best_match,
                            'match_score': 1.0,  # Score parfait pour correspondance exacte
                            'total_matches': len(matching_variables)
                        })
                        results['found_count'] += 1
                        print(f"   ✅ Variable trouvée avec correspondance EXACTE! ({len(matching_variables)} correspondance(s))")
                        print(f"       Nom OpenSILEX: {best_match.get('name', 'Sans nom')}")
                        print(f"       URI: {best_match.get('uri', 'Sans URI')}")
                    else:
                        results['not_found'].append({
                            'csv_data': row_data
                        })
                        results['not_found_count'] += 1
                        print(f"   ❌ Aucune variable avec correspondance EXACTE trouvée")
                        print(f"       (Entity + Characteristic + Method + Unit doivent être identiques)")
                        
                except Exception as e:
                    error_data = {
                        'csv_data': row_data,
                        'error': str(e)
                    }
                    results['errors'].append(error_data)
                    results['error_count'] += 1
                    print(f"   ⚠️ Erreur: {e}")
                
                print()  # Ligne vide pour la lisibilité
            
            # Sauvegarde des résultats
            if output_file:
                self._save_csv_results_to_file(results, output_file)
                
            return results
            
        except FileNotFoundError:
            print(f"❌ Fichier non trouvé: {csv_file_path}")
            return results
        except Exception as e:
            print(f"❌ Erreur lors de la lecture du fichier CSV: {e}")
            return results
    
    def _filter_variables_by_components(self, variables: List[Dict], entity_name: str, 
                                      characteristic_name: str, method_name: str, unit_name: str) -> List[Dict]:
        """Filtre les variables par correspondance EXACTE des composants"""
        filtered = []
        
        # Normalisation des noms pour comparaison exacte (suppression espaces, casse)
        entity_clean = entity_name.strip().lower()
        char_clean = characteristic_name.strip().lower()
        method_clean = method_name.strip().lower()
        unit_clean = unit_name.strip().lower()
        
        for var in variables:
            # Récupération des noms depuis OpenSILEX
            var_entity = var.get('entity', {}).get('name', '').strip().lower()
            var_char = var.get('characteristic', {}).get('name', '').strip().lower()
            var_method = var.get('method', {}).get('name', '').strip().lower()
            var_unit = var.get('unit', {}).get('name', '').strip().lower()
            
            # Correspondance EXACTE requise pour tous les composants
            entity_match = (entity_clean == var_entity)
            char_match = (char_clean == var_char)
            method_match = (method_clean == var_method)
            unit_match = (unit_clean == var_unit)
            
            # TOUS les composants doivent correspondre exactement
            if entity_match and char_match and method_match and unit_match:
                var['_match_score'] = 1.0  # Score parfait pour correspondance exacte
                filtered.append(var)
                print(f"       ✅ Correspondance exacte trouvée:")
                print(f"          Entity: '{var.get('entity', {}).get('name', '')}' = '{entity_name}'")
                print(f"          Characteristic: '{var.get('characteristic', {}).get('name', '')}' = '{characteristic_name}'")
                print(f"          Method: '{var.get('method', {}).get('name', '')}' = '{method_name}'")
                print(f"          Unit: '{var.get('unit', {}).get('name', '')}' = '{unit_name}'")
        
        return filtered
    
    def _exact_match(self, text1: str, text2: str) -> bool:
        """Vérifie une correspondance exacte entre deux textes (ignorer casse et espaces)"""
        if not text1 or not text2:
            return False
        return text1.strip().lower() == text2.strip().lower()
    
    def _calculate_match_score(self, variable: Dict, entity_name: str, characteristic_name: str, 
                             method_name: str, unit_name: str) -> float:
        """Calcule un score de correspondance pour une variable (maintenant uniquement 0 ou 1)"""
        entity_score = 1.0 if self._exact_match(entity_name, variable.get('entity', {}).get('name', '')) else 0.0
        char_score = 1.0 if self._exact_match(characteristic_name, variable.get('characteristic', {}).get('name', '')) else 0.0
        method_score = 1.0 if self._exact_match(method_name, variable.get('method', {}).get('name', '')) else 0.0
        unit_score = 1.0 if self._exact_match(unit_name, variable.get('unit', {}).get('name', '')) else 0.0
        
        # Retourne 1.0 seulement si TOUS les composants correspondent exactement
        total_score = (entity_score + char_score + method_score + unit_score) / 4
        return 1.0 if total_score == 1.0 else 0.0
    
    def _save_csv_results_to_file(self, results: Dict[str, Any], output_file: str):
        """Sauvegarde les résultats dans des fichiers"""
        base_name = output_file.rsplit('.', 1)[0] if '.' in output_file else output_file
        
        # Sauvegarde JSON complète
        json_file = f"{base_name}_complete.json"
        try:
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            print(f"💾 Résultats complets sauvegardés dans: {json_file}")
        except Exception as e:
            print(f"❌ Erreur lors de la sauvegarde JSON: {e}")
        
        # Sauvegarde CSV résumé
        csv_file = f"{base_name}_summary.csv"
        try:
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'Variable_Name_CSV', 'Entity_Name_CSV', 'Characteristic_Name_CSV', 
                    'Method_Name_CSV', 'Unit_Name_CSV', 'Variable_Name_OpenSILEX',
                    'Entity_Name_OpenSILEX', 'Characteristic_Name_OpenSILEX',
                    'Method_Name_OpenSILEX', 'Unit_Name_OpenSILEX', 'Match_Score',
                    'Status', 'Notes'
                ])
                
                # Variables trouvées
                for item in results['found']:
                    csv_data = item['csv_data']
                    opensilex_data = item['opensilex_data']
                    writer.writerow([
                        csv_data['variable_name'],
                        csv_data['entity_name'],
                        csv_data['characteristic_name'],
                        csv_data['method_name'],
                        csv_data['unit_name'],
                        opensilex_data.get('name', ''),
                        opensilex_data.get('entity', {}).get('name', ''),
                        opensilex_data.get('characteristic', {}).get('name', ''),
                        opensilex_data.get('method', {}).get('name', ''),
                        opensilex_data.get('unit', {}).get('name', ''),
                        f"{item['match_score']:.2f}",
                        'FOUND',
                        f"Variable trouvée avec {item['total_matches']} correspondance(s)"
                    ])
                
                # Variables non trouvées
                for item in results['not_found']:
                    csv_data = item['csv_data']
                    writer.writerow([
                        csv_data['variable_name'],
                        csv_data['entity_name'],
                        csv_data['characteristic_name'],
                        csv_data['method_name'],
                        csv_data['unit_name'],
                        '', '', '', '', '', '0.00',
                        'NOT_FOUND',
                        'Aucune variable correspondante trouvée'
                    ])
                
                # Erreurs
                for item in results['errors']:
                    csv_data = item['csv_data']
                    writer.writerow([
                        csv_data['variable_name'],
                        csv_data['entity_name'],
                        csv_data['characteristic_name'],
                        csv_data['method_name'],
                        csv_data['unit_name'],
                        '', '', '', '', '', '0.00',
                        'ERROR',
                        f"Erreur: {item['error']}"
                    ])
            
            print(f"💾 Résumé CSV sauvegardé dans: {csv_file}")
        except Exception as e:
            print(f"❌ Erreur lors de la sauvegarde CSV: {e}")
        
        # Sauvegarde spéciale : Variables trouvées avec leurs URIs
        self._save_found_variables_with_uris(results, base_name)
    
    def _save_found_variables_with_uris(self, results: Dict[str, Any], base_name: str):
        """Sauvegarde uniquement les variables trouvées avec leurs URIs"""
        
        # Fichier CSV pour les variables trouvées avec URIs
        found_csv_file = f"{base_name}_variables_trouvees.csv"
        try:
            with open(found_csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'Variable_Name_CSV',
                    'Variable_Name_OpenSILEX', 
                    'Variable_URI',
                    'Entity_Name',
                    'Entity_URI',
                    'Characteristic_Name', 
                    'Characteristic_URI',
                    'Method_Name',
                    'Method_URI',
                    'Unit_Name',
                    'Unit_URI',
                    'Variable_Description',
                    'Match_Score',
                    'Total_Matches'
                ])
                
                for item in results['found']:
                    csv_data = item['csv_data']
                    opensilex_data = item['opensilex_data']
                    
                    writer.writerow([
                        csv_data['variable_name'],
                        opensilex_data.get('name', ''),
                        opensilex_data.get('uri', ''),
                        opensilex_data.get('entity', {}).get('name', ''),
                        opensilex_data.get('entity', {}).get('uri', ''),
                        opensilex_data.get('characteristic', {}).get('name', ''),
                        opensilex_data.get('characteristic', {}).get('uri', ''),
                        opensilex_data.get('method', {}).get('name', ''),
                        opensilex_data.get('method', {}).get('uri', ''),
                        opensilex_data.get('unit', {}).get('name', ''),
                        opensilex_data.get('unit', {}).get('uri', ''),
                        opensilex_data.get('description', ''),
                        f"{item['match_score']:.2f}",
                        item['total_matches']
                    ])
            
            print(f"🎯 Variables trouvées avec URIs sauvegardées dans: {found_csv_file}")
            print(f"   ✅ {len(results['found'])} variables avec leurs URIs complètes")
            
        except Exception as e:
            print(f"❌ Erreur lors de la sauvegarde des variables trouvées: {e}")
        
        # Fichier JSON simple pour les variables trouvées
        found_json_file = f"{base_name}_variables_trouvees.json"
        try:
            found_variables = []
            for item in results['found']:
                csv_data = item['csv_data']
                opensilex_data = item['opensilex_data']
                
                found_variables.append({
                    'variable_name_csv': csv_data['variable_name'],
                    'variable_name_opensilex': opensilex_data.get('name', ''),
                    'variable_uri': opensilex_data.get('uri', ''),
                    'entity': {
                        'name': opensilex_data.get('entity', {}).get('name', ''),
                        'uri': opensilex_data.get('entity', {}).get('uri', '')
                    },
                    'characteristic': {
                        'name': opensilex_data.get('characteristic', {}).get('name', ''),
                        'uri': opensilex_data.get('characteristic', {}).get('uri', '')
                    },
                    'method': {
                        'name': opensilex_data.get('method', {}).get('name', ''),
                        'uri': opensilex_data.get('method', {}).get('uri', '')
                    },
                    'unit': {
                        'name': opensilex_data.get('unit', {}).get('name', ''),
                        'uri': opensilex_data.get('unit', {}).get('uri', '')
                    },
                    'description': opensilex_data.get('description', ''),
                    'match_score': item['match_score'],
                    'total_matches': item['total_matches']
                })
            
            with open(found_json_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'summary': {
                        'total_found': len(found_variables),
                        'generated_at': pd.Timestamp.now().isoformat()
                    },
                    'variables': found_variables
                }, f, indent=2, ensure_ascii=False)
            
            print(f"🎯 Variables trouvées JSON sauvegardées dans: {found_json_file}")
            
        except Exception as e:
            print(f"❌ Erreur lors de la sauvegarde JSON des variables trouvées: {e}")
        
        # Fichier texte simple avec juste les URIs des variables
        uris_file = f"{base_name}_uris_variables.txt"
        try:
            with open(uris_file, 'w', encoding='utf-8') as f:
                f.write("# URIs des variables trouvées dans OpenSILEX\n")
                f.write(f"# Généré le {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"# Total: {len(results['found'])} variables\n\n")
                
                for item in results['found']:
                    opensilex_data = item['opensilex_data']
                    variable_uri = opensilex_data.get('uri', '')
                    variable_name = opensilex_data.get('name', 'Sans nom')
                    if variable_uri:
                        f.write(f"{variable_uri}  # {variable_name}\n")
            
            print(f"📝 Liste des URIs sauvegardée dans: {uris_file}")
            
        except Exception as e:
            print(f"❌ Erreur lors de la sauvegarde des URIs: {e}")
    
    def print_csv_summary(self, results: Dict[str, Any]):
        """Affiche un résumé des résultats"""
        print("\n" + "="*80)
        print("📊 RÉSUMÉ DE LA VÉRIFICATION")
        print("="*80)
        print(f"Total variables dans le CSV: {results['total']}")
        print(f"✅ Trouvées dans OpenSILEX: {results['found_count']} ({results['found_count']/results['total']*100:.1f}%)")
        print(f"❌ Non trouvées: {results['not_found_count']} ({results['not_found_count']/results['total']*100:.1f}%)")
        print(f"⚠️ Erreurs: {results['error_count']} ({results['error_count']/results['total']*100:.1f}%)")
        
        if results['found']:
            print(f"\n✅ Variables trouvées avec correspondance EXACTE:")
            for i, item in enumerate(results['found'][:5], 1):
                csv_data = item['csv_data']
                opensilex_data = item['opensilex_data']
                print(f"   {i}. {csv_data['variable_name']} -> {opensilex_data.get('name', 'Sans nom')}")
                print(f"      URI: {opensilex_data.get('uri', 'Sans URI')}")
        
        if results['not_found']:
            print(f"\n❌ Variables sans correspondance exacte:")
            for i, item in enumerate(results['not_found'][:5], 1):
                csv_data = item['csv_data']
                print(f"   {i}. {csv_data['variable_name']}")
                print(f"      Recherchait: {csv_data['entity_name']} + {csv_data['characteristic_name']} + {csv_data['method_name']} + {csv_data['unit_name']}")
        
        print(f"\n💡 Note: Seules les variables avec correspondance EXACTE (Entity + Characteristic + Method + Unit) sont considérées comme trouvées.")
        print("="*80)

def main():
    """Fonction principale"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Vérificateur de variables OpenSILEX')
    parser.add_argument('--csv', '-c', help='Fichier CSV contenant les variables à vérifier')
    parser.add_argument('--output', '-o', help='Nom de base pour les fichiers de sortie')
    
    args = parser.parse_args()
    
    # Configuration avec vos paramètres
    BASE_URL = "https://phenome.inrae.fr/resources/rest"
    USERNAME = "admin@opensilex.org"
    PASSWORD = "phisc1.61"
    CSV_PATH = "/home/jemaa/Téléchargements/Variables_Pheno_Mais_2025 - Sheet1 (3).csv"
    
    # Initialisation du vérificateur
    checker = OpenSILEXVariableChecker(BASE_URL)
    
    # Authentification
    print("🔐 Authentification en cours...")
    if not checker.authenticate(USERNAME, PASSWORD):
        print("❌ Échec de l'authentification. Vérifiez vos identifiants.")
        sys.exit(1)
    
    # Utilise le fichier spécifié ou celui par défaut
    csv_file = args.csv if args.csv else CSV_PATH
    output_base = args.output if args.output else "resultats_variables"
    
    print(f"\n📋 Vérification des variables depuis: {csv_file}")
    results = checker.check_variables_from_csv(csv_file, output_base)
    checker.print_csv_summary(results)

if __name__ == "__main__":
    main()