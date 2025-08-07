import pandas as pd
import requests
import json
from typing import List, Dict, Optional
import urllib.parse
import os

class OpenSILEXDirectAPI:
    """
    OpenSILEX Client
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
        
        # Automatic authentication
        self.authenticate()
    
    def authenticate(self) -> bool:
        """Authentication via REST API"""
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
                print(" Authentication successful")
                return True
            else:
                print(f" Authentication failed: {response.status_code}")
                return False
                
        except Exception as e:
            print(f" Authentication error: {e}")
            return False
    
    def create_variable(self, variable_data: Dict) -> Optional[str]:
        """Creates a new variable"""
        try:
            print(f"Attempting to create: {variable_data.get('name', 'Unknown')}")
            print(f"   Data: {json.dumps(variable_data, indent=2)}")
            
            response = requests.post(
                f"{self.base_url}/core/variables",
                headers=self.headers,
                json=variable_data
            )
            
            if response.status_code == 201:
                result = response.json()
                print(f"Success: {result['result']}")
                return result['result']
            else:
                print(f"    Error {response.status_code}")
                print(f"   Response: {response.text}")
                return None
                
        except Exception as e:
            print(f"    Exception: {e}")
            return None
    
    def find_variable_by_name(self, variable_name: str) -> Optional[Dict]:
        """Finds a variable by its exact name"""
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
            print(f" Variable search error: {e}")
            return None
    
    def get_group_details(self, group_uri: str) -> Optional[Dict]:
        """Retrieves group details"""
        try:
            encoded_uri = urllib.parse.quote(group_uri, safe='')
            response = requests.get(
                f"{self.base_url}/core/variables_group/{encoded_uri}",
                headers=self.headers
            )
            
            if response.status_code == 200:
                return response.json()['result']
            else:
                print(f" Group retrieval error: {response.status_code}")
                return None
                
        except Exception as e:
            print(f" Group details error: {e}")
            return None
    
    def update_variable_group(self, group_uri: str, variable_uris: List[str]) -> bool:
        """Updates a group with variables"""
        try:
            # Retrieve current group details
            current_group = self.get_group_details(group_uri)
            if not current_group:
                print(f" Unable to retrieve group {group_uri}")
                return False
            
            # Prepare update data
            update_data = {
                "uri": group_uri,
                "name": current_group.get("name", ""),
                "description": current_group.get("description", ""),
                "variables": variable_uris
            }
            
            # Add other fields if they exist
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
                print(f" Group update error: {response.status_code}")
                return False
                
        except Exception as e:
            print(f" API update error: {e}")
            return False


class VariableImporter:
    """
    Specialized importer for variables with the provided CSV format
    """
    
    def __init__(self, csv_file_path: str, opensilex_client: OpenSILEXDirectAPI):
        self.csv_file_path = csv_file_path
        self.client = opensilex_client
        
        # Available groups - we will use the specified one or "Variables Environnementales M3P"
        self.available_groups = {
            "Variables Environnementales M3P": "opensilex-sandbox:id/variablesGroup/variables_environnementales_m3p",
            "Variables Phénotypiques M3P": "opensilex-sandbox:id/variablesGroup/variables_phnotypiques_m3p",
        }
        
        # Default group (you need to specify the relevant group)
        self.default_group = "ex : Variables Environnementales M3P"
    
    def read_csv(self) -> pd.DataFrame:
        """Reads the CSV file"""
        try:
            print(f" Reading CSV file: {self.csv_file_path}")
            
            # The file is in UTF-8 according to metadata
            df = pd.read_csv(self.csv_file_path, encoding='utf-8')
            print(f" File read successfully")
            print(f" {len(df)} variables found")
            print(f" Columns: {list(df.columns)}")
            
            return df
            
        except Exception as e:
            print(f" CSV reading error: {e}")
            return pd.DataFrame()
    
    def validate_csv_structure(self, df: pd.DataFrame) -> bool:
        """Validates that the CSV has the expected format"""
        required_columns = [
            'Variable_name', 'Variable_description', 'Entity_uri', 'Entity_name',
            'Characteristic_uri', 'Characteristic_name', 'Method_uri', 'Method_name',
            'Unit_uri', 'Unit_name', 'Datatype_uri'
        ]
        
        print("\n🔍 CSV structure validation:")
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f" Missing columns: {missing_columns}")
            return False
        
        print(" All required columns are present")
        
        # Check that there is data
        if df['Variable_name'].isnull().any():
            print(" Some variables have no name")
            return False
        
        print(" CSV structure is valid")
        return True
    
    def prepare_variable_data(self, row: pd.Series) -> Dict:
        """Prepares variable data for the OpenSILEX API"""
        
        # Basic data with complete URIs
        variable_data = {
            "name": str(row['Variable_name']).strip(),
            "description": str(row['Variable_description']).strip() if pd.notna(row['Variable_description']) else f"Variable {row['Variable_name']}",
            "entity": str(row['Entity_uri']).strip(),
            "characteristic": str(row['Characteristic_uri']).strip(),
            "method": str(row['Method_uri']).strip(),
            "unit": str(row['Unit_uri']).strip(),
            "datatype": str(row['Datatype_uri']).strip()
        }
        
        # Add variable URI if specified
        if 'Variable_uri' in row and pd.notna(row['Variable_uri']) and str(row['Variable_uri']).strip():
            variable_data['uri'] = str(row['Variable_uri']).strip()
        
        # Add alternative name if specified
        if 'Variable_alternative_name' in row and pd.notna(row['Variable_alternative_name']) and str(row['Variable_alternative_name']).strip():
            variable_data['alternative_name'] = str(row['Variable_alternative_name']).strip()
        
        return variable_data
    
    def determine_target_group(self, row: pd.Series) -> str:
        """Determines the target group for a variable"""
        # Priority: Group1, then Group2, then default
        
        if 'Group1' in row and pd.notna(row['Group1']) and str(row['Group1']).strip():
            group1 = str(row['Group1']).strip()
            if group1 in self.available_groups:
                return group1
        
        if 'Group2' in row and pd.notna(row['Group2']) and str(row['Group2']).strip():
            group2 = str(row['Group2']).strip()
            if group2 in self.available_groups:
                return group2
        
        # Default
        return self.default_group
    
    def import_variables(self) -> Dict[str, List[str]]:
        """Imports all variables"""
        print("\n Starting variable import ")
        
        # Read CSV
        df = self.read_csv()
        if df.empty:
            return {}
        
        # Validate structure
        if not self.validate_csv_structure(df):
            return {}
        
        # Statistics
        results = {group: [] for group in self.available_groups.keys()}
        stats = {
            'created': 0,
            'existing': 0,
            'failed': 0,
            'failed_names': []
        }
        
        print(f"\n Importing {len(df)} variables:")
        
        for index, row in df.iterrows():
            variable_name = str(row['Variable_name']).strip()
            print(f"\n   {index + 1}/{len(df)} - {variable_name}")
            
            # Determine target group
            target_group = self.determine_target_group(row)
            print(f"    Target group: {target_group}")
            
            # Check if variable already exists
            existing_var = self.client.find_variable_by_name(variable_name)
            
            if existing_var:
                print(f"    Variable already exists: {existing_var['uri']}")
                results[target_group].append(existing_var['uri'])
                stats['existing'] += 1
                continue
            
            # Prepare variable data
            try:
                variable_data = self.prepare_variable_data(row)
                
                # Create variable
                created_uri = self.client.create_variable(variable_data)
                
                if created_uri:
                    print(f"    Variable created: {created_uri}")
                    results[target_group].append(created_uri)
                    stats['created'] += 1
                else:
                    print(f"    Creation failed")
                    stats['failed'] += 1
                    stats['failed_names'].append(variable_name)
                    
            except Exception as e:
                print(f"    Error: {e}")
                stats['failed'] += 1
                stats['failed_names'].append(variable_name)
        
        # Import summary
        print(f"\n IMPORT SUMMARY:")
        print(f"    Variables created: {stats['created']}")
        print(f"    Existing variables: {stats['existing']}")
        print(f"    Failed variables: {stats['failed']}")
        
        if stats['failed_names']:
            print(f"    Failed variables: {stats['failed_names']}")
        
        # Display distribution by group
        print(f"\n GROUP DISTRIBUTION:")
        for group_name, variable_uris in results.items():
            if variable_uris:
                print(f"   {group_name}: {len(variable_uris)} variables")
        
        return results
    
    def attach_to_groups(self, grouped_variables: Dict[str, List[str]]) -> bool:
        """Attaches variables to appropriate groups"""
        overall_success = True
        
        print(f"\n Attaching variables to groups:")
        
        for group_name, variable_uris in grouped_variables.items():
            if not variable_uris:
                continue  # Skip empty groups
            
            group_uri = self.available_groups[group_name]
            print(f"\n Group: {group_name} ({len(variable_uris)} variables)")
            
            # Retrieve variables already in the group
            current_group = self.client.get_group_details(group_uri)
            existing_vars = []
            
            if current_group and 'variables' in current_group:
                existing_vars = [var['uri'] if isinstance(var, dict) else var 
                               for var in current_group['variables']]
                print(f"    Already present variables: {len(existing_vars)}")
            
            # Combine without duplicates
            all_variable_uris = list(set(existing_vars + variable_uris))
            print(f"    Total after addition: {len(all_variable_uris)}")
            
            # Update group
            success = self.client.update_variable_group(group_uri, all_variable_uris)
            
            if success:
                print(f"    Success for {group_name}")
                
                # Verify result
                updated_group = self.client.get_group_details(group_uri)
                if updated_group and 'variables' in updated_group:
                    final_count = len(updated_group['variables'])
                    print(f"    Group now contains {final_count} variables")
            else:
                print(f"    Failed for {group_name}")
                overall_success = False
        
        return overall_success


def main():
    """Main function for import"""
    print(" CSV variable import to OpenSILEX")
    print("=" * 60)
    
    # Use your path directly
    csv_file_path = "/home/variables"
    print(f" CSV file: {csv_file_path}")
    
    # Check that file exists
    if not os.path.exists(csv_file_path):
        print(f" File not found: {csv_file_path}")
        return
    
    # Initialize OpenSILEX client
    print("\n Connecting to OpenSILEX...")
    client = OpenSILEXDirectAPI()
    
    if not client.token:
        print(" Unable to connect to OpenSILEX")
        return
    
    # Initialize importer
    importer = VariableImporter(csv_file_path, client)
    
    # Import variables
    grouped_variables = importer.import_variables()
    
    # Check that we have variables to attach
    total_variables = sum(len(vars) for vars in grouped_variables.values())
    
    if total_variables > 0:
        print(f"\n {total_variables} variables total to attach to groups")
        
        # Attach to appropriate groups
        success = importer.attach_to_groups(grouped_variables)
        
        if success:
            print("\n Import completed successfully!")
        else:
            print("\n  Import completed with some attachment errors")
    else:
        print("\n No variables to attach")
    
    print("\n📋 AVAILABLE OPENSILEX GROUPS:")
    for group_name, group_uri in importer.available_groups.items():
        group_details = client.get_group_details(group_uri)
        if group_details and 'variables' in group_details:
            var_count = len(group_details['variables'])
            print(f"   {group_name}: {var_count} variables")
        else:
            print(f"   {group_name}: 0 variables")


if __name__ == "__main__":
    main()