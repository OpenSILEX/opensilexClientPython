from __future__ import print_function  
import opensilexClientToolsPython
from opensilexClientPython.os_extraction_data import os_extraction_data
from opensilexClientToolsPython.rest import ApiException
import pandas as pd
from pprint import pprint

# Define credentials and experiment
host = "https://localhost/rest"
identifiant = "guest@opensilex.org"
mdp = "guest"
uri_expe = "http://uri.com/"

# Create an instance of the API class
pythonClient = opensilexClientToolsPython.ApiClient()
pythonClient.connect_to_opensilex_ws(identifier=identifiant, password=mdp, host=host)

try:
    # Call your custom function os_extraction_data
    os_data = os_extraction_data(uri_expe, identifiant, mdp, host)
    
    # Example: Get data for a specific OS type (e.g., "plant")
    if "plant" in os_data:
        df_plant = os_data["plant"]
        
        # Display the first few rows of the plant data
        pprint(df_plant.head())

        # Save the plant data to a CSV file
        csv_file_path = "./plant_data.csv"
        df_plant.to_csv(csv_file_path, index=False)

        print(f"Data has been saved to {csv_file_path}")
    else:
        print("No data found for Plant objects.")
        
except ApiException as e:
    print("Exception when calling DataApi: %s\n" % e)
except Exception as e:
    print("General exception: %s\n" % e)
