'''
#!/usr/bin/env python3
# -*- coding:utf-8 -*-
******************************************************************************
                                    Client.py
 OpenSILEX
 Copyright © INRAE 2021
 Creation date:  22 April, 2021
 Contact: arnaud.charleroy@inrae.fr, anne.tireau@inrae.fr, pascal.neveu@inrae.fr
******************************************************************************
'''
 
import opensilexClientToolsPython
import os
import logging
from functions import *

# Migration
# usage of a created instance of the API class
# ### V1.0
pythonClient = opensilexClientToolsPython.ApiClient()
pythonClient.connect_to_opensilex_ws(
    identifier="admin@opensilex.org", password="admin", host="http://localhost:8666/rest")
logging.info("Headers and token : " + str(pythonClient.default_headers) + '\n')

# Functions

# Import variable from csv and googlesheet
# Googlesheet format example : https://docs.google.com/spreadsheets/d/1tjnKPQGp37Xd8d2SFMBQGR1NCzBO4iOSk05QjVgiHa8/edit?usp=sharing
# Googlesheet url exemple: https://docs.google.com/spreadsheets/d/1tjnKPQGp37Xd8d2SFMBQGR1NCzBO4iOSk05QjVgiHa8/edit#gid=0
spreadsheet_url = "https://docs.google.com/spreadsheets/d/1mqmXkSekWhdde7Ic4baiw9R3jSkS90CwTuQbZoaUIVk"
gid_number = "0"

migrate_variables_from_googlesheet(pythonClient, None, spreadsheet_url, gid_number, False)
exit()
# with specific header
# Header Key => "csv Name"
# configVariableHeaders = {
#         "entityLabel": "entity.label", 
#         "entityUri": "entity.uri",
#         "entityComment": "entity.comment",
#         "characteristicLabel": "characteristic.label",
#         "characteristicUri": "characteristic.uri",
#         "characteristicComment": "characteristic.comment",
#         "methodUri": "method.uri",
#         "methodLabel": "method.label",
#         "methodComment": "method.comment",
#         "unitUri": "unit.uri",
#         "unitLabel": "unit.label",
#         "unitComment": "unit.comment",
#         "traitUri": "trait.uri",
#         "traitLabel": "trait.label",
#         "variableUri": "uri",
#         "variableLabel": "label",
#         "variableComment": "comment",
#         "dataType": "datatype",
#         "alternativeLabel": "alternativeName"
#     }

# Import variables from googlesheet 
# You can defined row columns with the third argument configVariableHeaders
# Uncomment following lines to execute 
# migrate_variables_from_googlesheet(pythonClient, None, spreadsheet_url, gid_number, False)

# Import variables from csv
# Uncomment following lines to execute 
# csvVariablePath = os.path.join(os.path.abspath(os.path.dirname(__file__)),"csv_example","variables.csv")
# migrate_variables_from_csv(pythonClient, None, csvVariablePath, False)

# ############ # ############ # ############ # ############ # ############ 
# ############# ############   EXPERIMENTAL  # ############ # ############ 
# ############ # ############ # ############ # ############ # ############   
# create an experiment
expeTocreate = {
   "uri": "http://www.opensilex.org/demo/Formation-2021",
   "name": "Formation2021",
   "objective": "For teaching purpose",
   "start_date": "2021-04-27",
   "end_date": "2021-04-27",
   "species": ["http://aims.fao.org/aos/agrovoc/c_12332"],
   "technical_supervisors": [],
   "scientific_supervisors": [],
   "projects": [],
   "is_public": True,
   "description": "Training OpenSILEX",
   "groups": []
}
# uri = create_experiment(pythonClient,expeTocreate)

# Uncomment to execute
# uri = create_experiment(pythonClient, expe) 

provenancesToSend = [{
    "name": "ENVIRONMENTAL PROVENANCE DEMO - TRAINING",
    "description": "Collected by several sensors",
    "prov_agent": [
        {
            "uri": "http://www.opensilex.org/demo/set/devices/station-demo-sensor",
            "rdf_type": "vocabulary:SensingDevice",
            "settings": {}
        }
    ]
}, {
    'uri': "http://www.opensilex.org/demo/id/provenance/phenotyping-provenance-demo",
    "name": "PHENOTYPING PROVENANCE DEMO - TRAINING",
    "description": "Collected by an operator",
}]
# Uncomment to execute
create_provenances(pythonClient,provenancesToSend )
 

sensor = {"uri": "http://www.opensilex.org/demo/set/devices/station-demo-sensor", "name": "Demo sensor", "type": "vocabulary:Station",
          "serial_number": "8173803780", "description": "Weather station"}
# Uncomment to execute
# create_sensor(pythonClient,sensor)


# http: // www.opensilex.org/demo/DIA2017-1
# csvFileName = "/home/charlero/savedisk/Projets/OpenSILEX/Presentation/Objets/objetsZA17.csv"

# In csv_example dir : "csv_example/objects.csv"
# Uncomment to execute
# csvObjectsPath = os.path.join(os.path.abspath(os.path.dirname(__file__)),"csv_example","data.csv")
# create_objects_from_csv(pythonClient, csvObjectsPath)
spreadsheet_url = "https://docs.google.com/spreadsheets/d/1tjnKPQGp37Xd8d2SFMBQGR1NCzBO4iOSk05QjVgiHa8"
gid_number = "1931021280"
# Uncomment to execute
#create_objects_from_googlesheet(pythonClient,spreadsheet_url,gid_number)
#update_objects_from_googlesheet(pythonClient,spreadsheet_url,gid_number)


# In csv_example dir : "csv_example/data.csv"
csvDataPath = os.path.join(os.path.abspath(os.path.dirname(__file__)),"csv_example","data.csv")
# Uncomment to execute
# add_data_from_csv(pythonClient, csvDataPath)

spreadsheet_url = "https://docs.google.com/spreadsheets/d/1tjnKPQGp37Xd8d2SFMBQGR1NCzBO4iOSk05QjVgiHa8"
gid_number = "2145718207" 
# Uncomment to execute
add_data_from_googlesheet(pythonClient,spreadsheet_url,gid_number)