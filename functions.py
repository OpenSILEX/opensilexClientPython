
from __future__ import print_function
import time
import opensilexClientToolsPython
from pprint import pprint
import requests
import re
import io
import pandas as pd
from dateparser import parse
import logging
from functions import *

logging.basicConfig(level=logging.INFO, handlers=[
    logging.FileHandler("debug.log"),
    logging.StreamHandler()
],format='%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s')


def create_experiment(experiment, pythonClient):
    results = {}
    experiment_os_api = opensilexClientToolsPython.ExperimentsApi(pythonClient)

    new_expriment = opensilexClientToolsPython.ExperimentCreationDTO(
        uri=experiment["uri"],
        name=experiment["name"],
        objective=experiment["objective"],
        start_date=experiment["start_date"],
        end_date=experiment["end_date"],
        technical_supervisors=experiment["technical_supervisors"],
        scientific_supervisors=experiment["scientific_supervisors"],
        variables=[],
        organisations=[],
        projects=experiment["projects"],
        is_public=experiment["is_public"],
        description=experiment["description"],
        groups=experiment["groups"],
        species=experiment["species"]
    )
    try:
        experiment_result = experiment_os_api.create_experiment(
            body=new_expriment)
        experiment["uri"] = experiment_result.get("result")[0]
        logging.info("new experiment" + experiment["name"] + " created")
        return experiment
    except Exception as e:
        if "exists" not in str(e):
            logging.error("Exception : %s\n" % e)
            exit()
        else:
            logging.info("experiment " + experiment["name"] + " exists")


def isEmpty(raw_value):
    value = str(raw_value)
    if(value == "NA" or value == None or value == "nan"):
        return True
    return False

def format_comment(raw_comment):
    comment = str(raw_comment) 
    if(isEmpty(comment)):
        return "No description"

    return raw_comment


def migrate_variables_from_googlesheet(pythonClient, configVariableHeaders, spreadsheet_url, gid_number):
    variables_url = spreadsheet_url + "/gviz/tq?tqx=out:csv&gid=" + str(gid_number)
    r = requests.get(variables_url).content
    variablesCsvString = requests.get(variables_url).content
    variablesCSV = pd.read_csv(io.StringIO(variablesCsvString.decode('utf-8')))
    return migrate_variables(pythonClient, configVariableHeaders, variablesCSV)

def migrate_variables_from_csv(pythonClient, configVariableHeaders, csv_path):
    variablesCSV = pd.read_csv(csv_path)
    return migrate_variables(pythonClient, configVariableHeaders, variablesCSV)

def migrate_variables(pythonClient, configVariableHeaders,variablesCSV):
    default_config = {
        "entityLabel": "entity.label",
        "entityUri": "entity.uri",
        "entityComment": "entity.comment",
        "characteristicLabel": "characteristic.label",
        "characteristicUri": "characteristic.uri",
        "characteristicComment": "characteristic.comment",
        "methodUri": "method.uri",
        "methodLabel": "method.label",
        "methodComment": "method.comment",
        "unitUri": "unit.uri",
        "unitLabel": "unit.label",
        "unitComment": "unit.comment",
        "traitUri": "trait.uri",
        "traitLabel": "trait.label",
        "variableUri": "uri",
        "variableLabel": "label",
        "variableComment": "comment",
        "dataType": "datatype",
        "alternativeLabel": "alternativeName"
    }
    
    if configVariableHeaders is None :
        configVariableHeaders = default_config

    csvColumns = variablesCSV.columns

    if not {configVariableHeaders['entityLabel'],
            configVariableHeaders['characteristicLabel'],
            configVariableHeaders['methodLabel'],
            configVariableHeaders['unitLabel'],
            configVariableHeaders['variableLabel'],
            configVariableHeaders['dataType']}.issubset(csvColumns):
        logging.error("Missing mandatory - variable columns '" )
        logging.info(configVariableHeaders['entityLabel'] + "' '" +
            configVariableHeaders['characteristicLabel'] + "' '" +
            configVariableHeaders['methodLabel'] + "' " +
            configVariableHeaders['unitLabel'] + "' '" +
            configVariableHeaders['variableLabel'] + "' '" +
            configVariableHeaders['dataType'] + "' are mandatory columns")
        exit()    

    variable_os_api = opensilexClientToolsPython.VariablesApi(pythonClient)
   
    totalCount = len(variablesCSV.index)

    entities = {} 

    characteristics = {} 

    methods = {} 

    units = {} 

    nbEntities = 0

    

    for index, row in variablesCSV.iterrows():
        try: 
            if configVariableHeaders['entityLabel'] in csvColumns:
                entityUri = None
                if configVariableHeaders['entityUri'] in csvColumns:
                    entityUri = row[configVariableHeaders['entityUri']]
                if entityUri is not None and entityUri not in entities:
                    if row[configVariableHeaders['entityLabel']] not in entities.values(): 
                        description = None
                        if configVariableHeaders['entityComment'] in csvColumns:
                            description = format_comment(row[configVariableHeaders['entityComment']])
                        else:
                            description = "No description"
                        entity = opensilexClientToolsPython.EntityCreationDTO(
                            uri=entityUri,
                            name=row[configVariableHeaders['entityLabel']],
                            description= description)
                        try:
                            result = variable_os_api.create_entity(body=entity)
                            entities[row[configVariableHeaders['entityLabel']]] = result.get('result')[0]
                        except Exception as e:
                            if "exists" not in str(e):
                                logging.info(entity)
                                logging.error("Exception : %s\n" % e)
                                exit()
            else:
                logging.info("Not existing - " + configVariableHeaders['entityLabel'] + configVariableHeaders['entityUri'] + " columns")
           

            
            if configVariableHeaders['characteristicLabel'] in csvColumns:
                characteristicUri = None
                if configVariableHeaders['characteristicUri'] in csvColumns:
                     characteristicUri = row[configVariableHeaders['characteristicUri']]
                if(characteristicUri is not None and characteristicUri not in characteristics):
                    if(row[configVariableHeaders['characteristicLabel']] not in characteristics.values()):
                        description = None
                        if configVariableHeaders['characteristicComment'] in csvColumns:
                            description = format_comment(row[configVariableHeaders['characteristicComment']])
                        else:
                            description = "No description"
                        characteristic = opensilexClientToolsPython.CharacteristicCreationDTO(
                            uri=row[configVariableHeaders['characteristicUri']],
                            name=row[configVariableHeaders['characteristicLabel']],
                            description=description)
                        try:
                            result = variable_os_api.create_characteristic(
                                body=characteristic)
                            characteristics[row[configVariableHeaders['characteristicLabel']]] = result.get('result')[0]
                        except Exception as e:
                            if "exists" not in str(e):
                                logging.error("Exception : %s\n" % e)
                                exit()
            else:
                logging.info("Not existing - " + configVariableHeaders['characteristicLabel'] + configVariableHeaders['characteristicUri'] + " columns")
           

            if configVariableHeaders['methodLabel']  in csvColumns:
                methodUri = None
                if configVariableHeaders['methodUri'] in csvColumns:
                    methodUri = row[configVariableHeaders['methodUri']]
                if(methodUri is not None and methodUri not in methods):
                    if(row[configVariableHeaders['methodLabel']] not in methods.values()):
                        description = None
                        if configVariableHeaders['methodComment'] in csvColumns:
                            description = format_comment(row[configVariableHeaders['methodComment']])
                        else:
                            description = "No description"
                        method = opensilexClientToolsPython.MethodCreationDTO(
                            uri=row[configVariableHeaders['methodUri']],
                            name=row[configVariableHeaders['methodLabel']],
                            description=description
                        )
                        try:
                            result = variable_os_api.create_method(body=method)
                            methods[row[configVariableHeaders['methodLabel']]]= row[configVariableHeaders['methodUri']]
                        except Exception as e:
                            if "exists" not in str(e):
                                logging.error("Exception : %s\n" % e)
                                exit()
                
            else:
                logging.info("Not existing - " + configVariableHeaders['methodLabel'] + configVariableHeaders['methodUri'] + " columns")
           
            if configVariableHeaders['unitLabel']  in csvColumns:
                unitUri = None
                if configVariableHeaders['unitUri'] in csvColumns:
                    unitUri = row[configVariableHeaders['unitUri']]
                if(unitUri is None and unitUri not in units.values()):
                    if(row[configVariableHeaders['unitLabel']] not in units):
                        description = None
                        if configVariableHeaders['methodComment'] in csvColumns:
                            description = format_comment(row[configVariableHeaders['methodComment']])
                        else:
                            description = "No description"
                        unit = opensilexClientToolsPython.UnitCreationDTO(
                            uri=row[configVariableHeaders['unitUri']],
                            name=row[configVariableHeaders['unitLabel']],
                            description=description 
                        )
                        try:
                            result = variable_os_api.create_unit(body=unit)
                            units[row[configVariableHeaders['unitLabel']]] =row[configVariableHeaders['unitUri']]
                        except Exception as e:
                            if "exists" not in str(e):
                                logging.error("Exception : %s\n" % e)
                                exit()
                
            else:
                logging.info("Not existing - " + configVariableHeaders['methodLabel'] + configVariableHeaders['methodUri'] + " columns")
           
            traitUri = None
            if configVariableHeaders['traitUri']  in csvColumns:
                if not isEmpty(row[configVariableHeaders['traitUri']]):
                    traitUri = row[configVariableHeaders['traitUri']]
            else :
                logging.info("Not existing - " + configVariableHeaders['traitUri'] + " in columns")

            traitLabel = None 
            if traitUri is not None :
                if configVariableHeaders['traitLabel'] in csvColumns:
                    if row[configVariableHeaders['traitLabel']] is not None:
                        traitLabel = row[configVariableHeaders['traitLabel']]
                else :
                    logging.info("Not existing - " + configVariableHeaders['traitLabel'] + " in columns")
 
            datatypeUri = "http://www.w3.org/2001/XMLSchema#string"
            if configVariableHeaders['traitLabel']  in csvColumns:
                
                search_list_decimal = ['decimal']
                # re.IGNORECASE is used to ignore case
                if re.compile('|'.join(search_list_decimal), re.IGNORECASE).search(row[configVariableHeaders['dataType']]):
                    datatypeUri = "http://www.w3.org/2001/XMLSchema#decimal"

                search_list_integer = ['integer']
                # re.IGNORECASE is used to ignore case
                if re.compile('|'.join(search_list_integer), re.IGNORECASE).search(row[configVariableHeaders['dataType']]):
                    datatypeUri = "http://www.w3.org/2001/XMLSchema#integer"

                search_list_integer = ['date']
                # re.IGNORECASE is used to ignore case
                if re.compile('|'.join(search_list_integer), re.IGNORECASE).search(row[configVariableHeaders['dataType']]):
                    datatypeUri = "http://www.w3.org/2001/XMLSchema#date"
            else :
                logging.error("Not existing - " + configVariableHeaders['dataType'] + " in columns")
                exit()

            if(traitUri is None and traitLabel is None):
                description = None
                if configVariableHeaders['variableComment'] in csvColumns:
                    description = format_comment(row[configVariableHeaders['variableComment']])
                else:
                    description = "No description"

            alternative_name = None
            if configVariableHeaders['alternativeLabel'] in csvColumns:
                if not isEmpty(row[configVariableHeaders['alternativeLabel']]):
                    alternative_name = row[configVariableHeaders['alternativeLabel']]
             
            variable = opensilexClientToolsPython.VariableCreationDTO(
                uri=row[configVariableHeaders['variableUri']],
                name=row[configVariableHeaders['variableLabel']],
                alternative_name=alternative_name,
                entity=row[configVariableHeaders['entityUri']],
                characteristic=row[configVariableHeaders['characteristicUri']],
                method=row[configVariableHeaders['methodUri']],
                unit=row[configVariableHeaders['unitUri']],
                description=configVariableHeaders['variableComment'] in csvColumns if format_comment(
                                    row[configVariableHeaders['unitComment']]) else  "No description" ,
                datatype=datatypeUri,
                trait= traitUri,
                trait_name=traitLabel
            )  
            try:
                variable_os_api.create_variable(body=variable)
                nbEntities = nbEntities + 1
            except Exception as e:
                if "exists" in str(e):
                   logging.info("Variable " + row[configVariableHeaders['variableLabel']] + " already exists")
                else:
                    logging.error("Exception : %s\n" % e)
                    logging.info("Variable failed")
                    logging.info(variable)
                    logging.info("Number of variables to transfered : " +  str(nbEntities) + '/' + str(totalCount))
                    exit()
        except Exception as e:
            logging.error("Exception : %s\n" % e)
            logging.info("Variable failed")
            logging.info(row)

    logging.info("Number of variables to transfer : " +  str(nbEntities) + '/' + str(totalCount))


def create_sensor(sensor,  pythonClient):
    device_api = opensilexClientToolsPython.DevicesApi(pythonClient)
    sensorTosend = opensilexClientToolsPython.DeviceCreationDTO(
        uri=sensor["uri"], name=sensor["name"], rdf_type=sensor["type"],
        serial_number=sensor["serial_number"], description=sensor["description"])
    try:
        result = device_api.create_device(body=sensorTosend)
        return result.get("result")[0]
    except Exception as e:
        if "exists" not in str(e):
            logging.error("Exception : %s\n" % e)
            exit()
        else:
            logging.info("sensor " + sensor["name"] + " exists")



def create_provenances(provenances, pythonClient):
    data_api = opensilexClientToolsPython.DataApi(pythonClient)
    provenanceUris = []

    for provenance in provenances:
        prov_agent = []
        if "prov_agent" in provenance and provenance["prov_agent"] != None and len(provenance["prov_agent"]) > 0:
            for agent in provenance["prov_agent"]:
                prov_agent_to_send = opensilexClientToolsPython.AgentModel(
                    uri=agent["uri"], rdf_type=agent["rdf_type"])
                prov_agent.append(prov_agent_to_send)

        provenanceTOSend = opensilexClientToolsPython.ProvenanceCreationDTO(
            uri=provenance["uri"], name=provenance["name"], description=provenance["description"], prov_agent=prov_agent
        )
        try:
            result = data_api.create_provenance(body=provenanceTOSend)
            provenance["uri"] = result.get("result")[0]
            provenanceUris.append(provenance)
        except Exception as e:
            if "exists" not in str(e):
                logging.error("Exception : %s\n" % e)
                exit()
            else:
                logging.info("provenance " + provenance["name"] + " exists")
    return provenanceUris


def create_objects_from_csv(csvFile, pythonClient):
    os_api = opensilexClientToolsPython.ScientificObjectsApi(pythonClient)

    objectCSV = pd.read_csv(csvFile)
    logging.info(str(len(objectCSV)) + " objects")
    for index, row in objectCSV.iterrows():
        new_os = opensilexClientToolsPython.ScientificObjectCreationDTO(
            uri=row.uri,
            rdf_type=row.type,
            name=row["name"],
            experiment=row.experimentUri
        )
        try:
            logging.info(str(index + 1) + "/" + str(len(objectCSV)))
            os_api.update_scientific_object(body=new_os)
        except Exception as e:
            logging.error("Exception : %s\n" % e)


def transformDate(date):
    try:
        date = parse(date)
    except Exception as e:
        logging.error("Exception : %s\n" % e)
    return date.astimezone().isoformat()


def create_data_from_csv(csvFile, pythonClient):
    data_api = opensilexClientToolsPython.DataApi(pythonClient)

    dataCSV = pd.read_csv(csvFile)
    data_list = []
    for index, row in dataCSV.iterrows():
        provenanceData = opensilexClientToolsPython.DataProvenanceModel(
            uri=row["provenanceURI"], experiments=[row["experimentURI"]],)

        new_data = opensilexClientToolsPython.DataCreationDTO(
            _date=transformDate(row["date"]),
            scientific_objects=[
                row["objectURI"]],
            variable=row["variableURI"],
            value=row["value"],
            provenance=provenanceData
        )
        data_list.append(new_data)
    logging.info("sending " + str(len(data_list)) + "observations")
    try:
        data_api.add_list_data(body=data_list)
    except Exception as e:
        logging.error("Exception : %s\n" % e)
