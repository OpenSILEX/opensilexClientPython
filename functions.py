
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

logging.basicConfig(level=logging.DEBUG, handlers=[
    logging.FileHandler("debug.log"),
    logging.StreamHandler()
],format='%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s')


def create_experiment(pythonClient, experiment):
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
    if raw_value is None:
        return True
    value = str(raw_value)
    if(value == "NA" or value == None or value == "nan"):
        return True
    return False

def format_comment(raw_comment):
    comment = str(raw_comment) 
    if(isEmpty(comment)):
        return "No description"

    return raw_comment


def migrate_variables_from_googlesheet(pythonClient, configVariableHeaders, spreadsheet_url, gid_number,update):
    variables_url = spreadsheet_url + "/gviz/tq?tqx=out:csv&gid=" + str(gid_number)
    logging.info(variables_url)
    r = requests.get(variables_url).content
    variablesCsvString = requests.get(variables_url).content
 
    variablesCSV = pd.read_csv(io.StringIO(variablesCsvString.decode('utf-8')))
    return migrate_variables(pythonClient, configVariableHeaders, variablesCSV,update)

def migrate_variables_from_csv(pythonClient, configVariableHeaders, csv_path,update):
    variablesCSV = pd.read_csv(csv_path)
    return migrate_variables(pythonClient, configVariableHeaders, variablesCSV,update)

def migrate_variables(pythonClient, configVariableHeaders,variablesCSV,update):
    variablesCSV = variablesCSV.where(pd.notnull(variablesCSV), None)
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

    if update is True :
        update = True 
    else:
        update = False

    logging.info("Update mode variable is set to " + str(update) )

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
            entity_info = create_base_variable(configVariableHeaders,csvColumns, row, entities, 'entity')
            if entity_info is None:
                exit()
            entity = opensilexClientToolsPython.EntityCreationDTO(
                            uri=entity_info['uri'],
                            name=entity_info['name'],
                            description= entity_info['description'])
            try:
                if update:
                    result = variable_os_api.update_entity(body=entity)
                else :
                    result = variable_os_api.create_entity(body=entity)
                
                entities[row[configVariableHeaders['entityLabel']]] = result.get('result')[0]
                entity.uri = result.get('result')[0] 
            except Exception as e:
                if "exists" not in str(e):
                    logging.info(entity)
                    logging.error("Exception : %s\n" % e)
                    exit() 
            
            
            characteristic_info = create_base_variable(configVariableHeaders,csvColumns, row, characteristics, 'characteristic')
            if characteristic_info is None:
                exit()

            characteristic = opensilexClientToolsPython.CharacteristicCreationDTO(
                    uri=characteristic_info['uri'],
                    name=characteristic_info['name'],
                    description= characteristic_info['description'])
            try:    
                if update:
                    result = variable_os_api.update_characteristic(
                        body=characteristic)
                else :
                    result = variable_os_api.create_characteristic(
                        body=characteristic)
                characteristics[row[configVariableHeaders['characteristicLabel']]] = result.get('result')[0]
                characteristic.uri = result.get('result')[0]
            except Exception as e:
                if "exists" not in str(e):
                    logging.error("Exception : %s\n" % e)
                    exit() 

            method_info = create_base_variable(configVariableHeaders,csvColumns, row, methods, 'method')
            if method_info is None:
                exit()
 
            method = opensilexClientToolsPython.MethodCreationDTO(
                uri=method_info['uri'],
                name=method_info['name'],
                description= method_info['description']
            )
            try:
                if update:
                    result = variable_os_api.update_method(body=method)
                else :
                    result = variable_os_api.create_method(body=method)
                methods[row[configVariableHeaders['methodLabel']]] = result.get('result')[0]
                method.uri = result.get('result')[0]
            except Exception as e:
                if "exists" not in str(e):
                    logging.error("Exception : %s\n" % e)
                    exit()
                 
        
            unit_info = create_base_variable(configVariableHeaders,csvColumns, row, units, 'unit')
            if unit_info is None:
                exit()
            unit = opensilexClientToolsPython.UnitCreationDTO(
                uri=method_info['uri'],
                name=method_info['name'],
                description= method_info['description']
            )
            try: 
                if update:
                    result = variable_os_api.update_unit(body=unit)
                else :
                    result = variable_os_api.create_unit(body=unit)

                units[row[configVariableHeaders['unitLabel']]] = result.get('result')[0]
                unit.uri = result.get('result')[0]
            except Exception as e:
                if "exists" not in str(e):
                    logging.error("Exception : %s\n" % e)
                    exit()  
           
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

            alternative_name = None
            if configVariableHeaders['alternativeLabel'] in csvColumns:
                if not isEmpty(row[configVariableHeaders['alternativeLabel']]):
                    alternative_name = row[configVariableHeaders['alternativeLabel']]
             

            var_description = None
            if configVariableHeaders['variableComment'] in csvColumns:
                var_description = format_comment(row[configVariableHeaders['variableComment']])
            else:
                var_description = "No description"
            
            variable = opensilexClientToolsPython.VariableCreationDTO(
                uri=row[configVariableHeaders['variableUri']],
                name=row[configVariableHeaders['variableLabel']],
                alternative_name=alternative_name,
                entity=entity.uri,
                characteristic=characteristic.uri,
                method=method.uri,
                unit=unit.uri,
                description=var_description ,
                datatype=datatypeUri,
                trait= traitUri,
                trait_name=traitLabel
            )  
            
            try:
                if update:
                    variable_os_api.update_variable(body=variable)
                else:
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
            logging.info(variable)

    logging.info("number of transfered variables : " +  str(nbEntities) + '/' + str(totalCount))

def create_base_variable(configVariableHeaders,csvColumns, row, save_list, variable_subtype) :
    result ={}
    name = row[configVariableHeaders[variable_subtype +'Label']] 
    uri = row[configVariableHeaders[variable_subtype +'Uri']]
    description = None
    if configVariableHeaders[variable_subtype +'Label'] in csvColumns and configVariableHeaders[variable_subtype +'Uri'] in csvColumns : 
        if uri is None or uri not in save_list.values():
            if name not in save_list: 
                description = None
                if configVariableHeaders[variable_subtype +'Comment'] in csvColumns:
                    description = format_comment(row[configVariableHeaders[variable_subtype +'Comment']])
                else:
                    description = "No description"
                    
    
        return { "name": name,  "uri" : uri, "description" : description  }
    else:
        logging.info("Not existing - " + configVariableHeaders[variable_subtype +'Label'] + "or" +  configVariableHeaders[variable_subtype +'Uri'] + " columns")
        return None


def create_sensor(pythonClient,sensor):
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



def create_provenances(pythonClient,provenances):
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
            if "exists" not in str(e) and "duplicate"  not in str(e) :
                logging.error("Exception : %s\n" % e)
                exit()
            else:
                logging.info("provenance " + provenance["name"] + " exists")
    return provenanceUris



def update_objects_from_googlesheet(pythonClient, spreadsheet_url, gid_number):
    variables_url = spreadsheet_url + "/gviz/tq?tqx=out:csv&gid=" + str(gid_number)
    r = requests.get(variables_url).content
    variablesCsvString = requests.get(variables_url).content
    object_csv = pd.read_csv(io.StringIO(variablesCsvString.decode('utf-8')))
    return create_update_objects(pythonClient, object_csv,True)

def update_objects_from_csv(pythonClient, csv_path):
    object_csv = pd.read_csv(csv_path)
    return create_update_objects(pythonClient, object_csv,True)

 
def update_objects(pythonClient, object_csv ):
    return create_update_objects(pythonClient, object_csv,True)

def create_objects_from_googlesheet(pythonClient, spreadsheet_url, gid_number):
    variables_url = spreadsheet_url + "/gviz/tq?tqx=out:csv&gid=" + str(gid_number)
    r = requests.get(variables_url).content
    variablesCsvString = requests.get(variables_url).content
    object_csv = pd.read_csv(io.StringIO(variablesCsvString.decode('utf-8')))
    return create_update_objects(pythonClient, object_csv,False)

def create_objects_from_csv(pythonClient, csv_path):
    object_csv = pd.read_csv(csv_path)
    return create_update_objects(pythonClient, object_csv,False)

 
def create_update_objects(pythonClient,object_csv,update):
    os_api = opensilexClientToolsPython.ScientificObjectsApi(pythonClient)
 
    logging.info(str(len(object_csv)) + " objects")
    for index, row in object_csv.iterrows():
        new_os = opensilexClientToolsPython.ScientificObjectCreationDTO(
            uri=row.uri,
            rdf_type=row.type,
            name=row["name"],
            experiment=row.experimentUri
        )
        try:
            logging.info(str(index + 1) + "/" + str(len(object_csv)))
            if update is None or update is False:
                os_api.create_scientific_object(body=new_os)
            else:
                os_api.update_scientific_object(body=new_os)
        except Exception as e:
            if "exists" not in str(e) and "duplicate"  not in str(e) :
                logging.error("Exception : %s\n" % e)
                exit()
            else: 
                logging.info("scientific object " + str(row["name"]) + " exists") 


def transformDate(date):
    try:
        date = parse(date)
    except Exception as e:
        logging.error("Exception : %s\n" % e)
    return date.astimezone().isoformat()



def add_data_from_googlesheet(pythonClient, spreadsheet_url, gid_number):
    variables_url = spreadsheet_url + "/gviz/tq?tqx=out:csv&gid=" + str(gid_number)
    logging.debug("variables url : " + variables_url)
    r = requests.get(variables_url).content
    variablesCsvString = requests.get(variables_url).content
    data_csv = pd.read_csv(io.StringIO(variablesCsvString.decode('utf-8')))
    return add_data_from(pythonClient, data_csv)

def add_data_from_csv(pythonClient, csv_path):
    data_csv = pd.read_csv(csv_path)
    return add_data_from(pythonClient, data_csv)

def add_data_from(pythonClient,data_csv):
    data_api = opensilexClientToolsPython.DataApi(pythonClient) 
    data_list = []
    for index, row in data_csv.iterrows():
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
        if "DUPLICATE" in str(e):
            logging.error("Duplicate data")
        exit()


def getVariablesbyURI(uri="", pythonClient=""):
    if isEmpty(uri):
        uri = []
    if type(uri) is not list:
        uri = [uri]
    var_api = opensilexClientToolsPython.VariablesApi(pythonClient)
    try:
        result = var_api.get_variables_by_ur_is(uris=uri)
        Name = []
        Uri = []
        Alternative_name = []
        Entity_name = []
        Entity_uri = []
        Characteristic_name = []
        Characteristic_uri = []
        Method_name = []
        Method_uri = []
        Unit_name = []
        Unit_uri = []
        Description = []
        for var in result['result']:
            Name.append(var.name)
            Uri.append(var.uri)
            Alternative_name.append(var.alternative_name)
            Entity_name.append(var.entity.name)
            Entity_uri.append(var.entity.uri)
            Characteristic_name.append(var.characteristic.name)
            Characteristic_uri.append(var.characteristic.uri)
            Method_name.append(var.method.name)
            Method_uri.append(var.method.uri)
            Unit_name.append(var.unit.name)
            Unit_uri.append(var.unit.uri)
            Description.append(var.description)
            variable_list = {"name": Name, "Uri": Uri, "alternative_name": Alternative_name, "entity_name": Entity_name, "entity_uri": Entity_uri, "characteristic_name": Characteristic_name,
                             "characteristic_uri": Characteristic_uri, "method_name": Method_name, "method_uri": Method_uri, "unit_name": Unit_name, "unit_uri": Unit_uri, "description": Description}
            var_data = pd.DataFrame(variable_list)
        return var_data
    except Exception as e:
        print("Exception : %s\n" % e)
        return None


def getVariablesByExperiment(experiment="", name="", year="", species="", factors="", pythonClient=""):
    if isEmpty(species):
        species = []
    if isEmpty(factors):
        factors = []
    if pythonClient=="":
        return print("STOP : You must provide the API connection object: pythonClient")  ## moyen moyen cette solution

    exp_api = opensilexClientToolsPython.ExperimentsApi(pythonClient)
    try:
        var_in_expe = exp_api.get_used_variables(uri=experiment)
        variables_details = pd.DataFrame()
        for variables in var_in_expe['result']:
            variables_details = variables_details.append(getVariablesbyURI(uri=[variables.uri], pythonClient = pythonClient))
        return variables_details
    except Exception as e:
        print("Exception : %s\n" % e)
        return None


def get_data(experiment=[], variables=[], scientific_objects=[], pythonClient="", start_date=[], end_date=[]):
    params = dict()
    if variables != []:
        params["variables"] = variables
    if experiment != []:
        params["experiment"] = experiment
    if scientific_objects != []:
        params["scientific_objects"] = scientific_objects
    if end_date != []:
        params["end_date"] = end_date
    if start_date != []:
        params["start_date"] = start_date
    data_api = opensilexClientToolsPython.DataApi(pythonClient)
    try:
        result = data_api.search_data_list(**params)
        Date = []
        Confidence = []
        Metadata = []
        Provenance = []
        Scientific_objects = []
        Uri = []
        Value = []
        Variable = []
        for var in result.get('result'):
            Date.append(var._date)
            Confidence.append(var.confidence)
            Metadata.append(var.metadata)
            Provenance.append(var.provenance)
            Scientific_objects.append(var.scientific_objects)
            Uri.append(var.uri)
            Value.append(var.value)
            Variable.append(var.variable)
        dataFrame = {"date": Date, "confidence": Confidence, "metadata": Metadata, "provenance": Provenance, "scientific_objects": Scientific_objects,
                     "uri": Uri, "value": Value, "variable": Variable}
        data = pd.DataFrame(dataFrame)
        return data
    except Exception as e:
        print("Exception : %s\n" % e)
        return None
