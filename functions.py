"""Custom functions for data imports

TOCHANGE : This script allows the user to print to the console all columns in the
spreadsheet. It is assumed that the first row of the spreadsheet is the
location of the columns.

TOCHANGE : This tool accepts comma separated value files (.csv) as well as excel
(.xls, .xlsx) files.

This script requires that the following packages be installed within 
the Python environment you are running this script in :
    * `opensilexClientToolsPython`
    * `requests`
    * `pandas`
    * `dateparser`

TOCHANGE : This file can also be imported as a module and contains 
the following functions:
    * get_spreadsheet_cols - returns the column headers of the file
    * main - the main function of the script
"""

# %%

from __future__ import print_function
import opensilexClientToolsPython
from pprint import pprint
from datetime import datetime
import requests
import re
import io
import pandas as pd
from dateparser import parse
import logging
from typing import Callable, Iterator, Union, Optional, List
from pydantic import validate_arguments
from functions import *

# %%

logging.basicConfig(level=logging.DEBUG, handlers=[
    logging.FileHandler("debug.log"),
    logging.StreamHandler()
],format='%(asctime)s %(levelname)-8s\
     [%(filename)s:%(lineno)d] %(message)s')

# %%

@validate_arguments(config=dict(arbitrary_types_allowed=True))
def create_experiment(
    pythonClient: opensilexClientToolsPython.ApiClient, 
    name: str, 
    objective: str, 
    start_date: datetime, 
    end_date: datetime = None, 
    uri: str = None, 
    description: str = None, 
    species: List[str] = None, 
    variables: List[str] = None, 
    organisations: List[str] = None, 
    projects: List[str] = None, 
    scientific_supervisorslist: str = None, 
    technical_supervisors: List[str] = None, 
    groups: List[str] = None, 
    factors: List[str] = None, 
    is_public: bool = None
) -> dict:
    """Creates an experiment

    Parameters
    ----------
    pythonClient : opensilexClientToolsPython.ApiClient
        The authenticated client to connect to Opensilex

    All the following arguments can be unpacked from a dict for easier use.
    To do so you just have to call the function as follows :
        create_experiment(pythonClient, **my_dict)

    name: str 
        The name of the experiment (required)
    objective: str 
        The objective of the experiment (required)
    start_date: datetime 
        The starting date of the experiment (required)
    end_date: datetime = None 
        The end date of the experiment
    uri: str = None 
        The uri of the experiment (optional, will be auto-generated if 
        none is given)
    description: str = None 
        A short description of the experiment
    species: List[str] = None 
        A list of the species in the experiment
    variables: List[str] = None 
        A list of the variables of the experiment
    organisations: List[str] = None 
        A list of the organisations that take part in the experiment (NOT SURE)
    projects: List[str] = None 
        A list of the projects in the experiment (NOT SURE)
    scientific_supervisorslist: str = None 
        The name (or uri) of the supervisor of the experiment (NOT SURE)
    technical_supervisors: List[str] = None 
        The names (or uris) of the supervisors of the experiment (NOT SURE)
    groups: List[str] = None 
        The groups (uris) that have acces to this experiment
    factors: List[str] = None 
        The factors (uris) studied in the experiment (NOT SURE)
    is_public: bool = None 
        Wether or not the experiment is public

    Returns
    -------
    dict
        A dictionary representing the experiment (with the 
        auto-generated uri if none was specified)
    """

    # Extract all non-None arguments for experiment as a dictionary
    loc = locals()
    experiment = {
        k: loc[k] 
        for k in loc.keys() 
        if(k!="pythonClient" and loc[k]!=None)
    }

    # Create an instance of the Experiment Api
    experiment_os_api = opensilexClientToolsPython\
        .ExperimentsApi(pythonClient)

    # Creating an object of ExperimentCreationDTO class to use for 
    # experiment creation
    new_expriment = opensilexClientToolsPython\
        .ExperimentCreationDTO(**experiment)

    try:
        # Creating an experiment on Opensilex and catching the result
        experiment_result = experiment_os_api\
            .create_experiment(body=new_expriment)
        # Updating the uri in case none was specified
        experiment["uri"] = experiment_result.get("result")[0]
        logging.info("new experiment" + experiment["name"] + " created")

        # Return the updated dict
        return experiment

    # Catch exceptions to use custom error message if the experiment 
    # already exists
    except Exception as e:
        if "exists" not in str(e):
            logging.error("Exception : %s\n" % e)
        else:
            logging.info("experiment " + experiment["name"] + " exists")

# %%
@validate_arguments(config=dict(arbitrary_types_allowed=True))
def is_empty(value: str) -> bool:
    """Check for emptyness

    Parameters
    ----------
    value : either None or str

    Returns
    -------
    bool
        True if empty, False if not
    """

    # Check for None, or equivalents : "NA", "nan"
    if value is None:
        return True
    if(value == "NA" or value == None or value == "nan"):
        return True
    return False

@validate_arguments(config=dict(arbitrary_types_allowed=True))
def format_comment(comment: str) -> str:
    """Format comment

    Parameters
    ----------
    raw_comment : str

    Returns
    -------
    str
        The comment in str type or "No description" if found empty
    """
    
    if(is_empty(comment)):
        return "No description"

    return comment

# %%
@validate_arguments(config=dict(arbitrary_types_allowed=True))
def migrate_variables_from_googlesheet(
    pythonClient: opensilexClientToolsPython.ApiClient, 
    spreadsheet_url: str, 
    gid_number: str, 
    config_variable_headers: dict = None, 
    update: bool = False
):
    """Get variables data from googlesheet

    Parameters
    ----------
    pythonClient: opensilexClientToolsPython.ApiClient
        The authenticated client to connect to Opensilex
    spreadsheet_url: str
        The url of the googlesheet to get the variables from
    gid_number: str
        TODO
    config_variable_headers: dict
        Dictionnary that describes the header of the file
    update: bool = False
        TODO (wether or not to update?)

    Returns
    -------
    pythonClient: opensilexClientToolsPython.ApiClient
        The authenticated client to connect to Opensilex
    config_variable_headers: dict
        Dictionnary that describes the header of the file
    variables_csv pandas.DataFrame
        Pandas dataframe that contains the variables data
    update
        TODO
    """

    variables_url = spreadsheet_url + "/gviz/tq?tqx=out:csv&gid=" + gid_number
    logging.info(variables_url)
    r = requests.get(variables_url).content
    variables_csvString = requests.get(variables_url).content
 
    variables_csv = pd.read_csv(io.StringIO(variables_csvString.decode('utf-8')))
    return migrate_variables(
        pythonClient, config_variable_headers, variables_csv, update
    )

@validate_arguments(config=dict(arbitrary_types_allowed=True))
def migrate_variables_from_csv(
    pythonClient: opensilexClientToolsPython.ApiClient, 
    csv_path: str, 
    config_variable_headers: dict = None, 
    update: bool = False
):
    """Get variables data from a csv

    Parameters
    ----------
    pythonClient: opensilexClientToolsPython.ApiClient
        The authenticated client to connect to Opensilex
    csv_path: str
        The path to the csv file to get the variables from
    config_variable_headers: dict
        Dictionnary that describes the header of the file
    update: bool = False
        TODO (wether or not to update?)

    Returns
    -------
    pythonClient: opensilexClientToolsPython.ApiClient
        The authenticated client to connect to Opensilex
    config_variable_headers: dict
        Dictionnary that describes the header of the file
    variables_csv pandas.DataFrame
        Pandas dataframe that contains the variables data
    update
        TODO
    """
    variables_csv = pd.read_csv(csv_path)
    return migrate_variables(
        pythonClient, config_variable_headers, variables_csv, update
    )

@validate_arguments(config=dict(arbitrary_types_allowed=True))
def migrate_variables(
    pythonClient: opensilexClientToolsPython.ApiClient, 
    variables_csv: pd.DataFrame, 
    entity_label: str = "entity.label",
    characteristic_label: str = "characteristic.label",
    method_label: str = "method.label",
    unit_label: str = "unit.label",
    variable_label: str = "variable.label",
    data_type: str = "variable.dataype",
    entity_comment: str = None,
    entity_uri: str = None,
    characteristic_uri: str = None,
    characteristic_comment: str = None,
    method_uri: str = None,
    method_comment: str = None,
    unit_uri: str = None,
    unit_comment: str = None,
    trait_uri: str = None,
    trait_label: str = None,
    variable_uri: str = None,
    variable_comment: str = None,
    alternative_label: str = None,
    update: bool = False
):

    logging.info("Update mode variable is set to " + str(update) )

    # Group parameters together to pass as kwargs
    loc = locals()
    excluded = [
        "pythonClient",
        "variables_csv",
        "update"
    ]
    config_variable_headers = {
        k: loc[k] 
        for k in loc.keys() 
        if (k not in excluded and loc[k] != None)
    }

    csv_columns = variables_csv.columns

    # Create the Api needed to manage variables
    variable_os_api = opensilexClientToolsPython.VariablesApi(pythonClient)

    # Counting lines in the data
    total_count = len(variables_csv.index)

    entities = {} 

    # Collect all columns related to entities
    entities_df = variables_csv[[
        config_variable_headers[k]
        for k in config_variable_headers.keys()
        if "entity" in k
    ]]

    # Drop duplicates as entities only need to be created once
    entities_df.drop_duplicates(inplace=True)

    # Take care of the ones that have no missing info first
    no_na = entities_df.dropna()

    # If all uris are unique ------------------------------STOPED HERE
    if no_na[[]].duplicated().any():
        # Create all

    for index, row in no_na.iterrows():
        try:



    characteristics = {} 

    methods = {} 

    units = {} 

    nb_entities = 0

    for index, row in variables_csv.iterrows():
        try:
            entity_info = create_base_variable(
                config_variable_headers, csv_columns, row, entities, 'entity'
            )
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
                
                entities[row[config_variable_headers['entity_label']]] = result.get('result')[0]
                entity.uri = result.get('result')[0] 
            except Exception as e:
                if "exists" not in str(e):
                    logging.info(entity)
                    logging.error("Exception : %s\n" % e)
                    exit() 
            
            
            characteristic_info = create_base_variable(config_variable_headers,csv_columns, row, characteristics, 'characteristic')
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
                characteristics[row[config_variable_headers['characteristic_label']]] = result.get('result')[0]
                characteristic.uri = result.get('result')[0]
            except Exception as e:
                if "exists" not in str(e):
                    logging.error("Exception : %s\n" % e)
                    exit() 

            method_info = create_base_variable(config_variable_headers,csv_columns, row, methods, 'method')
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
                methods[row[config_variable_headers['method_label']]] = result.get('result')[0]
                method.uri = result.get('result')[0]
            except Exception as e:
                if "exists" not in str(e):
                    logging.error("Exception : %s\n" % e)
                    exit()
                 
        
            unit_info = create_base_variable(config_variable_headers,csv_columns, row, units, 'unit')
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

                units[row[config_variable_headers['unit_label']]] = result.get('result')[0]
                unit.uri = result.get('result')[0]
            except Exception as e:
                if "exists" not in str(e):
                    logging.error("Exception : %s\n" % e)
                    exit()  
           
            trait_uri = None
            if config_variable_headers['trait_uri']  in csv_columns:
                if not is_empty(row[config_variable_headers['trait_uri']]):
                    trait_uri = row[config_variable_headers['trait_uri']]
            else :
                logging.info("Not existing - " + config_variable_headers['trait_uri'] + " in columns")

            trait_label = None 
            if trait_uri is not None :
                if config_variable_headers['trait_label'] in csv_columns:
                    if row[config_variable_headers['trait_label']] is not None:
                        trait_label = row[config_variable_headers['trait_label']]
                else :
                    logging.info("Not existing - " + config_variable_headers['trait_label'] + " in columns")
 
            data_typeUri = "http://www.w3.org/2001/XMLSchema#string"
            if config_variable_headers['trait_label']  in csv_columns:
                
                search_list_decimal = ['decimal']
                # re.IGNORECASE is used to ignore case
                if re.compile('|'.join(search_list_decimal), re.IGNORECASE).search(row[config_variable_headers['data_type']]):
                    data_typeUri = "http://www.w3.org/2001/XMLSchema#decimal"

                search_list_integer = ['integer']
                # re.IGNORECASE is used to ignore case
                if re.compile('|'.join(search_list_integer), re.IGNORECASE).search(row[config_variable_headers['data_type']]):
                    data_typeUri = "http://www.w3.org/2001/XMLSchema#integer"

                search_list_integer = ['date']
                # re.IGNORECASE is used to ignore case
                if re.compile('|'.join(search_list_integer), re.IGNORECASE).search(row[config_variable_headers['data_type']]):
                    data_typeUri = "http://www.w3.org/2001/XMLSchema#date"
            else :
                logging.error("Not existing - " + config_variable_headers['data_type'] + " in columns")
                exit()

            alternative_name = None
            if config_variable_headers['alternative_label'] in csv_columns:
                if not is_empty(row[config_variable_headers['alternative_label']]):
                    alternative_name = row[config_variable_headers['alternative_label']]
             

            var_description = None
            if config_variable_headers['variable_comment'] in csv_columns:
                var_description = format_comment(row[config_variable_headers['variable_comment']])
            else:
                var_description = "No description"
            
            variable = opensilexClientToolsPython.VariableCreationDTO(
                uri=row[config_variable_headers['variable_uri']],
                name=row[config_variable_headers['variable_label']],
                alternative_name=alternative_name,
                entity=entity.uri,
                characteristic=characteristic.uri,
                method=method.uri,
                unit=unit.uri,
                description=var_description ,
                data_type=data_typeUri,
                trait= trait_uri,
                trait_name=trait_label
            )  
            
            try:
                if update:
                    variable_os_api.update_variable(body=variable)
                else:
                    variable_os_api.create_variable(body=variable)
                nb_entities = nb_entities + 1
            except Exception as e:
                if "exists" in str(e):
                   logging.info("Variable " + row[config_variable_headers['variable_label']] + " already exists")
                else:
                    logging.error("Exception : %s\n" % e)
                    logging.info("Variable failed")
                    logging.info(variable)
                    logging.info("Number of variables to transfered : " +  str(nb_entities) + '/' + str(total_count))
                    exit()
        except Exception as e:
            logging.error("Exception : %s\n" % e)
            logging.info("Variable failed")
            logging.info(variable)

    logging.info("number of transfered variables : " +  str(nb_entities) + '/' + str(total_count))

# %%
def create_base_variable(
    config_variable_headers,csv_columns, row, save_list, variable_subtype
) :
    
    name = row[config_variable_headers[variable_subtype +'Label']] 
    uri = row[config_variable_headers[variable_subtype +'Uri']]
    description = None
    # First if is useless?
    if (config_variable_headers[variable_subtype +'Label'] in csv_columns 
    and config_variable_headers[variable_subtype +'Uri'] in csv_columns) : 

        # If no uri in the df, or the given uri is already created
        if uri is None or uri not in save_list.values():

            # If the name is not in the already created objects
            if name not in save_list: 

                # Why set the description to None?
                description = None

                # If a comment column exists for this type of object
                if (config_variable_headers[variable_subtype +'Comment'] 
                in csv_columns):

                    # Get the description from the df
                    description = format_comment(
                        row[config_variable_headers[variable_subtype +'Comment']]
                    )
                
                # If there is no comment column, set the description to default
                else:
                    description = "No description"
                    
        # If uri 
        return { "name": name,  "uri" : uri, "description" : description  }
    else:
        logging.info(
            "Not existing - " + config_variable_headers[variable_subtype +'Label'] 
            + "or" +  config_variable_headers[variable_subtype +'Uri'] + " columns"
        )
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
    variables_csvString = requests.get(variables_url).content
    object_csv = pd.read_csv(io.StringIO(variables_csvString.decode('utf-8')))
    return create_update_objects(pythonClient, object_csv,True)

def update_objects_from_csv(pythonClient, csv_path):
    object_csv = pd.read_csv(csv_path)
    return create_update_objects(pythonClient, object_csv,True)

 
def update_objects(pythonClient, object_csv ):
    return create_update_objects(pythonClient, object_csv,True)

def create_objects_from_googlesheet(pythonClient, spreadsheet_url, gid_number):
    variables_url = spreadsheet_url + "/gviz/tq?tqx=out:csv&gid=" + str(gid_number)
    r = requests.get(variables_url).content
    variables_csvString = requests.get(variables_url).content
    object_csv = pd.read_csv(io.StringIO(variables_csvString.decode('utf-8')))
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
    variables_csvString = requests.get(variables_url).content
    data_csv = pd.read_csv(io.StringIO(variables_csvString.decode('utf-8')))
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
            variable=row["variable_uri"],
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
