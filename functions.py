'''
File: functions.py
Project: opensilex-ws-python-client
Created Date: 01 May 2021
Author: Arnaud Charleroy
-----
Last Modified: Thu Sep 09 2021
Modified By: Gabriel Besombes
-----
HISTORY:
Date      	By	Comments
----------	---	---------------------------------------------------------
'''

from __future__ import print_function
from os import truncate # Must be here

# TODO : make a class instead 
# with a connect method that auto reconnects every x minutes unless signaled to stop
# recurcive method?
# Proper way is probably with an event loop : https://stackoverflow.com/questions/474528/what-is-the-best-way-to-repeatedly-execute-a-function-every-x-seconds

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

import opensilexClientToolsPython
from pprint import pprint
from datetime import datetime
from opensilexClientToolsPython.models.entity_creation_dto import EntityCreationDTO
import requests
import re
import io
import pandas as pd
from dateparser import parse
import logging
from typing import Union, List
from pydantic import validate_arguments
from functions import *

# %%
# Setup logging file
logging.basicConfig(level=logging.DEBUG, handlers=[
    logging.FileHandler("debug.log"),
    logging.StreamHandler()
],format='%(asctime)s %(levelname)-8s\
     [%(filename)s:%(lineno)d] %(message)s')

# %%
# Define full schema
full_schema = {
    'trait':'trait.uri',
    'trait_name':'trait.label',
    'entity':{
        'name':'entity.label',
        'uri':'entity.uri',
        'description':'entity.comment',
        'exact_match':'exact_match',
        'close_match':'close_match',
        'broad_match':'broad_match',
        'narrow_match':'narrow_match'
    },
    'characteristic':{
        'name':'characteristic.label',
        'uri':'characteristic.uri',
        'description':'characteristic.comment',
        'exact_match':'exact_match',
        'close_match':'close_match',
        'broad_match':'broad_match',
        'narrow_match':'narrow_match'
    },
    'method':{
        'name':'method.label',
        'uri':'method.uri',
        'description':'method.comment',
        'exact_match':'exact_match',
        'close_match':'close_match',
        'broad_match':'broad_match',
        'narrow_match':'narrow_match'
    },
    'unit':{
        'name':'unit.label',
        'uri':'unit.uri',
        'description':'unit.comment',
        'symbol':'symbol',
        'alternative_symbol':'alternative_symbol',
        'exact_match':'exact_match',
        'close_match':'close_match',
        'broad_match':'broad_match',
        'narrow_match':'narrow_match'
    },
    'uri':'variable.uri',
    'name':'variable.label',
    'description':'variable.description',
    'datatype':'variable.datatype',
    'alternative_name':'variable.alternative_name',
    'time_interval':'variable.timeinterval',
    'sampling_interval':'variable.sampleinterval',
    'exact_match':'exact_match',
    'close_match':'close_match',
    'broad_match':'broad_match',
    'narrow_match':'narrow_match'
}

# %%

@validate_arguments(config=dict(arbitrary_types_allowed=True))
def create_experiment(
    python_client: opensilexClientToolsPython.ApiClient, 
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
    python_client : opensilexClientToolsPython.ApiClient
        The authenticated client to connect to Opensilex

    All the following arguments can be unpacked from a dict for easier use.
    To do so you just have to call the function as follows :
        create_experiment(python_client, **my_dict)

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
        if(k!="python_client" and loc[k]!=None)
    }

    # Create an instance of the Experiment Api
    experiment_os_api = opensilexClientToolsPython\
        .ExperimentsApi(python_client)

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
        logging.info("new experiment " + experiment["name"] + " created")

        # Return the updated dict
        return experiment

    # Catch exceptions to use custom error message if the experiment 
    # already exists
    except Exception as e:
        if "exists" not in str(e):
            logging.error("Exception : %s\n" % e)
        else:
            logging.info("experiment " + experiment["name"] + " already exists")

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
    python_client: opensilexClientToolsPython.ApiClient, 
    spreadsheet_url: str, 
    gid_number: str, 
    variables_schema: dict = {
        'trait':'trait.uri',
        'trait_name':'trait.label',
        'entity':{
            'name':'entity.label',
            'uri':'entity.uri',
            'description':'entity.comment'
        },
        'characteristic':{
            'name':'characteristic.label',
            'uri':'characteristic.uri',
            'description':'characteristic.comment'
        },
        'method':{
            'name':'method.label',
            'uri':'method.uri',
            'description':'method.comment'
        },
        'unit':{
            'name':'unit.label',
            'uri':'unit.uri',
            'description':'unit.comment'
        },
        'uri':'variable.uri',
        'name':'variable.label',
        'description':'variable.description',
        'datatype':'variable.datatype',
        'alternative_name':'variable.alternative_name',
        'time_interval':'variable.timeinterval',
        'sampling_interval':'variable.sampleinterval'
    }, 
    update: bool = False
) -> None:
    """Get variables data from googlesheet

    Parameters
    ----------
    python_client: opensilexClientToolsPython.ApiClient
        The authenticated client to connect to Opensilex
    spreadsheet_url: str
        The url of the googlesheet to get the variables from
    gid_number: str
        TODO
    variables_schema: dict
        Dictionnary that describes the header of the in correspondance
        with the names in opensilex.
        Format is 'opensilexname':'columnname'
        or 'opensilexsubtype':{'opensilexname':'columnname'}
        Example : {
            'trait':'trait.uri',
            'trait_name':'trait.label',
            'entity':{
                'name':'entity.label',
                'uri':'entity.uri',
                'description':'entity.comment'
            },
            'characteristic':{
                'name':'characteristic.label',
                'uri':'characteristic.uri',
                'description':'characteristic.comment'
            },
            'method':{
                'name':'method.label',
                'uri':'method.uri',
                'description':'method.comment'
            },
            'unit':{
                'name':'unit.label',
                'uri':'unit.uri',
                'description':'unit.comment'
            },
            'uri':'variable.uri',
            'name':'variable.label',
            'description':'variable.description',
            'datatype':'variable.datatype',
            'alternative_name':'variable.alternative_name',
            'time_interval':'variable.timeinterval',
            'sampling_interval':'variable.sampleinterval'
        }
    update: bool = False
        TODO (wether or not to update?)

    Returns
    -------
    None
    """

    variables_url = spreadsheet_url + "/gviz/tq?tqx=out:csv&gid=" + gid_number
    logging.info(variables_url)
    variables_csv_string = requests.get(variables_url).content
 
    variables_csv = pd.read_csv(io.StringIO(variables_csv_string.decode('utf-8')))

    # Replace nan with None
    variables_csv = variables_csv.where(
        pd.notnull(variables_csv), None
    )

    return migrate_variables(
        python_client=python_client, 
        variables_csv=variables_csv, 
        variables_schema=variables_schema,
        update=update
    )

@validate_arguments(config=dict(arbitrary_types_allowed=True))
def migrate_variables_from_csv(
    python_client: opensilexClientToolsPython.ApiClient, 
    csv_path: str, 
    variables_schema: dict = {
        'trait':'trait.uri',
        'trait_name':'trait.label',
        'entity':{
            'name':'entity.label',
            'uri':'entity.uri',
            'description':'entity.comment'
        },
        'characteristic':{
            'name':'characteristic.label',
            'uri':'characteristic.uri',
            'description':'characteristic.comment'
        },
        'method':{
            'name':'method.label',
            'uri':'method.uri',
            'description':'method.comment'
        },
        'unit':{
            'name':'unit.label',
            'uri':'unit.uri',
            'description':'unit.comment'
        },
        'uri':'variable.uri',
        'name':'variable.label',
        'description':'variable.description',
        'datatype':'variable.datatype',
        'alternative_name':'variable.alternative_name',
        'time_interval':'variable.timeinterval',
        'sampling_interval':'variable.sampleinterval'
    }, 
    update: bool = False
) -> None:
    """Get variables data from a csv

    Parameters
    ----------
    python_client: opensilexClientToolsPython.ApiClient
        The authenticated client to connect to Opensilex
    csv_path: str
        The path to the csv file to get the variables from
    variables_schema: dict
        Dictionnary that describes the header of the in correspondance
        with the names in opensilex.
        Format is 'opensilexname':'columnname'
        or 'opensilexsubtype':{'opensilexname':'columnname'}
        Example : {
            'trait':'trait.uri',
            'trait_name':'trait.label',
            'entity':{
                'name':'entity.label',
                'uri':'entity.uri',
                'description':'entity.comment'
            },
            'characteristic':{
                'name':'characteristic.label',
                'uri':'characteristic.uri',
                'description':'characteristic.comment'
            },
            'method':{
                'name':'method.label',
                'uri':'method.uri',
                'description':'method.comment'
            },
            'unit':{
                'name':'unit.label',
                'uri':'unit.uri',
                'description':'unit.comment'
            },
            'uri':'variable.uri',
            'name':'variable.label',
            'description':'variable.description',
            'datatype':'variable.datatype',
            'alternative_name':'variable.alternative_name',
            'time_interval':'variable.timeinterval',
            'sampling_interval':'variable.sampleinterval'
        }
    update: bool = False
        TODO (wether or not to update?)

    Returns
    -------
    None
    """
    variables_csv = pd.read_csv(csv_path)

    if len(variables_csv.columns) <= 2:
        variables_csv = pd.read_csv(csv_path, sep=";")

    # Replace nan with None
    variables_csv = variables_csv.where(
        pd.notnull(variables_csv), None
    )
    
    return migrate_variables(
        python_client=python_client, 
        variables_csv=variables_csv, 
        variables_schema=variables_schema,
        update=update
    )

# TODO Create a func to exchange heys and values in a dict

@validate_arguments(config=dict(arbitrary_types_allowed=True))
def migrate_variables(
    python_client: opensilexClientToolsPython.ApiClient, 
    variables_csv: pd.DataFrame, 
    variables_schema: dict = {
        'trait':'trait.uri',
        'trait_name':'trait.label',
        'entity':{
            'name':'entity.label',
            'uri':'entity.uri',
            'description':'entity.comment'
        },
        'characteristic':{
            'name':'characteristic.label',
            'uri':'characteristic.uri',
            'description':'characteristic.comment'
        },
        'method':{
            'name':'method.label',
            'uri':'method.uri',
            'description':'method.comment'
        },
        'unit':{
            'name':'unit.label',
            'uri':'unit.uri',
            'description':'unit.comment',
        },
        'uri':'variable.uri',
        'name':'variable.label',
        'description':'variable.description',
        'datatype':'variable.datatype',
        'alternative_name':'variable.alternative_name',
        'time_interval':'variable.timeinterval',
        'sampling_interval':'variable.sampleinterval'
    },
    update: bool = False
) -> None:
    """Create variables from a pandas.DataFrame

        Parameters
        ----------
        python_client: opensilexClientToolsPython.ApiClient
            The authenticated client to connect to Opensilex
        variables_csv: pd.DataFrame
            A pandas DataFrame containing the data needed to create the variables
        variables_schema: dict
            Dictionnary that describes the header of the in correspondance
            with the names in opensilex.
            Format is 'opensilexname':'columnname'
            or 'opensilexsubtype':{'opensilexname':'columnname'}
            Example : {
                'trait':'trait.uri',
                'trait_name':'trait.label',
                'entity':{
                    'name':'entity.label',
                    'uri':'entity.uri',
                    'description':'entity.comment'
                },
                'characteristic':{
                    'name':'characteristic.label',
                    'uri':'characteristic.uri',
                    'description':'characteristic.comment'
                },
                'method':{
                    'name':'method.label',
                    'uri':'method.uri',
                    'description':'method.comment'
                },
                'unit':{
                    'name':'unit.label',
                    'uri':'unit.uri',
                    'description':'unit.comment'
                },
                'uri':'variable.uri',
                'name':'variable.label',
                'description':'variable.description',
                'datatype':'variable.datatype',
                'alternative_name':'variable.alternative_name',
                'time_interval':'variable.timeinterval',
                'sampling_interval':'variable.sampleinterval'
            }
        update: bool = False
            TODO (wether or not to update?)

        Returns
        -------
        None
        """

    # The update isn't currently implemented
    logging.info("Update mode variable is set to " + str(update) + "\n\n")

    # Check if the names given in the schema exist in the DataFrame
    cols = []
    for val in variables_schema.values():
        if type(val)==str:
            cols.append(val)
        else:
            cols = cols + list(val.values())

    col_match = [
        col
        for col in cols
        if col not in variables_csv.columns
    ]

    if col_match:
        raise ValueError(
            """The following names in the schema couldn't be matched to any columns :
    {0}
The actual columns found are :
    {1}
            """.format(col_match, variables_csv.columns)
        )
    
    # DataFrame for results of objects creations
    variables_df = pd.DataFrame(
        columns=[key for key in full_schema]
    )

    # DataFrame for objects already existed
    already_df = pd.DataFrame(
        columns=[key for key in full_schema]
    )

    # DataFrame for failed objects
    failed_df = pd.DataFrame(
        columns=[key for key in variables_schema]
    )

    # Fetch all datatypes for Variable creation
    var_api_instance = opensilexClientToolsPython.VariablesApi(python_client)
    datatypes = var_api_instance.get_datatypes()

    # Create all objects that need to be created on opensilex
    for key in variables_schema.keys():

        # Create objects on opensilex if needed
        if type(variables_schema[key])==dict:

            # TODO work from unique values then join the results to get the right format
            
            # Subset of the dataframe with the data needed to create the objects
            sub_df = variables_csv[variables_schema[key].values()]
            
            # Change column labels to opensilex labels
            col_exchange = {
                v:k
                for k,v in variables_schema[key].items()
            }
            sub_df.rename(columns=col_exchange, inplace=True)
            
            # DataFrame for results
            df_res = pd.DataFrame(columns=full_schema[key].keys(), dtype=object)

            # Create all the objects line by line
            for index, row in sub_df.iterrows():

                # Check if row is a duplicate of previous rows
                duplicate_of = (
                    sub_df.loc[:index-1].values == row.values
                ).all(axis=1)

                # If it is a duplicate use the data already in df_res
                if duplicate_of.any():

                    # Use the first row of all the duplicates
                    df_res.loc[index] = df_res.loc[:index-1]\
                        .loc[duplicate_of].iloc[0]

                # Otherwise create the object
                else:
                    try:
                        object_info = create_base_variable(
                            python_client=python_client,
                            row=row,
                            index=index,
                            variable_subtype=key,
                        )
                        
                        # If failed set the row to False and save it in failed
                        if object_info[1] == "failed":
                            failed_df = failed_df.append(row, ignore_index=True)
                            df_res.loc[index] = False

                        # If already existed update the info and save it in already existed
                        elif object_info[1] == "already":
                            already_df = already_df.append(object_info[0], ignore_index=True)
                            df_res.loc[index] = object_info[0]

                        else:
                            df_res.loc[index] = object_info[0]
                        
                    except Exception as e:
                        logging.info(object_info)
                        logging.error("Exception : %s\n" % e)
                        failed_df = failed_df.append(row, ignore_index=True)
                        df_res.loc[index] = False
            
            # The column for the variable subtype is set to contain the uris
            variables_df[key] = df_res.uri

        # Special case : 'datatype'
        elif key == "datatype":

            # Subset of the dataframe with the datatypes
            sub_df = pd.DataFrame(variables_csv[variables_schema[key]])
            
            # Change column labels to opensilex labels
            sub_df.rename(columns={variables_schema[key]:key}, inplace=True)
            
            # DataFrame for results
            df_res = pd.DataFrame(columns=full_schema.keys(), dtype=object)

            # Set the datatypes line by line
            for index, row in sub_df.iterrows():
                
                datatype_matches = [
                    dt.uri
                    for dt in datatypes["result"]
                    if row[key].lower() in dt.name.lower()
                ]
                
                if any(datatype_matches):
                    # If multiple matches, keep first one : should never happen
                    df_res.loc[index, key] = datatype_matches[0]
                else:
                    # If no matches, set to False
                    df_res.loc[index, key] = False
                    logging.info(
                        """Couldn't find a datatype for : {}\n""".format(
                            row.loc[key]
                        )
                    )
                    failed_df = failed_df.append(row, ignore_index=True)
                    df_res.loc[index, key] = False
            variables_df[key] = df_res[key]

        else:
            variables_df[key] = variables_csv[variables_schema[key]]
    
    # Now that all necessary objects were created the Variables can be created
    # TODO Should probably be a separate func
    for index, row in variables_df.iterrows():
        
        # If at least one object couldn't be created
        if (row==False).any():
            logging.info(
                """This variable couldn't be created because one or more objects couldn't be created:
{}\n""".format(dict(row))
            )

            # Save it in the failed and remove it from the successes
            failed_df = failed_df.append(row, ignore_index=True)
            variables_df.drop(index=index, inplace=True)

        else:
            try:
                # Replace nan with None
                r = row.where(pd.notnull(row), None)

                # Create the variable
                var_info = create_base_variable(
                    python_client=python_client,
                    row=r,
                    index=index,
                    variable_subtype='variable'
                )
                
                if var_info[1] == "failed":
                    # Save it in the failed and remove it from the successes
                    failed_df = failed_df.append(row, ignore_index=True)
                    variables_df.drop(index=index, inplace=True)
                
                elif var_info[1] == "already":
                    # Save it in the failed and remove it from the successes
                    already_df = already_df.append(var_info[0], ignore_index=True)
                    variables_df.drop(index=index, inplace=True)

                else:
                    # Update the values after variable creation
                    variables_df.loc[index, var_info[0].keys()] = var_info[0]

            except Exception as e:
                logging.info(dict(r))
                logging.error("Exception : %s\n" % e)
    
    # Export variables DataFrame to csv
    variables_df.to_csv("variables_created.csv", index=False)
    
    # Export already existed DataFrame to csv
    already_df.to_csv("already_existed.csv", index=False)
    
    # Export failed objects DataFrame to csv
    failed_df.to_csv("failed.csv", index=False)

    return variables_df

# %%
# Create variables or objects on opensilex
@validate_arguments(config=dict(arbitrary_types_allowed=True))
def create_base_variable(
    python_client: opensilexClientToolsPython.ApiClient,
    row: pd.Series,
    index: int,
    variable_subtype: str
)-> Union[dict, str]:
    """Create objects in opensilex

    Parameters
    ----------
    python_client: opensilexClientToolsPython.ApiClient
        The authenticated client to connect to Opensilex
    row: pd.Series
        The series containing the data to create the object on opensilex
    variable_subtype: str
        The subtype of the object to be created (entity, unit, etc...)

    Returns
    -------
    created_object: Union[dict, bool]
        Returns a dict corresponding to the object created or found that 
        already existed on opensilex or False if it failed
    """

    # Dictionnary of DTO functions to use for each object subtype
    dtos = {
        "entity": opensilexClientToolsPython.EntityCreationDTO,
        "characteristic": opensilexClientToolsPython.CharacteristicCreationDTO,
        "unit": opensilexClientToolsPython.UnitCreationDTO,
        "method": opensilexClientToolsPython.MethodCreationDTO,
        "variable": opensilexClientToolsPython.VariableCreationDTO
    }

    # Dictionnary of apis to use for each object subtype
    apis = {
        "entity": opensilexClientToolsPython.VariablesApi(python_client),
        "characteristic": opensilexClientToolsPython.VariablesApi(python_client),
        "unit": opensilexClientToolsPython.VariablesApi(python_client),
        "method": opensilexClientToolsPython.VariablesApi(python_client),
        "variable": opensilexClientToolsPython.VariablesApi(python_client)
    }
    
    # Dictionnary of creation functions to use for each object subtype
    creation_func = {
        "entity": apis["entity"].create_entity,
        "characteristic": apis["characteristic"].create_characteristic,
        "unit": apis["unit"].create_unit,
        "method": apis["method"].create_method,
        "variable": apis["variable"].create_variable,
    }

    # Dictionnary of search functions to use for each object subtype
    search_func = {
        "entity": apis["entity"].search_entities,
        "characteristic": apis["characteristic"].search_characteristics,
        "unit": apis["unit"].search_units,
        "method": apis["method"].search_methods,
        "variable": apis["variable"].search_variables
    }
    
    # Dictionnary of get functions to use for each object subtype
    get_func = {
        "entity": apis["entity"].get_entity,
        "characteristic": apis["characteristic"].get_characteristic,
        "unit": apis["unit"].get_unit,
        "method": apis["method"].get_method,
        "variable": apis["variable"].get_variable
    }
          
    # If no name was given, custom message
    if "name" in row.index and row["name"] == None:
        
        # If no uri was given, custom message
        if ("uri" in row.index and row["uri"] == None) or "uri" not in row.index:
            logging.info(
                """The object {} couldn't be created as no name was given and couldn't be found as no uri was given\n"""\
                    .format(dict(row))
            )
            return(dict(row), "failed") 

        # Trying to match uri
        try:
            old_object = get_func[variable_subtype](
                uri = row["uri"]
            )
        except:
            # If no name was given and the uri doesn't match any object, custom message
            logging.info(
                """The object {} couldn't be created as no name was given and couldn't be found as no object with that uri exist\n"""\
                    .format(dict(row))
            )
            return(dict(row), "failed")
        v = vars(old_object["result"])
        return_dict = {
            col.replace("_", "", 1): v[col]
            for col in v
            if "_" in col
        }
        logging.info(
            """Object {0} at row {1} wasn't created as no name was given and an object with that uri already exists.
That object was skipped and will appear in the "already_existed.csv" file.
The object used instead is {2}\n""".format(dict(row), index, return_dict)
        )
        # TODO add row to already_existed.csv
        return (return_dict, "already")
    
    # Check if the object already exists
    try:
        # Escape regex for exact match
        # TODO : ignore case
        escaped_name = re.escape(row["name"])
        old_object = search_func[variable_subtype](
            name="^" + escaped_name + "$"
        )

        if len(old_object["result"]) != 0:

            # Making sure to have a consistent output
            old_object = get_func[variable_subtype](
                uri = old_object["result"][0].uri
            )
            v = vars(old_object["result"])
            return_dict = {
                col.replace("_", "", 1): v[col]
                for col in v
                if "_" in col
            }
            logging.info(
                """Object {0} at row {1} wasn't created as an object with that name already exists.
That object was skipped and will appear in the "already_existed.csv" file.
The object used instead is {2}\n""".format(dict(row), index, return_dict)
            )
            # TODO add row to already_existed.csv
            return (return_dict, "already")

    except Exception as e:
        logging.error("""Exception on object {0} :
    {1}
    
    """.format(dict(row), e))
        # TODO add row to failed.csv
        return (dict(row), "failed")
    
    # Try to use DTO function
    try:
        new_dto = dtos[variable_subtype](**row)

        # Try to use creation function
        try:
            new_object = creation_func[variable_subtype](body=new_dto)
            new_object = get_func[variable_subtype](
                uri = new_object["result"][0]
            )
            
            # TODO make this a func to extract and normalize result object's attributes
            v = vars(new_object["result"])
            return_dict = {
                col.replace("_", "", 1): v[col]
                for col in v
                if "_" in col
            }
            logging.info("Object created: {}\n".format(return_dict))
            # TODO add row to created.csv
            return (return_dict, "created")

        except Exception as e:
            # Catch 'URI already exists' exception separately
            if("URI already exists" in str(e)):
                old_object = get_func[variable_subtype](
                    uri = row['uri']
                )
                
                v = vars(old_object["result"])
                return_dict = {
                    col.replace("_", "", 1): v[col]
                    for col in v
                    if "_" in col
                }
                logging.info(
                    """Object {0} at row {1} couldn't be created as this URI already exists.
That object was skipped and will appear in the "skipped.csv" file.
The object with that URI will be used instead : {2}
For the exact error see the following:
ValueError: {3}\n""".format(dict(row), index, return_dict, e)
                )
                # TODO add row to already_existed.csv
                return (return_dict, "already")
            else:
                logging.error("""Exception on object{0} :
    {1}
    
    """.format(dict(row), e))
                # TODO add row to failed.csv
                return (dict(row), "failed")

    except Exception as e:
        logging.error("""Exception on object{0} :
    {1}

""".format(dict(row), e))
        # TODO add row to failed.csv
        return (dict(row), "failed")



# %%
def create_sensor(python_client,sensor):
    device_api = opensilexClientToolsPython.DevicesApi(python_client)
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



def create_provenances(python_client,provenances):
    data_api = opensilexClientToolsPython.DataApi(python_client)
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



def update_objects_from_googlesheet(python_client, spreadsheet_url, gid_number):
    variables_url = spreadsheet_url + "/gviz/tq?tqx=out:csv&gid=" + str(gid_number)
    r = requests.get(variables_url).content
    variables_csv_string = requests.get(variables_url).content
    object_csv = pd.read_csv(io.StringIO(variables_csv_string.decode('utf-8')))
    return create_update_objects(python_client, object_csv,True)

def update_objects_from_csv(python_client, csv_path):
    object_csv = pd.read_csv(csv_path)
    return create_update_objects(python_client, object_csv,True)

 
def update_objects(python_client, object_csv ):
    return create_update_objects(python_client, object_csv,True)

def create_objects_from_googlesheet(python_client, spreadsheet_url, gid_number):
    variables_url = spreadsheet_url + "/gviz/tq?tqx=out:csv&gid=" + str(gid_number)
    r = requests.get(variables_url).content
    variables_csv_string = requests.get(variables_url).content
    object_csv = pd.read_csv(io.StringIO(variables_csv_string.decode('utf-8')))
    return create_update_objects(python_client, object_csv,False)

def create_objects_from_csv(python_client, csv_path):
    object_csv = pd.read_csv(csv_path)
    return create_update_objects(python_client, object_csv,False)

 
def create_update_objects(python_client,object_csv,update):
    os_api = opensilexClientToolsPython.ScientificObjectsApi(python_client)
 
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



def add_data_from_googlesheet(python_client, spreadsheet_url, gid_number):
    variables_url = spreadsheet_url + "/gviz/tq?tqx=out:csv&gid=" + str(gid_number)
    logging.debug("variables url : " + variables_url)
    r = requests.get(variables_url).content
    variables_csv_string = requests.get(variables_url).content
    data_csv = pd.read_csv(io.StringIO(variables_csv_string.decode('utf-8')))
    return add_data_from(python_client, data_csv)

def add_data_from_csv(python_client, csv_path):
    data_csv = pd.read_csv(csv_path)
    return add_data_from(python_client, data_csv)

def add_data_from(python_client,data_csv):
    data_api = opensilexClientToolsPython.DataApi(python_client) 
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

# %%
# Fetch variables
@validate_arguments(config=dict(arbitrary_types_allowed=True))
def get_variables(
    python_client: opensilexClientToolsPython.ApiClient, 
    csv_path: str = "variables_export.csv",
    uri_match = None,
    name_match = None,
    variables_schema: dict = {
        'trait':'trait.uri',
        'trait_name':'trait.label',
        'entity':{
            'name':'entity.label',
            'uri':'entity.uri',
            'description':'entity.comment'
        },
        'characteristic':{
            'name':'characteristic.label',
            'uri':'characteristic.uri',
            'description':'characteristic.comment'
        },
        'method':{
            'name':'method.label',
            'uri':'method.uri',
            'description':'method.comment'
        },
        'unit':{
            'name':'unit.label',
            'uri':'unit.uri',
            'description':'unit.comment',
        },
        'uri':'variable.uri',
        'name':'variable.label',
        'description':'variable.description',
        'datatype':'variable.datatype',
        'alternative_name':'variable.alternative_name',
        'time_interval':'variable.timeinterval',
        'sampling_interval':'variable.sampleinterval'
    } 
) -> None:
    """TODO UPDDATE DOCSTRING
    Create variables from a pandas.DataFrame

        Parameters
        ----------
        python_client: opensilexClientToolsPython.ApiClient
            The authenticated client to connect to Opensilex
        variables_csv: pd.DataFrame
            A pandas DataFrame containing the data needed to create the variables
        variables_schema: dict
            Dictionnary that describes the header of the in correspondance
            with the names in opensilex.
            Format is 'opensilexname':'columnname'
            or 'opensilexsubtype':{'opensilexname':'columnname'}
            Example : {
                'trait':'trait.uri',
                'trait_name':'trait.label',
                'entity':{
                    'name':'entity.label',
                    'uri':'entity.uri',
                    'description':'entity.comment'
                },
                'characteristic':{
                    'name':'characteristic.label',
                    'uri':'characteristic.uri',
                    'description':'characteristic.comment'
                },
                'method':{
                    'name':'method.label',
                    'uri':'method.uri',
                    'description':'method.comment'
                },
                'unit':{
                    'name':'unit.label',
                    'uri':'unit.uri',
                    'description':'unit.comment'
                },
                'uri':'variable.uri',
                'name':'variable.label',
                'description':'variable.description',
                'datatype':'variable.datatype',
                'alternative_name':'variable.alternative_name',
                'time_interval':'variable.timeinterval',
                'sampling_interval':'variable.sampleinterval'
            }
        update: bool = False
            TODO (wether or not to update?)

        Returns
        -------
        None
        """

    # Dictionnary of get functions to use for each object subtype
    var_api = opensilexClientToolsPython.VariablesApi(python_client)
    get_func = {
        "entity": var_api.get_entity,
        "characteristic": var_api.get_characteristic,
        "unit": var_api.get_unit,
        "method": var_api.get_method,
    }

    # Flattened full schema
    cols = []
    for val in variables_schema.values():
        if type(val)==str:
            cols.append(val)
        else:
            cols = cols + list(val.values())

    # Pandas DataFrame for the results
    df_res = pd.DataFrame(columns=cols)

    # TODO Better way to do this?
    # Get number of results
    res_vars = var_api.search_variables_details()

    # Get all results
    res_vars = var_api.search_variables_details(
        page_size=res_vars["metadata"]["pagination"]["totalCount"]
    )
    
    # Populate DataFrame with results
    for res in res_vars["result"]:
        
        if(
            (uri_match and uri_match in res.uri)
            or (name_match and name_match in res.name)
            or (not uri_match and not name_match)
        ):

            # Extract the attributes of the resultDTO object
            v = vars(res)
            var_dict = {
                col.replace("_", "", 1): v[col]
                for col in v
                if "_" in col
            }
            
            # Dictionary to extract the data from one result
            res_dict = {}
            
            for key in variables_schema.keys():
                if type(variables_schema[key]) == str:
                    res_dict[variables_schema[key]] = var_dict[key]
                
                else:
                    
                    # Isolate sub values
                    sub_schema = variables_schema[key]

                    # In case Method/Unit/Characteristic/Entity is None
                    # (Shouldn't be possible but still happens)
                    if var_dict[key] != None:
                        sub_res = get_func[key](uri=var_dict[key].uri)
                        
                        # Extract the attributes of the resultDTO object
                        sub_v = vars(sub_res["result"])
                        sub_dict = {
                            col.replace("_", "", 1): sub_v[col]
                            for col in sub_v
                            if "_" in col
                        }
                        for key_2 in sub_schema.keys():
                            res_dict[sub_schema[key_2]] = sub_dict[key_2]
                    
                    else:
                        for key_2 in sub_schema.keys():
                            res_dict[sub_schema[key_2]] = None

            df_res = df_res.append(res_dict, ignore_index=True)
    
    # Saving the result as a csv
    df_res.to_csv(csv_path, index=False)

    return df_res



# %%
# Post users
@validate_arguments(config=dict(arbitrary_types_allowed=True))
def create_users_from_google_sheet(
    python_client: opensilexClientToolsPython.ApiClient, 
    sheet_id: str = "1hcWI9BoMlJLMi0RGfl1C2pDdnxEVDaUvbCY14VxUZ4g",
    sheet_name: str = "DiaPhen_users",
    maping: dict = {
        "uri": "orcid",
        "first_name": "first_name",
        "last_name": "family_name",
        "email": "email",
        "admin": "isadmin"
    }) -> None:
    """
    """
    
    # Fetching data from google sheet
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
    users_df = pd.read_csv(url)

    # Maping columns
    users_mod = pd.DataFrame(columns=[
        "uri", "first_name", "last_name", "email", "language", "password", "admin"
    ])
    for col in maping.keys():
        users_mod[col] = users_df[maping[col]]
    
    # List of attributes that have no default
    no_default = ["first_name", "last_name", "email"]

    
    # Setting missing values to none or default
    users_mod["uri"] = users_mod["uri"].apply(lambda x: x if pd.notnull(x)  else None)
    users_mod["language"] = users_mod["language"].apply(lambda x: x if pd.notnull(x)  else "fr")
    users_mod["password"] = users_mod.apply(
        (lambda row: row["password"] if pd.notnull(row["password"])
         else users_mod.loc[row.name,"first_name"].lower()),
        axis=1
    )
    users_mod["admin"] = users_mod["admin"].apply(lambda x: False if not x==True else x)

    # Api client to create users
    sec_api = opensilexClientToolsPython.SecurityApi(python_client)

    # Creating the users
    for index,row in users_mod.iterrows():
        no_missing = True
        for col in no_default:
            if (not col in users_mod.columns or not pd.notnull(row[col])):
                logging.error(
                    "Exception : On row {} value for '{}' is missing"\
                        .format(index, col)
                )
                no_missing = False

        if no_missing:
            new_user = opensilexClientToolsPython.UserCreationDTO(**row)
            
            try :
                sec_api.create_user(body=new_user)
            except Exception as e:
                logging.error(
                    "Exception : {}"\
                        .format(e)
                )


# %%
