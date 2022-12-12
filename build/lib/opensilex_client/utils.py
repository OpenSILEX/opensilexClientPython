import pandas as pd
import numpy as np
import time
import decorator
import yaml
from datetime import datetime, timedelta
from typing import Any
import typing
from csv import DictReader
import urllib.request as urlreq
from re import escape

def retry(howmany : int = 5, delay : int = 5) -> decorator.decorator:

    @decorator.decorator
    def try_it(func, delay=delay, *fargs, **fkwargs):
        for i in range(howmany):
            try:
                return func(*fargs, **fkwargs)
            except Exception as e:
                if i == howmany - 1:
                    raise e
                else:
                    time.sleep(delay)
    return try_it


def is_float(element : Any) -> bool:

    try:
        np.float64(element)
        return True
    except ValueError:
        return False


def get_args_yaml(filename : str) -> dict:

    with open(filename, "r") as file:
        args = yaml.safe_load(file)
    
    return args


def timeparser(str_time : str) -> float:
    
    d = datetime.strptime(str_time, '%H:%M:%S.%f')
    return timedelta(hours=d.hour, minutes=d.minute, seconds=d.second, microseconds=d.microsecond).total_seconds()


def is_empty(value: str) -> bool:

    # Check for None, or equivalents : "NA", "nan"
    if value is None:
        return True
    if(value == "NA" or value == None or value == "nan"):
        return True
    return False

def return_if_exists(list_of_keys:list, object_to_explore):
    try:
        return_object = object_to_explore.copy()
        for k in list_of_keys:
            return_object = return_object[k]
        if return_object:
            return return_object
        else:
            return None
    except KeyError:
        return None
        

def arg_from_row_dict(row : pd.Series, dict_to_parse : dict, arg: str):
    if arg == "prov_used":
        return None
    col_name = return_if_exists(list_of_keys=["columns", arg], object_to_explore=dict_to_parse)
    arg_from_dict = return_if_exists(list_of_keys=["other", arg], object_to_explore=dict_to_parse)
    
    if col_name and arg_from_dict:
        raise ValueError(f"value for arg '{arg}' found in both 'columns' and 'other'")
    elif type(col_name) == dict:
        to_return = col_name
    elif col_name:
        to_return = row[col_name]
    elif arg_from_dict:
        to_return = arg_from_dict
    else:
        return None
    
    if type(to_return) == pd.Series:
        return list(to_return)
    else:
        return to_return
    

def method_arg_parser(row : pd.Series, dict_to_parse : dict, method_to_parse_arg_for):
    """expects a dict of format {"columns":{}, "other":{}} "prov_used" needs to be handled afterwards"""
    # TODO : split this in multiple funcs
    new_dict = {}

    # if isinstance(method_to_parse_arg_for, types.FunctionType) or isinstance(method_to_parse_arg_for, types.MethodType):
    type_map = typing.get_type_hints(method_to_parse_arg_for)
    for arg, arg_type in type_map.items():

        if "Optional" in str(arg_type):
            arg_type = typing.get_args(arg_type)[0]


        if arg == "body":
            new_dict[arg] = arg_type(
                **method_arg_parser(row=row, dict_to_parse=dict_to_parse, method_to_parse_arg_for=arg_type.__init__)
            )
        
        else:
            # if complex type (ie : List, Dict, etc) returns the native equivalent (ie : list, dict, etc...) else returns None
            origin_type = typing.get_origin(arg_type)
                
            # Look for the arg in the row via dict mapping or in the dict
            arg_value = arg_from_row_dict(row=row, dict_to_parse=dict_to_parse, arg=arg)
            if not arg_value:
                new_dict[arg] = None
            
            elif origin_type == None:
                new_dict[arg] = arg_to_basetype(arg_type=arg_type, arg_value=arg_value, row=row)
            
            elif origin_type == list:
                list_with_nones = arg_to_list(arg_type=arg_type, arg_value=arg_value, row=row)
                no_none_list = list(filter(lambda x: x != None, list_with_nones))
                if no_none_list:
                    new_dict[arg] = no_none_list
                else:
                    new_dict[arg] = None

            elif origin_type == dict:
                new_dict[arg] = arg_to_dict(arg_type=arg_type, arg_value=arg_value, row=row)
            
            else:
                raise ValueError(f"Unsupported type {arg_type}")
    return new_dict


def arg_to_list(arg_type, arg_value, row:pd.Series):
    type_to_cast = typing.get_args(arg_type)[0]

    if "DTO" in str(type_to_cast) or "Model" in str(type_to_cast):
        if type(arg_value) == list:
            return [
                type_to_cast(
                    **method_arg_parser(row=row, dict_to_parse=dict(val), method_to_parse_arg_for=type_to_cast.__init__)
                )
                for val in arg_value
                if val != None
            ]
        else:
            return [
                type_to_cast(
                    **method_arg_parser(row=row, dict_to_parse=dict(arg_value), method_to_parse_arg_for=type_to_cast.__init__)
                )
            ]

    elif type(arg_value) == list:
        return [
            type_to_cast(val)
            for val in arg_value
            if val != None
        ]
    else:
        if arg_value:
            return [type_to_cast(arg_value)]
        else:
            return []


def arg_to_dict(arg_type, arg_value, row:pd.Series):
    dict_value = dict(arg_value)
    key_type, value_type = typing.get_args(arg_type)
    if "DTO" in str(value_type) or "Model" in str(value_type):
        return {
            key_type(k):value_type(
                **method_arg_parser(row=row, dict_to_parse=dict(v), method_to_parse_arg_for=value_type.__init__)
            )
            for k,v in dict_value.items()
        }
    # TODO : case where value_type is an opensilex type
    else:
        return {
            key_type(k):value_type(v)
            for k,v in dict_value.items()
        }
    


def arg_to_basetype(arg_type, arg_value, row:pd.Series):
    # Recursive aspect for opensilexClientToolsPython types
    if "DTO" in str(arg_type) or "Model" in str(arg_type):
        return arg_type(
            **method_arg_parser(row=row, dict_to_parse=dict(arg_value), method_to_parse_arg_for=arg_type.__init__)
        )
    else:
        return arg_type(arg_value)


def df_from_file_or_url(path_or_url_to_sheet: str) -> pd.DataFrame:

    if "http" in path_or_url_to_sheet:

        # check google sheet url
        if "docs.google.com/spreadsheets" in path_or_url_to_sheet:
            url_split = path_or_url_to_sheet.split("/")
            split_end = url_split[-1].split("gid")
            if "&format=csv" not in split_end[1]:
                split_end[1] += "&format=csv"
            url_split[-1] = "export?gid" + split_end[1]
            path_or_url_to_sheet = "/".join(url_split)
            with urlreq.urlopen(path_or_url_to_sheet) as sheet_file:
                col_names = sheet_file.readline().decode("utf-8").split(",")
                col_names[-1] = col_names[-1].replace("\r", "").replace("\n", "")
        else:
            raise ValueError(f"Couldn't recognise \"{path_or_url_to_sheet}\" as a google sheet url")

    else:
        # Ensure right colnames despite duplicates
        col_names = DictReader(open(path_or_url_to_sheet, 'r')).fieldnames
    
    df = pd.read_csv(path_or_url_to_sheet)
    df.columns = col_names
    df = df.astype(object).where(pd.notnull(df), None)
    return df

def format_regex(str_to_match : str) -> str:
    return "^" + escape(str_to_match) + "$"
