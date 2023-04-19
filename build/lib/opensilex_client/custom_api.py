import opensilexClientToolsPython as oCTP
from .client import _Client
from .utils import get_args_yaml, df_from_file_or_url, return_if_exists, format_regex, method_arg_parser
from abc import ABC, abstractmethod
import pandas as pd
from typing import Union
from csv import DictReader
from copy import deepcopy
import re




class _CustomApi(ABC):

    def __init__(self, authenticated_client : _Client):
        self.ontology_api = oCTP.OntologyApi(api_client=self._client._generated_client)
        self.api
        self._search_to_wrap
        self._get_details_to_wrap
        self._create_to_wrap

    def search(self, row : pd.Series, dict_to_parse : dict):
        self._client._connect_if_necessary()
        row_copy = deepcopy(row)
        dict_to_parse_copy = deepcopy(dict_to_parse)
        if return_if_exists(["columns", "name"], dict_to_parse_copy):
            if row_copy[dict_to_parse_copy["columns"]["name"]]:
                row_copy[dict_to_parse_copy["columns"]["name"]] = format_regex(str_to_match=row_copy[dict_to_parse_copy["columns"]["name"]])
            else:
                return {"result": "No name given"}
        elif return_if_exists(["other", "name"], dict_to_parse_copy):
            dict_to_parse_copy["other"]["name"] = format_regex(str_to_match=dict_to_parse_copy["other"]["name"])
        else:
            return {"result": "No name given"}
        return self._search_to_wrap(
            **method_arg_parser(
                row=row_copy, dict_to_parse=dict_to_parse_copy, 
                method_to_parse_arg_for=self._search_to_wrap
            )
        )
    
    def get_details(self, uri : str):
        self._client._connect_if_necessary()
        return self._get_details_to_wrap(uri=str(uri))
    
    def create(self, row : pd.Series, dict_to_parse : dict):
        self._client._connect_if_necessary()
        return self._create_to_wrap(
            **method_arg_parser(
                row=row, dict_to_parse=dict_to_parse, 
                method_to_parse_arg_for=self._create_to_wrap
            )
        )
    
    def update(self, row : pd.Series, dict_to_parse : dict):
        self._client._connect_if_necessary()
        return self._update_to_wrap(
            **method_arg_parser(
                row=row, dict_to_parse=dict_to_parse, 
                method_to_parse_arg_for=self._update_to_wrap
            )
        )

    def check_uri_exists(self, uri : str):
        # TODO : I should probably change this to the get_by_uri service
        # I get a false positive if this uri was declared as a skos match
        res = self.ontology_api.get_uri_label(uri = uri)["result"]
        return res != uri

    def uri_exists(self, row : pd.Series, dict_to_parse : dict):
        self._client._connect_if_necessary()
        if return_if_exists(list_of_keys=["columns", "uri"], object_to_explore=dict_to_parse):
            uri = row[dict_to_parse["columns"]["uri"]]
            if uri:
                res = self.check_uri_exists(uri=uri)
                if res:
                    return uri
        elif return_if_exists(list_of_keys=["other", "uri"], object_to_explore=dict_to_parse):
            uri = dict_to_parse["other"]["uri"]
            if uri:
                res = self.check_uri_exists(uri=uri)
                if res:
                    return uri
        else:
            return False

    # TODO : Split in row_create_if_not_exists and create_if_not_exists

    def create_if_not_exists(self, row : pd.Series, dict_to_parse : dict):
        self._client._connect_if_necessary()

        existing_uri = self.uri_exists(row=row, dict_to_parse=dict_to_parse)
        if existing_uri:
            existing = self.get_details(
                uri=existing_uri
            )["result"].to_dict()
            existing["status"] = "already_existed"
            return existing

        existing = self.search(row=row, dict_to_parse=dict_to_parse)["result"]
        if existing == "No name given":
            return {"status":"No name given"}
        if len(existing) == 1:
            existing = self.get_details(
                uri=existing[0].uri
            )["result"].to_dict()
            existing["status"] = "already_existed"
            return existing
        elif len(existing) > 1:
            return {"status":"Multiple results found"}
        else:
            created = self.create(row=row, dict_to_parse=dict_to_parse)["result"][0]
            created_dict = self.get_details(uri=created)["result"].to_dict()
            created_dict["status"] = "created"
            return created_dict

    def df_create_if_not_exists(self, df : pd.DataFrame, import_args : dict):
        self._client._connect_if_necessary()
        res_series = df.apply(func = self.create_if_not_exists, axis=1, dict_to_parse=import_args)
        res_df = pd.DataFrame(res_series.to_list())
        res_df.to_csv(f"./{self.__class__.__name__}_results.csv")
        return res_df

    def sheet_create_if_not_exists(self, path_or_url_to_sheet : str, yaml_path : str):
        self._client._connect_if_necessary()
        df = df_from_file_or_url(path_or_url_to_sheet)
        import_args = get_args_yaml(filename=yaml_path)
        if "columns" not in import_args.keys() or not import_args["columns"]:
            import_args["columns"] = {}
        if "other" not in import_args.keys() or not import_args["other"]:
            import_args["other"] = {}
        return self.df_create_if_not_exists(df=df, import_args=import_args)