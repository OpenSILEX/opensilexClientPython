import opensilexClientToolsPython as oCTP
import pandas as pd
from .utils import get_args_yaml, df_from_file_or_url, return_if_exists, format_regex, method_arg_parser
from .custom_api import _CustomApi
from .client import _Client
from copy import deepcopy



class _Variable(_CustomApi):

    def __init__(self, authenticated_client : _Client):
        self._client=authenticated_client
        self.api = oCTP.VariablesApi(api_client=self._client._generated_client)
        self._search_to_wrap = self.api.search_variables
        self._get_details_to_wrap = self.api.get_variable
        self._create_to_wrap = self.api.create_variable
        super().__init__(authenticated_client=authenticated_client)
        self._entity = _Entity(authenticated_client=authenticated_client)
        self._entity_of_interest = _EntityOfInterest(authenticated_client=authenticated_client)
        self._method = _Method(authenticated_client=authenticated_client)
        self._characteristic = _Characteristic(authenticated_client=authenticated_client)
        self._unit = _Unit(authenticated_client=authenticated_client)
        self._group = _Group(authenticated_client=authenticated_client)
        self.datatypes = self.api.get_datatypes()["result"]
        self._elements = [
            {"element_name": "entity", "element_attribute": self._entity},
            {"element_name": "entity_of_interest", "element_attribute": self._entity_of_interest},
            {"element_name": "method", "element_attribute": self._method},
            {"element_name": "characteristic", "element_attribute": self._characteristic},
            {"element_name": "unit", "element_attribute": self._unit}
        ]

    
    def create_if_not_exists(self, row : pd.Series, dict_to_parse : dict):
        self._client._connect_if_necessary()

        if not all([
            row[elem["element_name"]] if elem["element_name"] != "entity_of_interest"
            else True
            for elem in self._elements
        ]):
            return {"status":"Not created because of missing element"}

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

    def df_create_if_not_exists(self, df : pd.DataFrame, import_args : dict, group : bool = False):
        self._client._connect_if_necessary()
        res_series = df.apply(func = self.create_if_not_exists, axis=1, dict_to_parse=import_args)
        res_df = pd.DataFrame(res_series.to_list())
        res_df = res_df.where(pd.notnull(res_df), None)
        if group:
            general_group_import_args = return_if_exists(list_of_keys=["other", "group"], object_to_explore=import_args)
            column_group_import_args = return_if_exists(list_of_keys=["columns", "group"], object_to_explore=import_args)
            if general_group_import_args:
                group_import_args = {
                    "columns":{},
                    "other":general_group_import_args
                }
                group_import_args["other"]["variables"] = [x for x in res_df.uri.unique().tolist() if x]
                created_groups_df = self._group.df_create_if_not_exists(df=res_df, import_args=group_import_args)

                if column_group_import_args:
                    group_import_args = {
                        "columns":column_group_import_args,
                        "other":{}
                    }
                    # TODO : handle case with uri=None (when a variable wasn't created)
                    created_groups_df2 = self._group.df_create_if_not_exists(df=res_df, import_args=group_import_args)
                    created_groups_df = pd.concat([created_groups_df, created_groups_df2], axis=1)
            elif column_group_import_args:
                group_import_args = {
                    "columns":column_group_import_args,
                    "other":{}
                }
                # TODO : handle case with uri=None (when a variable wasn't created)
                created_groups_df = self._group.df_create_if_not_exists(df=res_df, import_args=group_import_args)
            else:
                raise ValueError("The group option was set to True but no group info was given")
            # TODO change names of columns (group.column_name)
            res_df = pd.concat([res_df, created_groups_df], axis=1)
        res_df.to_csv(f"./{self.__class__.__name__}_results.csv")
        return res_df

    def sheet_create_if_not_exists(self, path_or_url_to_sheet : str, yaml_path : str, group : bool = False):
        self._client._connect_if_necessary()
        df = df_from_file_or_url(path_or_url_to_sheet)
        import_args = get_args_yaml(filename=yaml_path)
        if "columns" not in import_args.keys() or not import_args["columns"]:
            import_args["columns"] = {}
        if "other" not in import_args.keys() or not import_args["other"]:
            import_args["other"] = {}
        
        for elem in self._elements:
            element_import_args = {
                key : (
                    import_args[key][elem["element_name"]] if return_if_exists(list_of_keys=[key, elem["element_name"]], object_to_explore=import_args) else {}
                )
                for key in ["columns", "other"]
            }
            element = elem["element_attribute"].df_create_if_not_exists(df=df, import_args=element_import_args)
            df[elem["element_name"]] = element.apply(func = lambda x: x["uri"] if x["status"] in ["already_existed", "created"] else None, axis=1)
            import_args["columns"][elem["element_name"]] = elem["element_name"]
            if elem["element_name"] in import_args["other"].keys():
                del import_args["other"][elem["element_name"]]
        return self.df_create_if_not_exists(df=df, import_args=import_args, group=group)

class _Entity(_CustomApi):

    def __init__(self, authenticated_client : _Client):
        self._client=authenticated_client
        self.api = oCTP.VariablesApi(api_client=self._client._generated_client)
        self._search_to_wrap = self.api.search_entities
        self._get_details_to_wrap = self.api.get_entity
        self._create_to_wrap = self.api.create_entity
        super().__init__(authenticated_client=authenticated_client)


class _EntityOfInterest(_CustomApi):

    def __init__(self, authenticated_client : _Client):
        self._client=authenticated_client
        self.api = oCTP.VariablesApi(api_client=self._client._generated_client)
        self._search_to_wrap = self.api.search_interest_entity
        self._get_details_to_wrap = self.api.get_interest_entity
        self._create_to_wrap = self.api.create_interest_entity
        super().__init__(authenticated_client=authenticated_client)


class _Method(_CustomApi):

    def __init__(self, authenticated_client : _Client):
        self._client=authenticated_client
        self.api = oCTP.VariablesApi(api_client=self._client._generated_client)
        self._search_to_wrap = self.api.search_methods
        self._get_details_to_wrap = self.api.get_method
        self._create_to_wrap = self.api.create_method
        super().__init__(authenticated_client=authenticated_client)


class _Characteristic(_CustomApi):

    def __init__(self, authenticated_client : _Client):
        self._client=authenticated_client
        self.api = oCTP.VariablesApi(api_client=self._client._generated_client)
        self._search_to_wrap = self.api.search_characteristics
        self._get_details_to_wrap = self.api.get_characteristic
        self._create_to_wrap = self.api.create_characteristic
        super().__init__(authenticated_client=authenticated_client)


class _Unit(_CustomApi):

    def __init__(self, authenticated_client : _Client):
        self._client=authenticated_client
        self.api = oCTP.VariablesApi(api_client=self._client._generated_client)
        self._search_to_wrap = self.api.search_units
        self._get_details_to_wrap = self.api.get_unit
        self._create_to_wrap = self.api.create_unit
        super().__init__(authenticated_client=authenticated_client)


class _Group(_CustomApi):

    def __init__(self, authenticated_client : _Client):
        self._client=authenticated_client
        self.api = oCTP.VariablesApi(api_client=self._client._generated_client)
        self._search_to_wrap = self.api.search_variables_groups
        self._get_details_to_wrap = self.api.get_variables_group
        self._create_to_wrap = self.api.create_variables_group
        super().__init__(authenticated_client=authenticated_client)
