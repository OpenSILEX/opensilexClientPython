# The following code defines a set of classes that provide an interface for interacting with the OpenSILEX
# API to manage variables, entities, methods, characteristics, units, and groups.
import opensilexClientToolsPython as oCTP
import pandas as pd
from .utils import get_args_yaml, df_from_file_or_url, return_if_exists
from .custom_api import _CustomApi
from .client import _Client



# The `_Variable` class is a subclass of `_CustomApi` that provides methods for
# searching, getting details, and creating variables on an OpenSILEX instance.
class _Variable(_CustomApi):

    def __init__(self, authenticated_client : _Client):
        '''The method initializes an object with various attributes and sets up API endpoints for
        interacting with variables.
        
        Parameters
        ----------
        authenticated_client : _Client
            The parameter "authenticated_client" is an instance of the "_Client" class, which represents an
            authenticated client for accessing an OpenSILEX API.
        
        '''
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
        # self._elements describes the different elements of a variable and their
        # corresponding management classes in the _Variable classes parameters
        self._elements = [
            {"element_name": "entity", "element_attribute": self._entity},
            {"element_name": "entity_of_interest", "element_attribute": self._entity_of_interest},
            {"element_name": "method", "element_attribute": self._method},
            {"element_name": "characteristic", "element_attribute": self._characteristic},
            {"element_name": "unit", "element_attribute": self._unit}
        ]

    
    def create_if_not_exists(self, row : pd.Series, dict_to_parse : dict) -> pd.Series:
        '''The method `create_if_not_exists` checks if a variable already exists and creates it if it
        doesn't.
        
        Parameters
        ----------
        row : pd.Series
            The `row` parameter is a pandas Series object that represents a variable.
        dict_to_parse : dict
            The `dict_to_parse` parameter is a dictionary that contains the mapping between the column
            names in the `row` parameter and the corresponding element names in the `_elements` list. This
            mapping is used to determine which elements need to be present in the `row` for the creation
            process.
        
        Returns
        -------
            a pandas Series object. The content of the returned Series object depends on the conditions and
            logic within the function. It could be a modified version of the input `row` Series object, an
            existing variable details dictionary, or a newly created variable details dictionary.
        
        '''
        self._client._connect_if_necessary()

        # Check that all mandatory elements were guiven
        for elem in self._elements:
            if elem["element_name"] != "entity_of_interest" and not row[dict_to_parse["columns"][elem["element_name"]]]:
                return {"status": "Not created because the {} is missing".format(elem["element_name"])}

        # If the variable with this uri already exists return its details
        existing_uri = self.uri_exists(row=row, dict_to_parse=dict_to_parse)
        if existing_uri:
            existing = self.get_details(
                uri=existing_uri
            )["result"].to_dict()
            existing["status"] = "A variable with this uri already exists and will be used"
            return existing

        existing = self.search(row=row, dict_to_parse=dict_to_parse)["result"]
        if existing == "No name given":
            return {"status": "No name given for variable"}
        if len(existing) == 1:
            existing = self.get_details(
                uri=existing[0].uri
            )["result"].to_dict()
            existing["status"] = "A variable with this name already exists and will be used"
            return existing
        elif len(existing) > 1:
            return {"status": "Multiple variables with this name found"}
        else:
            # Create the variable and return its detail
            created = self.create(row=row, dict_to_parse=dict_to_parse)["result"][0]
            created_dict = self.get_details(uri=created)["result"].to_dict()
            created_dict["status"] = "Variable created"
            return created_dict

    def df_create_if_not_exists(self, df : pd.DataFrame, import_args : dict, group : bool = False) -> pd.DataFrame:
        '''The method `df_create_if_not_exists` takes a DataFrame, import arguments, and a group flag as
        input, and creates new variables and groups based on the import arguments if they do not already
        exist on the OpenSILEX instance. It then returns the modified DataFrame and saves it as a CSV file.
        
        Parameters
        ----------
        df : pd.DataFrame
            The `df` parameter is a pandas DataFrame that represents the variables and groups to create.
        import_args : dict
            The `import_args` parameter is a dictionary that contains the arguments needed for creating variables. 
            It is used to determine how the data should be imported and processed. It includes general informations
            in the "other" key as well as mapping infomations in the "columns" key.
        group : bool, optional
            The `group` parameter is a boolean flag that indicates whether or not to create groups based on
            the data in the DataFrame. If `group` is set to `True`, the method will create groups using
            the specified import arguments. If `group` is set to `False`, no groups will be created.
        
        Returns
        -------
            a pandas DataFrame `res_df` representing the result of its execution.
        
        '''
        
        # Create a variable for each row and format the returned DataFrame
        self._client._connect_if_necessary()
        res_df = df.apply(func = self.create_if_not_exists, axis=1, dict_to_parse=import_args)
        res_df = pd.DataFrame(list(res_df))
        res_df = res_df.where(pd.notnull(res_df), None)
        print(f"DF = \n{res_df}")

        # Groups generation
        if "uri" in res_df.columns and not res_df.uri.empty and group:
            # TODO : handle mix of columns for groups and general groups
            general_group_import_args = return_if_exists(list_of_keys=["other", "group"], object_to_explore=import_args)
            column_group_import_args = return_if_exists(list_of_keys=["columns", "group"], object_to_explore=import_args)
            group_import_args = {}
            if general_group_import_args:
                group_import_args["other"] = general_group_import_args
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
            created_groups_df = created_groups_df.add_prefix("group_")
            res_df = pd.concat([res_df, created_groups_df], axis=1)

        res_df.to_csv(f"./{self.__class__.__name__}_results.csv")
        return res_df

    def sheet_create_if_not_exists(self, path_or_url_to_sheet : str, yaml_path : str, group : bool = False) -> pd.DataFrame:
        '''The method `sheet_create_if_not_exists` creates variables if they don't already exist,
        using the provided path or URL to the sheet and a YAML file for import arguments.
        
        Parameters
        ----------
        path_or_url_to_sheet : str
            The parameter `path_or_url_to_sheet` is a string that represents the path or URL to the sheet
            that describes the variables to create.
        yaml_path : str
            The `yaml_path` parameter is a string that represents the path to a YAML file containing hte mapping
            for the columns of the sheet as well as more global parameters for variables creation.
        group : bool, optional
            The `group` parameter is a boolean flag that indicates whether or not to create groups based on
            the data in the DataFrame. If `group` is set to `True`, the method will create groups using
            the specified import arguments. If `group` is set to `False`, no groups will be created.
        
        Returns
        -------
            a pandas DataFrame `res_df` representing the result of its execution.
        
        '''
        
        # Import the sheet as a DataFrame object and complete the import_args if
        # one of the keys is missing
        self._client._connect_if_necessary()
        df = df_from_file_or_url(path_or_url_to_sheet)
        import_args = get_args_yaml(filename=yaml_path)
        if "columns" not in import_args.keys() or not import_args["columns"]:
            import_args["columns"] = {}
        if "other" not in import_args.keys() or not import_args["other"]:
            import_args["other"] = {}
        
        # Create or get the necessary elements
        for elem in self._elements:
            # Get the element's import_args
            element_import_args = {
                key : (
                    import_args[key][elem["element_name"]] if return_if_exists(list_of_keys=[key, elem["element_name"]], object_to_explore=import_args) else {}
                )
                for key in ["columns", "other"]
            }
            # Create the element or get its detail if it already exists
            element = elem["element_attribute"].df_create_if_not_exists(df=df, import_args=element_import_args)
            created_col_uri = "created_" + elem["element_name"] + "_uri"
            created_col_status = "created_" + elem["element_name"] + "_status"
            df[created_col_uri] = element.apply(func = lambda x: x["uri"] if x["status"] in ["already_existed", "created"] else None, axis=1)
            df[created_col_status] = element.apply(func = lambda x: x["status"], axis=1)
            import_args["columns"][elem["element_name"]] = created_col_uri
            if elem["element_name"] in import_args["other"].keys():
                del import_args["other"][elem["element_name"]]
        return self.df_create_if_not_exists(df=df, import_args=import_args, group=group)


# The `_Entity` class is a subclass of `_CustomApi` that provides methods for
# searching, getting details, and creating entities on an OpenSILEX instance.
class _Entity(_CustomApi):

    def __init__(self, authenticated_client : _Client):
        self._client=authenticated_client
        self.api = oCTP.VariablesApi(api_client=self._client._generated_client)
        self._search_to_wrap = self.api.search_entities
        self._get_details_to_wrap = self.api.get_entity
        self._create_to_wrap = self.api.create_entity
        super().__init__(authenticated_client=authenticated_client)


# The `_EntityOfInterest` class is a subclass of `_CustomApi` that provides methods for
# searching, getting details, and creating entities of interest on an OpenSILEX instance.
class _EntityOfInterest(_CustomApi):

    def __init__(self, authenticated_client : _Client):
        self._client=authenticated_client
        self.api = oCTP.VariablesApi(api_client=self._client._generated_client)
        self._search_to_wrap = self.api.search_interest_entity
        self._get_details_to_wrap = self.api.get_interest_entity
        self._create_to_wrap = self.api.create_interest_entity
        super().__init__(authenticated_client=authenticated_client)


# The `_Method` class is a subclass of `_CustomApi` that provides methods for
# searching, getting details, and creating methods on an OpenSILEX instance.
class _Method(_CustomApi):

    def __init__(self, authenticated_client : _Client):
        self._client=authenticated_client
        self.api = oCTP.VariablesApi(api_client=self._client._generated_client)
        self._search_to_wrap = self.api.search_methods
        self._get_details_to_wrap = self.api.get_method
        self._create_to_wrap = self.api.create_method
        super().__init__(authenticated_client=authenticated_client)


# The `_Characteristic` class is a subclass of `_CustomApi` that provides methods for
# searching, getting details, and creating characteristics on an OpenSILEX instance.
class _Characteristic(_CustomApi):

    def __init__(self, authenticated_client : _Client):
        self._client=authenticated_client
        self.api = oCTP.VariablesApi(api_client=self._client._generated_client)
        self._search_to_wrap = self.api.search_characteristics
        self._get_details_to_wrap = self.api.get_characteristic
        self._create_to_wrap = self.api.create_characteristic
        super().__init__(authenticated_client=authenticated_client)


# The `_Unit` class is a subclass of `_CustomApi` that provides methods for
# searching, getting details, and creating units on an OpenSILEX instance.
class _Unit(_CustomApi):

    def __init__(self, authenticated_client : _Client):
        self._client=authenticated_client
        self.api = oCTP.VariablesApi(api_client=self._client._generated_client)
        self._search_to_wrap = self.api.search_units
        self._get_details_to_wrap = self.api.get_unit
        self._create_to_wrap = self.api.create_unit
        super().__init__(authenticated_client=authenticated_client)


# The `_Group` class is a subclass of `_CustomApi` that provides methods for
# searching, getting details, and creating groups on an OpenSILEX instance.
class _Group(_CustomApi):

    def __init__(self, authenticated_client : _Client):
        self._client=authenticated_client
        self.api = oCTP.VariablesApi(api_client=self._client._generated_client)
        self._search_to_wrap = self.api.search_variables_groups
        self._get_details_to_wrap = self.api.get_variables_group
        self._create_to_wrap = self.api.create_variables_group
        self._update_to_wrap = self.api.update_variables_group
        super().__init__(authenticated_client=authenticated_client)
