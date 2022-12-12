import pandas as pd
import yaml
from .data import _Data
from .utils import get_args_yaml
from .client import _Client
from .variables import _Entity, _Method, _Characteristic, _Unit, _Variable



class OpensilexClient(_Data):

    # TODO : Add a list delete in case of an error?
    # using GET /ontology/uris_labels to get

    def __init__(self, config_file : str = "default_config.yaml", host : str = False, identifier : str = False, password : str = False, verbose : bool = False):
        # TODO : Create method to update/create config
        if host or identifier or password:
            if host and identifier and password:
                self.client = _Client(host=host, identifier=identifier, password=password, verbose=verbose)
            else:
                raise ValueError("If any of the `host`, `identifier` or `password` arguments are used, all of them must be used")
        else:
            try:
                with open("../opensilex_configs/"+config_file, "r") as file:
                    config = yaml.safe_load(file)
            except FileNotFoundError:
                try:
                    with open(config_file, "r") as file:
                        config = yaml.safe_load(file)
                except FileNotFoundError:
                    raise FileNotFoundError("The file couldn't be found at the specified path and the opensilex_configs directory")
            self.client = _Client(**config, verbose=verbose)
        self.entity = _Entity(authenticated_client=self.client)
        self.method = _Method(authenticated_client=self.client)
        self.characteristic = _Characteristic(authenticated_client=self.client)
        self.unit = _Unit(authenticated_client=self.client)
        self.variable = _Variable(authenticated_client=self.client)
        # TODO: same for data

    
    def import_data_and_files(self, params_yaml : str, csv_file_or_url : str):

        # NOTE : "prov_used" is only supported with data that is created in this file,
        # not with data that is already on the instance.
        # TODO : to allow this I should try to match unique keys (prov, date, target) but that'd be really slow.
        
        # The methods used here are inherited from the _Data class

        # TODO : Should modify to use utils.dict_row_parser

        # TODO : Split this in multiple methods (delegate part to new Data class?)
        # design : pass the dataframe and relevant info to new Data class and get
        # back a list of dto or list of dict with params for datafiles.
        # Call the new Client class to create data/file on opensilex

        # Importing the necessary files
        df = pd.read_csv(csv_file_or_url)
        import_args = get_args_yaml(filename=params_yaml)
        
        done = []
        # Necessary for the use of "prov_used"
        while len(done) != len(import_args["data"]):
            previous_done = len(done)
            for i in range(len(import_args["data"])):
                if import_args["data"][i] not in done:
                    d = import_args["data"][i]

                    # If there is no prov_used the data/datafile can be created directly
                    if "prov_used" not in d["other"].keys():
                        col_to_keep = {**d["columns"], **import_args["general"]["columns"]}
                        cropped_df = df[[*col_to_keep.values()]]
                        cropped_df = cropped_df.rename(columns={v:k for k,v in col_to_keep.items()})
                        res = self.import_data_or_files(cropped_df, **d["other"], **import_args["general"]["other"])
                        df["uri"+str(i)] = res
                        done.append(import_args["data"][i])

                    else:
                        prov_used_uris_cols = []
                        for prov_used in [x for x in d["other"]["prov_used"]]:
                            for j in range(len(import_args["data"])):
                                if prov_used["file_name"] in list(import_args["data"][j]["columns"].values()):
                                    prov_used_uris_cols.append({"uri":"uri"+str(j),"rdf_type":prov_used["rdf_type"]})
                        # If all necessary prov_used haven't been created the data/datafile can't be created
                        if len(prov_used_uris_cols) == len(d["other"]["prov_used"]):
                            d["other"]["prov_used"] = prov_used_uris_cols
                            col_to_keep = {**d["columns"], **import_args["general"]["columns"]}
                            cropped_df = df[[
                                *col_to_keep.values(), 
                                *[x["uri"] for x in prov_used_uris_cols]
                            ]]
                            cropped_df = cropped_df.rename(columns={v:k for k,v in col_to_keep.items()})
                            res = self.import_data_or_files(cropped_df, **import_args["general"]["other"], **d["other"])
                            df["uri"+str(i)] = res
                            done.append(import_args["data"][i])
            if previous_done == len(done):
                raise ValueError("Some prov_used can't be found. Currently done:\n{}".format(done))
        return df

