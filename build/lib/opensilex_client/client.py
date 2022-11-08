import opensilexClientToolsPython as osCP
from datetime import datetime, timedelta
from .utils import retry
import json
from typing import List



class _Client():
    """Hidden class that intarfaces between the client defined in this package
and the one defined in the opensilexClientToolsPython package"""

    def __init__(self, host : str, identifier : str, password : str):
        self.host = host
        self.identifier = identifier
        self.password = password

        self._generated_client = osCP.ApiClient()
        self._generated_client.connect_to_opensilex_ws(
            host=self.host, 
            identifier=self.identifier, 
            password=self.password
        )
        self.connection_time = datetime.now()
        self.data_api = osCP.DataApi(api_client=self._generated_client)
        

    # In case of error retry it every 15 minutes 5 times
    @retry(delay=15*60)
    def _connect_if_necessary(self, force : bool = False):
        """
        If the last connection was more than 30 minutes ago, or if the force parameter is set to True,
        connect to the webservice
        
        :param force: if True, the connection will be forced even if the last connection was less than
        30 minutes ago, defaults to False
        :type force: bool (optional)
        """
        if datetime.now() - self.connection_time > timedelta(0,1800) or force == True:
            self._generated_client.connect_to_opensilex_ws(
                host=self.host, 
                identifier=self.identifier, 
                password=self.password
            )
            self.connection_time = datetime.now()

    def _send_data(self, data_dto_list : List[osCP.DataCreationDTO]):
        self._connect_if_necessary()
        return self.data_api.add_list_data(body = data_dto_list)

    def _send_datafile(self, description : dict, file_path : str):
        self._connect_if_necessary()
        return self.data_api.post_data_file(
            description=json.dumps(description), 
            file=file_path
        )
