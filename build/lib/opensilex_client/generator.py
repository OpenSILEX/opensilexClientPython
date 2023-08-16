from datetime import datetime
from datetime import timedelta
import pytz
import tzlocal
import opensilexClientToolsPython as osCP
from random import uniform, choices
import string
from typing import List


class _Generator():

    def __init__(self, parent):

        self.parent = parent


    def prefixed_random_string(self, prefix : str, length : int) -> str:
        return prefix + "_" + "".join(choices(string.ascii_uppercase + string.ascii_lowercase + string.digits, k=length))

    

# def generate_data_creation_dto_list(metadata_to_send : dict, associated_with : bool, n_data_to_generate : int, current_count : int = 0) -> List[osCP.DataCreationDTO]:
#     """
#     It generates a list of `DataCreationDTO` objects, each with a different timestamp, and with the same
#     target, variable, provenance, and device (if associated_with is True)
    
#     :param metadata_to_send: a dictionary containing the metadata to send with the data
#     :type metadata_to_send: dict
#     :param associated_with: Whether or not the data should be associated with a device
#     :type associated_with: bool
#     :param n_data_to_generate: the number of data points to generate
#     :type n_data_to_generate: int
#     :param current_count: The number of data points that have already been generated. This is used to
#     ensure that the data points have unique timestamps, defaults to 0
#     :type current_count: int (optional)
#     :return: A list of DataCreationDTO objects
#     """

#     # Timezones
#     utc = pytz.timezone('UTC')
#     local = tzlocal.get_localzone()

#     # generate data
#     base_date = utc.localize(datetime.now()).astimezone(local)
#     dto_list = [
#         osCP.DataCreationDTO(
#             _date=(base_date + timedelta(0,i)).isoformat('T'), 
#             target=metadata_to_send["target"], 
#             variable=metadata_to_send["variable"], 
#             value=uniform(-100, 100),
#             provenance=osCP.DataProvenanceModel(
#                 uri=metadata_to_send["provenance"],
#                 prov_was_associated_with=[
#                     osCP.ProvEntityModel(
#                         uri=metadata_to_send["device"], 
#                         rdf_type=metadata_to_send["device_type"]
#                     )
#                 ]
#             )
#         )
#         if associated_with
#         else
#         osCP.DataCreationDTO(
#             _date=(base_date + timedelta(0,i)).isoformat('T'), 
#             target=metadata_to_send["target"], 
#             variable=metadata_to_send["variable"], 
#             value=uniform(-100, 100),
#             provenance=osCP.DataProvenanceModel(
#                 uri=metadata_to_send["provenance"]
#             )
#         )
#         for i in range(current_count, current_count + n_data_to_generate)
#     ]

#     return dto_list


# def generate_entity_dto():
#     rds = random_string(length=10)
#     return osCP.EntityCreationDTO(
#         name="Script_Entity_" + rds, 
#         description=rds, 
#         exact_match=[rds], 
#         close_match=[rds], 
#         broad_match=[rds], 
#         narrow_match=[rds]
#     )

# # TODO entity of interest once the package is updated

# def generate_characteristic_dto():
#     rds = random_string(length=10)
#     return osCP.CharacteristicCreationDTO(
#         name="Script_Characteristic_" + rds, 
#         description=rds, 
#         exact_match=[rds], 
#         close_match=[rds], 
#         broad_match=[rds], 
#         narrow_match=[rds]
#     )

# def generate_method_dto():
#     rds = random_string(length=10)
#     return osCP.MethodCreationDTO(
#         name="Script_Method_" + rds, 
#         description=rds, 
#         exact_match=[rds], 
#         close_match=[rds], 
#         broad_match=[rds], 
#         narrow_match=[rds]
#     )

# def generate_unit_dto():
#     rds = random_string(length=10)
#     return osCP.UnitCreationDTO(
#         name="Script_Unit_" + rds, 
#         description=rds, 
#         symbol=rds, 
#         alternative_symbol=rds, 
#         exact_match=[rds], 
#         close_match=[rds], 
#         broad_match=[rds], 
#         narrow_match=[rds]
#     )

# # Should it be passed to the generator directly? can get it from a call to Variables/datatypes
# DATATYPES = [
#     "datatypes.boolean",
#     "datatypes.date",
#     "datatypes.datetime",
#     "datatypes.decimal",
#     "datatypes.number",
#     "datatypes.string"
# ]

# def generate_variable_dto(entity, entity_of_interest, characteristic, method, unit):
#     rds = random_string(length=10)
#     return osCP.VariableCreationDTO(
#         name="Script_Variable_" + rds,
#         alternative_name=rds,
#         description=rds,
#         entity=entity,
#         entity_of_interest=None, # TODO implement this : entity_of_interest,
#         characteristic=characteristic,
#         trait=rds,
#         trait_name=rds,
#         method=method,
#         unit=unit,
#         species=None, # TODO implement this
#         datatype=choices(DATATYPES),
#         time_interval=None, # TODO implement this
#         sampling_interval=None, # TODO implement this
#         exact_match=[rds],
#         close_match=[rds],
#         broad_match=[rds],
#         narrow_match=[rds],
#     )

# TODO create other elements such as variables, objects, devices, etc.. (keep a single prov for easy delete?)