import json
# standard
import random
import re
import os

import requests
# external
from dotenv import dotenv_values
# local
from authenticate import get_halo_token


#############
# Functions #
#############

def generate_random_hex(length):
    decimals = random.choices(range(16), k=length)
    hexadecimal = "".join(["{:x}".format(decimal) for decimal in decimals])
    return hexadecimal

def get_temporary_id():

    temporary_id_components = [
        generate_random_hex(8),
        generate_random_hex(4),
        generate_random_hex(4),
        generate_random_hex(4),
        generate_random_hex(12)]

    temporary_id = "-".join(temporary_id_components)
    return temporary_id


def get_halo_asset_fields(x: dict) -> list:
    all_keys = list()
    for key, value in x.items():
        all_keys += [key]
        if key == "fields":
            all_keys += [field["field_name"] for field in value]
    return all_keys


##################################
# Set up environmental variables #
##################################

env_file_path = ".env"
# dotenv_values just reads from file without assigning variables to environment.
# use load_dotenv for the "real thing"
env_variables = dotenv_values(env_file_path)

HALO_API_ASSET_GROUP_URL = env_variables["HALO_API_ASSET_GROUP_URL"]
HALO_API_ASSET_TYPE_URL = env_variables["HALO_API_ASSET_TYPE_URL"]
HALO_API_ASSET_FIELD_URL = env_variables["HALO_API_ASSET_FIELD_URL"]
HALO_REQUIRED_ASSET_TYPES = env_variables["HALO_REQUIRED_ASSET_TYPES"]
RMM_HALO_FIELD_MAP = env_variables["RMM_HALO_FIELD_MAP"]
HALO_SYNC_INDICATOR_FIELD = env_variables["HALO_SYNC_INDICATOR_FIELD"]


#################################
# Get read assets authorization #
#################################

read_assets_token = get_halo_token("read:assets")
headers = {"Authorization": f"{read_assets_token['token_type']} {read_assets_token['access_token']}"}


#####################
# Check asset types #
#####################

asset_type_parameters = {}
asset_type_response = requests.get(
    url=HALO_API_ASSET_TYPE_URL,
    headers=headers,
    params=asset_type_parameters)
asset_types = asset_type_response.json()

existing_asset_type_names = [asset_type["name"].lower().strip() for asset_type in asset_types]
expected_asset_type_names = [asset_type.lower().strip() for asset_type in HALO_REQUIRED_ASSET_TYPES.split(",")]
missing_asset_type_names = [asset_type for asset_type in expected_asset_type_names
                            if asset_type not in existing_asset_type_names]
if missing_asset_type_names:
    log_string = f"Halo is missing the following required asset types: {', '.join(missing_asset_type_names)}! " \
                 f"You should add them in Configuration > Asset Management > Asset Type. " \
                 f"If these asset types are not added, the script will not work."
    print(log_string)
    del log_string


################
# Check fields #
################

field_parameters = {}
field_response = requests.get(
    url=HALO_API_ASSET_FIELD_URL,
    headers=headers,
    params=field_parameters)

existing_field_names = [field["name"].lower() for field in field_response.json()]
expected_field_names = [mapping.split(":")[1].lower().strip() for mapping in RMM_HALO_FIELD_MAP.split(",")]
missing_field_names = [field for field in expected_field_names
                       if field not in existing_field_names]

if missing_field_names:
    log_string = f"Halo is missing the following recommended custom fields: {', '.join(missing_field_names)}. " \
                 f"You can add the fields in Configuration > Asset Management > Asset Fields. " \
                 f"If these fields are not added, they will not be synced from RMM."
    print(log_string)
    del log_string


######################
# Check asset fields #
######################

relevant_asset_types = [asset_type for asset_type in asset_types
                        if asset_type["name"].lower() in expected_asset_type_names]

expected_asset_field_names = [mapping.split(":")[1].lower().strip() for mapping in RMM_HALO_FIELD_MAP.split(",")]
expected_asset_field_names += [HALO_SYNC_INDICATOR_FIELD]

for asset_type in relevant_asset_types:
    existing_asset_field_names = [asset_field.lower() for asset_field in get_halo_asset_fields(asset_type)]
    missing_asset_field_names = [asset_field for asset_field in expected_asset_field_names
                                 if asset_field not in existing_asset_field_names]
    if missing_asset_field_names:
        log_string = f"Halo asset type '{asset_type['name']}' has not been assigned the following asset fields: " \
                     f"{', '.join(missing_asset_field_names)}. " \
                     f"You can add them in Configuration > Asset Management > Asset Types > " \
                     f"{asset_type['name']} > Field List. " \
                     f"If these fields are not assigned to {asset_type['name']} asset type, " \
                     f"they will not be synced from RMM for {asset_type['name']}s."
        print(log_string)
        del log_string

key_fields = dict()
for asset_type in relevant_asset_types:
    asset_type_key_fields = [field_value for field_name, field_value in asset_type.items()
                             if re.search(r"keyfield\d_name", field_name)]
    key_fields[asset_type["name"]] = asset_type_key_fields

def parse_ini_line(line: str, parse_parameter: bool = True) -> (str, (str, dict)):
    """
    Discard comments
    Assign variables
    Optional: parse key-value pairs

    :param line:
    :param parse_parameter_json:
    :return:
    """
    if re.search(r"^#", line.strip()) or not re.search(r"=", line.strip()):
        return
    else:
        ini_parameter = (line.split("=")[0].strip(), line.split("=")[1].strip())
    if parse_parameter:
        try:
            ini_parameter = (ini_parameter[0], json.loads(ini_parameter[1]))
        except ValueError:
            if re.search(r",", ini_parameter[1]):
                ini_parameter = (ini_parameter[0], ini_parameter[1].split(","))
    return ini_parameter


test = '{"model": "Model", "os": "OS Version", "serial_number": "Serial Number", "product_key": "product_key", "ram_gb": "RAM Memory", "cpu": "Processor", "disk": "Disc Size", "id": "rmm_id", "ip": "device_ip", "mac": "mac_address", "os_install_date": "os_install_date", "monitor": "monitor", "custom_fields": "rmm_custom_fields", "name": "device_name", "username": "username", "device_description": "description", "device_role": "device_role"}'

INI_PATH = ".ini"
ini_parameters_raw = list()
with open(INI_PATH) as ini_settings:
    for line in ini_settings.readlines():
        ini_parameters_raw += [parse_ini_line(line)]

ini_parameters = [parameter for parameter in ini_parameters_raw if parameter is not None]

for variable, value in ini_parameters:
    globals()[variable] = value

globals()["a"] = 1



# check key fields (recommended)

####################
# Post asset types #
####################

# headers = {
#     "Authorization": f"{access_token_type} {access_token}"}
#
# post_asset_type_payload = {
#     "name": "Mart test asset type 2",
#     "assetgroup_id": 112,
#     "defaultsequence": 0,
#     "show_to_users": True,
#     "fiid": -1,
#     "fields": [
#         {"field_id": 148,
#          "field_name": "Mart test field",
#          "_temp_id": get_temporary_id()}],
#     "weeklycost": 0,
#     "monthlycost": 0,
#     "quarterlycost": 0,
#     "sixmonthlycost": 0,
#     "yearlycost": 0,
#     "twoyearlycost": 0,
#     "threeyearlycost": 0,
#     "fouryearlycost": 0,
#     "fiveyearlycost": 0,
#     "linkedcontracttype": -1,
#     "enableresourcebooking": False,
#     "resourcebooking_workdays_id": 1,
#     "resourcebooking_allow_asset_selection": False,
#     "resourcebooking_asset_restriction_type": 0,
#     "resourcebooking_min_hours_advance": 1,
#     "resourcebooking_max_days_advance": 365,
#     "xtype_roles": [
#         {"_temp_id": get_temporary_id()}],
#     "allowall_status": True}
#
# ####### with scope read:assets gives 403 (forbidden)
# ####### with scope edit:assets fives 400 (bad request)
# post_asset_type_response = requests.post(
#     url=HALO_API_ASSET_TYPE_URL,
#     headers=headers,
#     json=post_asset_type_payload)


#####################
# Post asset fields #
#####################

# headers = {
#     "Authorization": f"{access_token_type} {access_token}"}
#
# post_asset_field_payload = {
#     "kind": "T",
#     "parenttype_id": "0",
#     "name": "Mart test field2",
#     "validate": "X",
#     "systemuse": "0",
#     "order_values_alphanumerically": True}
#
# ############ Returns 403 (forbidden)
# post_asset_type_response = requests.post(
#     url=HALO_API_ASSET_FIELD_URL,
#     headers=headers,
#     json=post_asset_field_payload)


####################
# Get asset groups #
####################

# asset_group_parameters = {}
# asset_group_response = requests.get(
#     url=HALO_API_ASSET_GROUP_URL,
#     headers=headers,
#     params=asset_group_parameters)

# for asset_group in asset_group_response.json():
#     print(asset_group["name"], asset_group["id"])


#####################
# Post asset groups #
#####################

# headers = {
#     "Authorization": f"{access_token_type} {access_token}"}
#
# post_asset_group_payload = [{
#     "name": "Mart test asset group 3",
#     "nominalcodepurchase": str(),
#     "showasequip": True,
#     "tax_id": "0",
#     "tax_id_purchase": "0"}]
#
# post_asset_group_response = requests.post(
#     url=HALO_API_ASSET_GROUP_URL,
#     headers=headers,
#     json=post_asset_group_payload)