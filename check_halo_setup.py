
# standard
from datetime import datetime
import logging
import os
import re
import requests
# local
from authenticate import get_halo_token
import general


#############
# Functions #
#############

def get_halo_asset_fields(api_response: dict) -> list:
    """
    Get field names from Halo asset api response. Useful if some of the fields are under "fields" parameter
    as field objects.

    :param api_response: Parsed json dict from Halo AssetType request
    :return: List with all field names.
    """
    all_keys = list()
    for key, value in api_response.items():
        all_keys += [key]
        if key == "fields":
            all_keys += [field["field_name"] for field in value]
    return all_keys


###############
# Set logging #
###############

os.environ["LOG_DIR_PATH"] = "."
os.environ["LOG_LEVEL"] = "INFO"

LOG_DIR_PATH = os.environ["LOG_DIR_PATH"]
LOG_LEVEL = os.environ["LOG_LEVEL"].upper()
if not os.path.exists(LOG_DIR_PATH):
    os.makedirs(LOG_DIR_PATH)

logging.basicConfig(
    # stream=sys.stdout,    # Show log info in stdout (for debugging).
    filename=f"{LOG_DIR_PATH}/{datetime.today().strftime('%Y_%m')}.log",
    format="{asctime}|{funcName}|{levelname}:{message}",
    style="{",
    level=logging.getLevelName(LOG_LEVEL))


###########################################
# Import environmental / config variables #
###########################################

env_file_path = ".env"
ini_file_path = ".ini"

general.parse_input_file(
    env_file_path,
    parse_values=False,
    set_environmental_variables=True)

ini_parameters = general.parse_input_file(ini_file_path)
HALO_API_URL = os.environ["HALO_API_URL"]


#################################
# Get read assets authorization #
#################################

read_assets_token = get_halo_token("read:assets")
headers = {"Authorization": f"{read_assets_token['token_type']} {read_assets_token['access_token']}"}


################## Add logging

#####################
# Check asset types #
#####################

api_asset_type_endpoint = "AssetType"
api_asset_type_url = HALO_API_URL.strip("/") + "/" + api_asset_type_endpoint

asset_type_parameters = {}
asset_type_response = requests.get(
    url=api_asset_type_url,
    headers=headers,
    params=asset_type_parameters)
asset_types = asset_type_response.json()

existing_asset_type_names = [asset_type["name"].lower().strip() for asset_type in asset_types]
expected_asset_type_names = [asset_type.lower() for asset_type in ini_parameters["HALO_REQUIRED_ASSET_TYPES"]]
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

api_field_endpoint = "field"
api_field_url = HALO_API_URL.strip("/") + "/" + api_field_endpoint

field_parameters = {}
field_response = requests.get(
    url=api_field_url,
    headers=headers,
    params=field_parameters)

existing_field_names = [field["name"].lower() for field in field_response.json()]
expected_field_names = [value.lower() for value in ini_parameters["RMM_HALO_FIELD_MAP"].values()]
missing_field_names = [field for field in ini_parameters["RMM_HALO_FIELD_MAP"].values()
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

expected_asset_field_names = [value.lower() for value in ini_parameters["RMM_HALO_FIELD_MAP"].values()]
expected_asset_field_names += [ini_parameters["HALO_SYNC_INDICATOR_FIELD"].lower()]

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
