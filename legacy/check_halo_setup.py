
# standard
from datetime import datetime
import itertools
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
    format="{asctime}|{funcName}|{levelname}: {message}",
    style="{",
    level=logging.getLevelName(LOG_LEVEL))

log_string = f"{'  HALO SETUP CHECK START  '  :-^100}"
print(log_string)
del log_string


###########################################
# Import environmental / config variables #
###########################################

env_file_path = "../.env"
ini_file_path = "../.ini"

general.parse_input_file(
    env_file_path,
    parse_values=False,
    set_environmental_variables=True)

ini_parameters = general.parse_input_file(ini_file_path)
HALO_API_URL = os.environ["HALO_API_URL"]


#################################
# Get read assets authorization #
#################################

read_assets_scope = "read:assets"
log_string = f"\nGetting authorization token for {read_assets_scope}..."
print(log_string)
del log_string

read_assets_token = get_halo_token(
    scope=read_assets_scope,
    url=os.environ["HALO_API_AUTHENTICATION_URL"],
    tenant=os.environ["HALO_API_TENANT"],
    client_id=os.environ["HALO_API_CLIENT_ID"],
    secret=os.environ["HALO_API_CLIENT_SECRET"])

headers = {"Authorization": f"{read_assets_token['token_type']} {read_assets_token['access_token']}"}


#####################
# Check asset types #
#####################

log_string = f"{'-'*100}\nChecking if required Asset types are defined..."
print(log_string)
del log_string

api_asset_type_url = HALO_API_URL.strip("/") + "/" + ini_parameters["HALO_ASSET_TYPE_ENDPOINT"]

asset_type_parameters = {}
asset_type_response = requests.get(
    url=api_asset_type_url,
    headers=headers,
    params=asset_type_parameters)
asset_types = asset_type_response.json()

existing_asset_types = [asset_type["name"].lower().strip() for asset_type in asset_types]
expected_asset_types = [asset_type.lower() for asset_type in ini_parameters["RMM_HALO_ASSET_MAP"].values()]
missing_asset_types = [asset_type for asset_type in expected_asset_types
                       if asset_type not in existing_asset_types]

if missing_asset_types:
    log_string = f"Halo is missing the following required Asset types:\n{', '.join(missing_asset_types)}!\n" \
                 f"You should add them in Configuration > Asset Management > Asset Type.\n" \
                 f"- If these asset types are not added, the script will not work.\n"
else:
    log_string = f"Checked if Halo environment has the following required Asset types: " \
                 f"{', '.join(expected_asset_types)}.\n" \
                 f"- All required Asset types are present. All good, carry on."

print(log_string)
del log_string


################
# Check fields #
################

log_string = f"{'-'*100}\nChecking if all recommended Fields are defined in Halo..."
print(log_string)
del log_string

api_field_url = HALO_API_URL.strip("/") + "/" + ini_parameters["HALO_FIELD_ENDPOINT"]

field_parameters = {}
field_response = requests.get(
    url=api_field_url,
    headers=headers,
    params=field_parameters)

halo_default_fields = [field.lower() for field in ini_parameters["HALO_DEFAULT_FIELDS"]]

mandatory_fields = list(ini_parameters["RMM_HALO_FIELD_MAP_MANDATORY"].values())
mandatory_fields.append(ini_parameters["HALO_SYNC_INDICATOR_FIELD"])
expected_mandatory_fields = [value.lower() for value in mandatory_fields
                             if value.lower() not in halo_default_fields]

recommended_fields = list(ini_parameters["RMM_HALO_FIELD_MAP_RECOMMENDED"].values())
expected_recommended_fields = [value.lower() for value in recommended_fields
                               if value.lower() not in halo_default_fields]

existing_fields = [field["name"].lower() for field in field_response.json()]

missing_mandatory_fields = [field for field in expected_mandatory_fields if field not in existing_fields]
missing_recommended_fields = [field for field in expected_recommended_fields if field not in existing_fields]

log_string = str()
if missing_mandatory_fields or missing_recommended_fields:
    if missing_mandatory_fields:
        log_string += f"Missing the following mandatory Fields:\n" \
                      f"{', '.join(missing_mandatory_fields)}.\n\n"

    if missing_recommended_fields:
        log_string += f"Missing the following recommended Fields:\n" \
                      f"{', '.join(missing_recommended_fields)}.\n\n"

    log_string += f"You can add the Fields in Configuration > Asset Management > Asset Fields.\n" \
                  f"If mandatory Fields are not added, the script will not run. " \
                  f"If recommended Fields are not added, these Fields will not be synced."

else:
    log_string += f"Checked if the following Fields are defined in Halo environment:\n" \
                  f"{', '.join(itertools.chain(expected_mandatory_fields, expected_recommended_fields))}.\n" \
                  f"- All required Fields are present. All good, carry on."

print(log_string)
del log_string


######################
# Check asset fields #
######################

log_string = f"{'-'*100}\nChecking if Halo Asset types have the required Fields..."
print(log_string)
del log_string

relevant_asset_type_names = [asset_type for asset_type in expected_asset_types
                             if asset_type not in missing_asset_types]
relevant_asset_types = [asset_type for asset_type in asset_types
                        if asset_type["name"].lower() in relevant_asset_type_names]

log_string = str()
missing_fields_found = False
for asset_type in relevant_asset_types:
    existing_asset_fields = [asset_field.lower() for asset_field in get_halo_asset_fields(asset_type)]
    missing_mandatory_asset_fields = [asset_field for asset_field in expected_mandatory_fields
                                      if not (asset_field in existing_asset_fields
                                              or asset_field in halo_default_fields)]
    missing_recommended_asset_fields = [asset_field for asset_field in expected_recommended_fields
                                        if not (asset_field in existing_asset_fields
                                                or asset_field in halo_default_fields)]

    if missing_mandatory_asset_fields:
        missing_fields_found = True
        log_string += f"Halo Asset type '{asset_type['name']}' has not been assigned " \
                      f"the following mandatory Fields:\n" \
                      f"{', '.join(missing_mandatory_asset_fields)}.\n\n"

    if missing_recommended_asset_fields:
        missing_fields_found = True
        log_string += f"Halo Asset type '{asset_type['name']}' has not been assigned " \
                      f"the following recommended Fields:\n" \
                      f"{', '.join(missing_recommended_asset_fields)}.\n\n"

if missing_fields_found:
    log_string += f"You can add the Fields to Assets in Configuration > Asset Management > Asset Types > " \
                  f"[asset type name] > Field List.\n" \
                  f"If mandatory Fields are not added, the script will not run. " \
                  f"If recommended Fields are not added, these Fields will not be synced."
else:
    log_string += f"- Verified that Asset types {', '.join(relevant_asset_type_names)} have the required Fields. " \
                  f"All good, carry on."

print(log_string)
del log_string


################################
# Check recommended key fields #
################################

log_string = f"{'-'*100}\nChecking if recommended Key Fields are configured for Halo Asset types..."
print(log_string)
del log_string

existing_key_fields = dict()
for asset_type in relevant_asset_types:
    asset_type_key_fields = [field_value for field_name, field_value in asset_type.items()
                             if re.search(r"keyfield\d_name", field_name)]
    existing_key_fields[asset_type["name"].lower()] = asset_type_key_fields

recommended_key_fields = ini_parameters["RECOMMENDED_KEY_FIELDS"]
missing_key_fields = dict()
for rmm_asset_type, key_fields in recommended_key_fields.items():
    halo_asset_type = ini_parameters["RMM_HALO_ASSET_MAP"][rmm_asset_type]
    key_fields = [key_fields] if not isinstance(key_fields, list) else key_fields
    missing = [field for field in key_fields if field not in existing_key_fields[halo_asset_type.lower()]]
    if missing:
        missing_key_fields[halo_asset_type] = missing

log_string = str()
if missing_key_fields:
    for asset_type, fields in missing_key_fields.items():
        log_string += f"Halo Asset type '{asset_type}' doesn't have the following Fields configured as Key Fields:\n" \
                      f"{' '.join(fields)}.\n\n"
    log_string += f"You can configure Key Fields for Assets in Configuration > Asset Management > Asset Types > " \
                  f"[asset type name] > Field List.\n" \
                  f"Even if Key Fields are not configured, the script will still work."
else:
    log_string = f"- Verified that Key Fields are correctly configured for Asset types " \
                 f"{', '.join(relevant_asset_type_names)}. All good, carry on."

print(log_string)
del log_string


#############################
# Check if Top Level exists #
#############################

nsight_toplevel = str(ini_parameters.get("NSIGHT_CLIENTS_TOPLEVEL", "")).strip()

if nsight_toplevel:
    log_string = f"{'-'*100}\nChecking if Halo has Top Level '{nsight_toplevel}' for N-sight clients..."
    print(log_string)
    del log_string

    read_customers_scope = "read:customers"
    log_string = f"\nGetting authorization token for {read_customers_scope}..."
    print(log_string)
    del log_string

    read_client_token = get_halo_token(
        scope=read_customers_scope,
        url=os.environ["HALO_API_AUTHENTICATION_URL"],
        tenant=os.environ["HALO_API_TENANT"],
        client_id=os.environ["HALO_API_CLIENT_ID"],
        secret=os.environ["HALO_API_CLIENT_SECRET"])

    api_toplevel_url = HALO_API_URL.strip("/") + "/" + ini_parameters["HALO_TOPLEVEL_ENDPOINT"]
    read_toplevel_headers = {"Authorization": f"{read_client_token['token_type']} {read_client_token['access_token']}"}
    read_toplevel_parameters = {
        "includeinactive": False}

    read_toplevel_response = requests.get(
        url=api_toplevel_url,
        headers=read_toplevel_headers,
        params=read_toplevel_parameters)
    toplevel_data = read_toplevel_response.json()

    toplevels = [{"id": toplevel["id"], "name": toplevel["name"]} for toplevel in toplevel_data["tree"]]

    nsight_toplevel_verified = str()
    for toplevel in toplevels:
        if nsight_toplevel in [toplevel["id"], toplevel["name"]]:
            nsight_toplevel_verified = toplevel
    if not nsight_toplevel_verified:
        existing_toplevels = [f'{toplevel["name"]} (id: {toplevel["id"]})' for toplevel in toplevels]
        log_string = f"The Top Level for N-sight clients '{nsight_toplevel}' does not exist in Halo Top Levels.\n" \
                     f"Existing Top Levels are {', '.join(existing_toplevels)}.\n" \
                     f"You can add the Top Level in Customers > New > +New Top Level.\n" \
                     f"In addition, make sure Top Levels are enabled in " \
                     f"Configuration > Users > General Settings >" \
                     f" Show an additional level ('Top Level') for grouping Customers.\n" \
                     f"If the Top Level is not added then N-sight clients will be assigned to random Top Level in Halo."
    else:
        log_string = f"- Verified that Top Level '{nsight_toplevel}' exists. All good, carry on."
    print(log_string)
    del log_string


log_string = f"{'  HALO SETUP CHECK END  '  :-^100}"
print(log_string)
del log_string
