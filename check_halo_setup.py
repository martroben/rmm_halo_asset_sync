
# standard
import random
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
HALO_REQUIRED_ASSET_TYPES = env_variables["HALO_REQUIRED_ASSET_TYPES"].split(",")


####################
# Get asset groups #
####################

read_assets_token = get_halo_token("read:assets")
headers = {"Authorization": f"{read_assets_token['token_type']} {read_assets_token['access_token']}"}

asset_group_parameters = {}
asset_group_response = requests.get(
    url=HALO_API_ASSET_GROUP_URL,
    headers=headers,
    params=asset_group_parameters)

for asset_group in asset_group_response.json():
    print(asset_group["name"], asset_group["id"])


#####################
# Post asset groups #
#####################

headers = {
    "Authorization": f"{access_token_type} {access_token}"}

post_asset_group_payload = [{
    "name": "Mart test asset group 3",
    "nominalcodepurchase": str(),
    "showasequip": True,
    "tax_id": "0",
    "tax_id_purchase": "0"}]

post_asset_group_response = requests.post(
    url=HALO_API_ASSET_GROUP_URL,
    headers=headers,
    json=post_asset_group_payload)


####################
# Pull asset types #
####################

read_assets_token = get_halo_token("read:assets")
headers = {"Authorization": f"{read_assets_token['token_type']} {read_assets_token['access_token']}"}

asset_type_parameters = {}
asset_type_response = requests.get(
    url=HALO_API_ASSET_TYPE_URL,
    headers=headers,
    params=asset_type_parameters)

# Check if some asset types are missing
asset_types = asset_type_response.json()
existing_asset_type_names = [asset_type["name"].lower() for asset_type in asset_types]
missing_asset_types = [asset_type for asset_type in HALO_REQUIRED_ASSET_TYPES
                       if asset_type.lower() not in existing_asset_type_names]
if missing_asset_types:
    print(f"Halo is missing the following required asset types: {', '.join(missing_asset_types)}!")

# Check if asset types have all the necessary fields
required_asset_type_names = [asset_type.lower() for asset_type in HALO_REQUIRED_ASSET_TYPES]
required_asset_types = [asset_type for asset_type in asset_types
                        if asset_type["name"].lower() in required_asset_type_names]
################### Import env variable and create function parse_asset_fields
for asset_type in required_asset_types:
    missing_asset_fields = [asset_field for asset_field in HALO_RECOMMENDED_ASSET_FIELDS
                            if asset_field.lower() not in parse_asset_fields(asset_type)]
    if missing_asset_fields:
        print(f"Asset type {asset_type['name']} in Halo is missing the following recommended fields: {', '.join(missing_asset_fields)}!")


for asset_type in asset_types:
    print(asset_type["name"], asset_type["id"], asset_type["keyfield_name"], asset_type["keyfield2_name"], asset_type["keyfield3_name"])


####################
# Post asset types #
####################

headers = {
    "Authorization": f"{access_token_type} {access_token}"}

post_asset_type_payload = {
    "name": "Mart test asset type 2",
    "assetgroup_id": 112,
    "defaultsequence": 0,
    "show_to_users": True,
    "fiid": -1,
    "fields": [
        {"field_id": 148,
         "field_name": "Mart test field",
         "_temp_id": get_temporary_id()}],
    "weeklycost": 0,
    "monthlycost": 0,
    "quarterlycost": 0,
    "sixmonthlycost": 0,
    "yearlycost": 0,
    "twoyearlycost": 0,
    "threeyearlycost": 0,
    "fouryearlycost": 0,
    "fiveyearlycost": 0,
    "linkedcontracttype": -1,
    "enableresourcebooking": False,
    "resourcebooking_workdays_id": 1,
    "resourcebooking_allow_asset_selection": False,
    "resourcebooking_asset_restriction_type": 0,
    "resourcebooking_min_hours_advance": 1,
    "resourcebooking_max_days_advance": 365,
    "xtype_roles": [
        {"_temp_id": get_temporary_id()}],
    "allowall_status": True}

####### with scope read:assets gives 403 (forbidden)
####### with scope edit:assets fives 400 (bad request)
post_asset_type_response = requests.post(
    url=HALO_API_ASSET_TYPE_URL,
    headers=headers,
    json=post_asset_type_payload)


#####################
# Pull asset fields #
#####################

read_assets_token = get_halo_token("read:assets")
headers = {"Authorization": f"{read_assets_token['token_type']} {read_assets_token['access_token']}"}

field_parameters = {}
field_response = requests.get(
    url=HALO_API_ASSET_FIELD_URL,
    headers=headers,
    params=field_parameters)

asset_field_ids = dict()
for field in field_response.json():
    asset_field_ids[field["name"]] = field["id"]


#####################
# Post asset fields #
#####################

headers = {
    "Authorization": f"{access_token_type} {access_token}"}

post_asset_field_payload = {
    "kind": "T",
    "parenttype_id": "0",
    "name": "Mart test field2",
    "validate": "X",
    "systemuse": "0",
    "order_values_alphanumerically": True}

############ Returns 403 (forbidden)
post_asset_type_response = requests.post(
    url=HALO_API_ASSET_FIELD_URL,
    headers=headers,
    json=post_asset_field_payload)
