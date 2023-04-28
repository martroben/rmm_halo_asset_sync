

import json
import random
from datetime import datetime
import requests
import os

from dotenv import dotenv_values

from authenticate import get_halo_token


##################################
# Set up environmental variables #
##################################

env_file_path = "../.env"
# dotenv_values just reads from file without assigning variables to environment.
# use load_dotenv for the "real thing"
env_variables = dotenv_values(env_file_path)

HALO_API_AUTHENTICATION_URL = env_variables["HALO_API_AUTHENTICATION_URL"]
HALO_API_TENANT = env_variables["HALO_API_TENANT"]
HALO_API_CLIENT_ID = env_variables["HALO_API_CLIENT_ID"]
HALO_API_CLIENT_SECRET = env_variables["HALO_API_CLIENT_SECRET"]
HALO_BACKUP_DIR = env_variables["HALO_BACKUP_DIR"]


###############
# Pull assets #
###############

HALO_API_ASSET_URL = env_variables["HALO_API_ASSET_URL"]

# Authentication
read_assets_token = get_halo_token("read:assets")
headers = {"Authorization": f"{read_assets_token['token_type']} {read_assets_token['access_token']}"}

# Asset requests need to have assettype_id as parameter to include custom fields.
# Query assets endpoint to get all assettype_id-s:
asset_parameters = {}
asset_response = requests.get(
    url=HALO_API_ASSET_URL,
    headers=headers,
    params=asset_parameters)
assettype_ids = dict()
for asset in asset_response.json()["assets"]:
    if asset["assettype_id"] not in assettype_ids:
        assettype_ids[asset["assettype_id"]] = asset["assettype_name"]

# Query assets endpoint again to get asset info with custom fields
halo_assets = list()
for assettype_id in assettype_ids:
    assets_with_fields_parameters = {
        "includeassetfields": "true",
        "assettype_id": assettype_id}
    asset_with_fields_response = requests.get(
        url=HALO_API_ASSET_URL,
        headers=headers,
        params=assets_with_fields_parameters)
    halo_assets += asset_with_fields_response.json()["assets"]

for asset in halo_assets:
    print(f"{asset.get('key_field'):30} | {asset.get('inventory_number', ''):30} | {asset.get('third_party_id_string', ''):15}")

# Print all asset info
# for asset in halo_assets:
#     for key, value in asset.items():
#         if value not in ("", 0):
#             print(f"{key} || {value}")
#     print("-"*50)


####################
# Check Halo setup #
####################

# check client and site exist
# check asset types exist
# check fields exist
# check key fields set as recommended


###############
# Post assets #
###############

# Authentication
post_assets_token = get_halo_token("edit:assets")
headers = {"Authorization": f"{post_assets_token['token_type']} {post_assets_token['access_token']}"}

# if id is included, updates, otherwise tries to create new
post_asset_payload = [{
    # "id": int()
    "client_name": "Altacom OU",
    # Can be either site_id alone or client_name + site_name. If client on site name doesn't exist, saves as Unknown client & site
    "site_name": "Office Tallinn",
    "assettype_name": "Laptop",  # Can give either assetype id or name. Has to be existing name. Case insensitive
    # "key_field": "name",  # Key fields are set under Asset Type
    # "key_field2": "username or user",
    # "key_field3": "description + device_role",
    "_dontaddnewfields": False,
    # "isassetdetails": True,
    # "assettype_id": "131",
    # "site_id": 43,
    # "third_party_id_string": "RMM123",

    "inventory_number": "123464",  # Asset number
    # "business_owner_cab_id": "0",
    # technical_owner_cab_id": "0",
    # "sla_id": "-1",
    # "priority_id": "0",
    # "status_id": "3",
    # "services": [
    #     {"id": "107",
    #      "name": "Work From Home"}],
    "inactive": False,
    # "infofield_51": "test model 2",
    # "infofield_117": None,
    # "infofield_102": None,
    # "infofield_46": None,
    # "infofield_122": None,
    # "infofield_44": None,
    # "infofield_140": None,
    # "infofield_145": None,
    "notes": "test notes",
    "fields": [
        {
         # "id": "51",  # Can either use id or name of field. Ignores nonexistent fields, even if _dontaddnewfields is set to False
         "name": "Model",
         "value": "test model"},
        {"id": "117",
         "value": "test serial"},
        {"id": "102",
         "value": "test os version"},
        {"id": "46",
         "value": "test disc size"},
        {"id": "122",
         "value": "test processor"},
        {"id": "44",
         "value": "test ram"},
        {"id": "140",
         "value": "test connect"},
        {"id": "145",
         "value": "1233.93"}],
    "third_party_id": 112233,  # third_party_id works, but third_party_id_string doesn't
    # "third_party_id_string": "RMM_112233"
    }]

post_asset_response = requests.post(
    url=HALO_API_ASSET_URL,
    headers=headers,
    json=post_asset_payload)

print(post_asset_response.status_code)
