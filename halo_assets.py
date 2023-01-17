
import json
import requests
from dotenv import dotenv_values


##################
# Authentication #
##################

env_file_path = ".env"
# dotenv_values just reads from file without assigning variables to environment.
# use load_dotenv for the "real thing"
env_variables = dotenv_values(env_file_path)

HALO_API_AUTHENTICATION_URL = env_variables["HALO_API_AUTHENTICATION_URL"]
HALO_API_TENANT = env_variables["HALO_API_TENANT"]
HALO_API_CLIENT_ID = env_variables["HALO_API_CLIENT_ID"]
HALO_API_CLIENT_SECRET = env_variables["HALO_API_CLIENT_SECRET"]

# Can't set several scopes?
api_application_scope = "read:assets"

parameters = {
    "tenant": HALO_API_TENANT}

authentication_body = {
    "grant_type": "client_credentials",
    "client_id": HALO_API_CLIENT_ID,
    "client_secret": HALO_API_CLIENT_SECRET,
    "scope": api_application_scope}

authentication_response = requests.post(
    url=HALO_API_AUTHENTICATION_URL,
    data=authentication_body,
    params=parameters)

authentication_response_json = authentication_response.json()
access_token_type = authentication_response_json["token_type"]
access_token = authentication_response_json["access_token"]


###############
# Pull assets #
###############

HALO_API_ASSET_URL = env_variables["HALO_API_ASSET_URL"]

headers = {
    "Authorization": f"{access_token_type} {access_token}"}

# Asset requests need to have assettype_id as parameter to get custom fields.
# Query assets endpoint to get all assettype_id-s:
asset_parameters = {}
asset_response = requests.get(HALO_API_ASSET_URL, headers=headers, params=asset_parameters)
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
    asset_with_fields_response = requests.get(HALO_API_ASSET_URL, headers=headers, params=assets_with_fields_parameters)
    halo_assets += asset_with_fields_response.json()["assets"]

for asset in halo_assets:
    for key, value in asset.items():
        if value not in ("", 0):
            print(f"{key} || {value}")
    print("-"*50)
