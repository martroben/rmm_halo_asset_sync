
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


##########
# Assets #
##########

HALO_API_ASSET_URL = env_variables["HALO_API_ASSET_URL"]   # To get custom fields add e.g. + "/14/"

headers = {
    "Authorization": f"{access_token_type} {access_token}"}


# Seems that paginate, page_size and page_no have no effect.
asset_parameters = {
    "includeassetfields": "true"
    # "pageinate": 1,
    # "page_size": 4,
    # "page_no": 6
}

asset_response = requests.get(HALO_API_ASSET_URL, headers=headers, params=asset_parameters)
assets = asset_response.json()#["assets"]
for asset in assets["assets"]:
    if asset["id"] == 7:
        print(asset)

type(assets)

# for i, asset in enumerate(assets):
#     print(f"{i} | {asset['id']} | {asset['key_field']}")

# asset_response.json()["page_size"]
# len(asset_response.json()["assets"])
