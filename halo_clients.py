
# standard
from datetime import datetime
import json
import logging
import os
import requests
# local
from authenticate import get_halo_token
import general


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


##################################
# Get read clients authorization #
##################################

log_string = f"\nGetting authorization token..."
print(log_string)
del log_string

read_client_token = get_halo_token(
    scope="read:customers",
    url=os.environ["HALO_API_AUTHENTICATION_URL"],
    tenant=os.environ["HALO_API_TENANT"],
    client_id=os.environ["HALO_API_CLIENT_ID"],
    secret=os.environ["HALO_API_CLIENT_SECRET"])


###############
# Get clients #
###############

def get_clients(api_url: str, endpoint: str, token: dict) -> requests.Response:

    api_client_url = api_url.strip("/") + "/" + endpoint
    read_client_headers = {"Authorization": f"{token['token_type']} {token['access_token']}"}
    read_client_parameters = {
        "includeinactive": False}

    read_client_response = requests.get(
        url=api_client_url,
        headers=read_client_headers,
        params=read_client_parameters)

    return read_client_response


def parse_clients(api_response: requests.Response) -> dict:

    client_list = api_response.json()["clients"]
    clients = [{"id": client["id"], "name": client["name"]} for client in client_list]
    return clients


####################
# Get Top Level id #
####################

def get_toplevels(api_url: str, endpoint: str, token: dict) -> list[dict]:
    api_toplevel_url = api_url.strip("/") + "/" + endpoint
    read_toplevel_headers = {"Authorization": f"{token['token_type']} {token['access_token']}"}
    read_toplevel_parameters = {
        "includeinactive": False}

    read_toplevel_response = requests.get(
        url=api_toplevel_url,
        headers=read_toplevel_headers,
        params=read_toplevel_parameters)
    toplevel_data = read_toplevel_response.json()["tree"]

    return toplevel_data


# Get N-sight toplevel id
nsight_toplevel = str(ini_parameters.get("HALO_NSIGHT_CLIENTS_TOPLEVEL", "")).strip()

if nsight_toplevel:
    toplevel_data = get_toplevels(HALO_API_URL, ini_parameters["HALO_TOPLEVEL_ENDPOINT"], read_client_token)
    toplevels = [{"id": toplevel["id"], "name": toplevel["name"]} for toplevel in toplevel_data]
    nsight_toplevel_id = [toplevel["id"] for toplevel in toplevels if toplevel["name"] == nsight_toplevel][0]
else:
    nsight_toplevel_id = ""


###################
# Compare clients #
###################

import nsight_requests

NSIGHT_BASE_URL = os.environ["NSIGHT_BASE_URL"]
NSIGHT_API_KEY = os.environ["NSIGHT_API_KEY"]

nsight_clients_response = nsight_requests.get_clients(NSIGHT_BASE_URL, NSIGHT_API_KEY)
nsight_clients = nsight_requests.parse_clients(nsight_clients_response)

halo_clients_response = get_clients(HALO_API_URL, ini_parameters["HALO_CLIENT_ENDPOINT"], read_client_token)
halo_clients = parse_clients(halo_clients_response)

nsight_client_names = [client["name"].lower() for client in nsight_clients]
halo_client_names = [client["name"].lower() for client in halo_clients]

halo_missing_client_names = [client for client in nsight_client_names if client not in halo_client_names]
halo_missing_clients = [nsight_client for nsight_client in nsight_clients
                        if nsight_client["name"].lower() in halo_missing_client_names]


################
# Post clients #
################

api_client_url = HALO_API_URL.strip("/") + "/" + ini_parameters["HALO_CLIENT_ENDPOINT"]

edit_client_token = get_halo_token(
    scope="edit:customers",
    url=os.environ["HALO_API_AUTHENTICATION_URL"],
    tenant=os.environ["HALO_API_TENANT"],
    client_id=os.environ["HALO_API_CLIENT_ID"],
    secret=os.environ["HALO_API_CLIENT_SECRET"])

edit_client_headers = {"Authorization": f"{edit_client_token['token_type']} {edit_client_token['access_token']}"}
edit_client_parameters = {}
edit_client_payload = [{
    "name": "API_customer",
    "toplevel_id": nsight_toplevel_id}]

edit_client_response = requests.post(
    url=api_client_url,
    json=edit_client_payload,
    headers=edit_client_headers,
    params=edit_client_parameters)

