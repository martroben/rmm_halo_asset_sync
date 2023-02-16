
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


#################################
# Get read assets authorization #
#################################

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

api_client_url = HALO_API_URL.strip("/") + "/" + ini_parameters["HALO_CLIENT_ENDPOINT"]
read_client_headers = {"Authorization": f"{read_client_token['token_type']} {read_client_token['access_token']}"}
read_client_parameters = {
    "includeinactive": False}

read_client_response = requests.get(
    url=api_client_url,
    headers=read_client_headers,
    params=read_client_parameters)
client_data = read_client_response.json()

clients = [{"id": client["id"], "name": client["name"]} for client in client_data["clients"]]


################
# Post clients #
################

edit_client_token = get_halo_token(
    scope="edit:customers",
    url=os.environ["HALO_API_AUTHENTICATION_URL"],
    tenant=os.environ["HALO_API_TENANT"],
    client_id=os.environ["HALO_API_CLIENT_ID"],
    secret=os.environ["HALO_API_CLIENT_SECRET"])

edit_client_headers = {"Authorization": f"{edit_client_token['token_type']} {edit_client_token['access_token']}"}
edit_client_parameters = {}
edit_client_payload = [{
    "name": "API_customer"
    #"toplevel_id": 12
}]

edit_client_response = requests.post(
    url=api_client_url,
    json=edit_client_payload,
    headers=edit_client_headers,
    params=edit_client_parameters)

# Bad request - add stuff to body

