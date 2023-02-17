
# standard
from datetime import datetime
import json
import logging
import os
import requests
# local
from authenticate import get_halo_token
import general
import nsight_requests


#############
# Functions #
#############

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


def parse_clients(api_response: requests.Response) -> list:

    client_list = api_response.json()["clients"]
    clients = [{"id": client["id"], "name": client["name"]} for client in client_list]
    return clients


def get_toplevels(api_url: str, endpoint: str, token: dict) -> requests.Response:
    api_toplevel_url = api_url.strip("/") + "/" + endpoint
    read_toplevel_headers = {"Authorization": f"{token['token_type']} {token['access_token']}"}
    read_toplevel_parameters = {
        "includeinactive": False}

    toplevel_response = requests.get(
        url=api_toplevel_url,
        headers=read_toplevel_headers,
        params=read_toplevel_parameters)

    return toplevel_response


def get_halo_session(token):
    headers = {"Authorization": f"{token['token_type']} {token['access_token']}"}
    session = requests.Session()
    session.headers.update(headers)
    return session


@general.retry_function()
def post_client(session, url, payload, **kwargs):
    response = session.post(
        url=url,
        json=[payload])
    if not response.ok:
        error_str = f"Posting Client gave a bad response. URL: {url} | " \
                    f"Response {response.status_code}: {response.reason} | Payload: {payload}"
        raise ConnectionError(error_str)


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


###############
# Set logging #
###############

# os.environ["LOG_DIR_PATH"] = "."
# os.environ["LOG_LEVEL"] = "INFO"

# LOG_DIR_PATH = os.environ["LOG_DIR_PATH"]
# LOG_LEVEL = os.environ["LOG_LEVEL"].upper()
# if not os.path.exists(LOG_DIR_PATH):
#     os.makedirs(LOG_DIR_PATH)

# logging.basicConfig(
#     # stream=sys.stdout,    # Show log info in stdout (for debugging).
#     filename=f"{LOG_DIR_PATH}/{datetime.today().strftime('%Y_%m')}.log",
#     format="{asctime}|{funcName}|{levelname}: {message}",
#     style="{",
#     level=logging.getLevelName(LOG_LEVEL))

logger = logging.getLogger("halo_clients")
logger.setLevel(ini_parameters["LOG_LEVEL"])
handler = logging.StreamHandler()
formatter = logging.Formatter("{asctime} | {funcName} | {levelname}: {message}", style="{", datefmt="%m/%d/%Y %H:%M:%S")
handler.setFormatter(formatter)
logger.addHandler(handler)

logger.info("tere")

##################################
# Get read clients authorization #
##################################

read_client_token = get_halo_token(
    scope="read:customers",
    url=os.environ["HALO_API_AUTHENTICATION_URL"],
    tenant=os.environ["HALO_API_TENANT"],
    client_id=os.environ["HALO_API_CLIENT_ID"],
    secret=os.environ["HALO_API_CLIENT_SECRET"])


####################
# Get Top Level id #
####################

# Get N-sight toplevel id
nsight_toplevel = str(ini_parameters.get("HALO_NSIGHT_CLIENTS_TOPLEVEL", "")).strip()

if nsight_toplevel:
    toplevel_response = get_toplevels(HALO_API_URL, ini_parameters["HALO_TOPLEVEL_ENDPOINT"], read_client_token)
    toplevel_data = toplevel_response.json()["tree"]
    toplevels = [{"id": toplevel["id"], "name": toplevel["name"]} for toplevel in toplevel_data]
    nsight_toplevel_id = [toplevel["id"] for toplevel in toplevels if toplevel["name"] == nsight_toplevel][0]
else:
    nsight_toplevel_id = ""


###################
# Compare clients #
###################

NSIGHT_BASE_URL = os.environ["NSIGHT_BASE_URL"]
NSIGHT_API_KEY = os.environ["NSIGHT_API_KEY"]

nsight_clients_response = nsight_requests.get_clients(NSIGHT_BASE_URL, NSIGHT_API_KEY, log_name="halo_clients")
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
DRYRUN = 1
if halo_missing_clients and not DRYRUN:
    edit_client_token = get_halo_token(
        scope="edit:customers",
        url=os.environ["HALO_API_AUTHENTICATION_URL"],
        tenant=os.environ["HALO_API_TENANT"],
        client_id=os.environ["HALO_API_CLIENT_ID"],
        secret=os.environ["HALO_API_CLIENT_SECRET"])

    api_client_url = HALO_API_URL.strip("/") + "/" + ini_parameters["HALO_CLIENT_ENDPOINT"]
    edit_client_payload = {"toplevel_id": nsight_toplevel_id}
    with get_halo_session(edit_client_token) as edit_client_session:
        for client in halo_missing_clients:
            edit_client_payload["name"] = client["name"]
            post_client(
                session=edit_client_session,
                url=api_client_url,
                payload=edit_client_payload,
                log_name="halo_clients")

if DRYRUN:
    dryrun_string = f"Found {len(halo_missing_clients)} N-sight Clients that are not synced to Halo. " \
                    f"Inserting the following Clients to Halo: " \
                    f"{', '.join([client['name'] for client in halo_missing_clients])}."
    logger.debug(dryrun_string)

