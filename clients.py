
# standard
from datetime import datetime
import json
import logging
import os
import sqlite3
# external
import xml.etree.ElementTree as xml_ET      # xml parser
# local
import general
import halo_requests
import nsight_requests
import sql_operations


##################
# Client classes #
##################

class Client:
    comparison_variables = ["name"]

    def __eq__(self, other):
        matching_fields = [str(self.__getattribute__(variable)).lower() == str(other.__getattribute__(variable)).lower()
                           for variable in self.comparison_variables]
        return all(matching_fields)

    def __repr__(self):
        name = self.__getattribute__("name")
        instance_id = self.__getattribute__("toplevel_id") if self.__class__ is HaloToplevel \
            else self.__getattribute__("client_id")
        return f"{name} (id: {instance_id})"


class NsightClient(Client):
    toplevel_id = ""

    def __init__(self, client_xml):
        self.client_id = client_xml.find("./clientid").text
        self.name = client_xml.find("./name").text

    def json_payload(self):
        payload = {
            "name": self.name,
            "toplevel_id": self.toplevel_id}
        return json.dumps(payload)


class HaloClient(Client):
    def __init__(self, client):
        self.client_id = client["id"]
        self.name = client["name"]
        self.toplevel_id = client["toplevel_id"]


class HaloToplevel(Client):
    def __init__(self, toplevel):
        self.toplevel_id = toplevel["id"]
        self.name = toplevel["name"]


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
env_parameters = {
    "HALO_API_URL": os.environ["HALO_API_URL"],
    "HALO_API_AUTHENTICATION_URL": os.environ["HALO_API_AUTHENTICATION_URL"],
    "HALO_API_TENANT": os.environ["HALO_API_TENANT"],
    "HALO_API_CLIENT_ID": os.environ["HALO_API_CLIENT_ID"],
    "HALO_API_CLIENT_SECRET": os.environ["HALO_API_CLIENT_SECRET"],
    "NSIGHT_BASE_URL": os.environ["NSIGHT_BASE_URL"],
    "NSIGHT_API_KEY": os.environ["NSIGHT_API_KEY"],
    "DRYRUN": int(os.environ["DRYRUN"])
}


###############
# Set logging #
###############

session_id = general.generate_random_hex(8)

log_name = "clients"
logger = logging.getLogger(log_name)
logger.setLevel(ini_parameters["LOG_LEVEL"])
handler = logging.StreamHandler()
formatter = logging.Formatter(
    fmt="{asctime} | {funcName} | {levelname}: {message}",
    datefmt="%m/%d/%Y %H:%M:%S",
    style="{")
handler.setFormatter(formatter)
logger.addHandler(handler)


#############
# Setup SQL #
#############

# Create directories for database if they don't exist
if ini_parameters["SQL_DATABASE_PATH"] != ":memory:":
    if not os.path.exists(os.path.dirname(ini_parameters["SQL_DATABASE_PATH"])):
        os.makedirs(os.path.dirname(ini_parameters["SQL_DATABASE_PATH"]))

# Use in-memory SQL for dry-run
sql_database_path = ":memory:" if env_parameters["DRYRUN"] else ini_parameters["SQL_DATABASE_PATH"]

sql_connection = sqlite3.connect(sql_database_path)
sql_sessions_table = sql_operations.SqlTableSessions(sql_connection)
sql_backup_table = sql_operations.SqlTableBackup(sql_connection, active=ini_parameters["BACKUP_ACTIVE"])

# Insert session info to SQL
sql_sessions_table.insert(
    id=session_id,
    time_unix=int(datetime.now().timestamp()),
    status="started")


##################
# Get Halo token #
##################

halo_authorizer = halo_requests.HaloAuthorizer(
    url=env_parameters["HALO_API_AUTHENTICATION_URL"],
    tenant=env_parameters["HALO_API_TENANT"],
    client_id=env_parameters["HALO_API_CLIENT_ID"],
    secret=env_parameters["HALO_API_CLIENT_SECRET"])

halo_client_token = halo_authorizer.get_token(
    scope="edit:customers",
    log_name=log_name,
    fatal=True)             # Abort if there is error in authorization


###############
# Get clients #
###############

# Get N-Sight clients
nsight_clients_response = nsight_requests.get_clients(
    url=env_parameters["NSIGHT_BASE_URL"],
    api_key=env_parameters["NSIGHT_API_KEY"],
    log_name=log_name,
    fatal=False)            # Continue even if unable to retrieve N-sight clients

nsight_clients_raw = xml_ET.fromstring(nsight_clients_response.text).findall("./items/client")
nsight_clients = [NsightClient(client) for client in nsight_clients_raw]

halo_client_session = halo_requests.HaloSession(halo_client_token)

# Set toplevel id for N-sight clients if toplevel name is provided in .ini, and it exists in Halo
nsight_toplevel = str(ini_parameters.get("NSIGHT_CLIENTS_TOPLEVEL", "")).strip()
if nsight_toplevel:
    # Get Halo toplevels
    halo_toplevel_api = halo_requests.HaloInterface(
        url=env_parameters["HALO_API_URL"],
        endpoint=ini_parameters["HALO_TOPLEVEL_ENDPOINT"],
        dryrun=env_parameters["DRYRUN"])

    halo_toplevel_parameters = {"includeinactive": False}
    halo_toplevels_raw = halo_toplevel_api.get_all(
        field="tree",
        session=halo_client_session,
        parameters=halo_toplevel_parameters,
        log_name="clients")

    halo_toplevels = [HaloToplevel(toplevel) for toplevel in halo_toplevels_raw]

    # Set new default toplevel id for all N-sight clients
    nsight_toplevel_id = [toplevel.toplevel_id for toplevel in halo_toplevels if toplevel.name == nsight_toplevel][0]
    NsightClient.toplevel_id = nsight_toplevel_id

    # Add toplevel_id as a comparison variable (only clients with matching toplevel_id's are counted as equal)
    Client.comparison_variables += ["toplevel_id"]


# Get Halo clients
halo_client_api = halo_requests.HaloInterface(
    url=env_parameters["HALO_API_URL"],
    endpoint=ini_parameters["HALO_CLIENT_ENDPOINT"],
    dryrun=env_parameters["DRYRUN"])

halo_client_parameters = {"includeinactive": False}
halo_clients_raw = halo_client_api.get_all(
    field="clients",
    session=halo_client_session,
    parameters=halo_client_parameters,
    log_name="clients",
    fatal=True)             # Abort if a request fails, otherwise missing clients will be incorrectly determined

halo_clients = [HaloClient(client) for client in halo_clients_raw]


#################################
# Get clients missing from Halo #
#################################

missing_clients = [client for client in nsight_clients if client not in halo_clients]


########################
# Post missing clients #
########################

if not missing_clients:
    exit(0)

logger.info(f"{bool(env_parameters['DRYRUN'])*'(DRYRUN) '}Found N-sight Clients that are not synced to Halo: " 
            f"{len(missing_clients)}")


with halo_requests.HaloSession(halo_client_token) as halo_session:
    for client in missing_clients:
        payload = client.json_payload()
        logger.info(f"{bool(env_parameters['DRYRUN'])*'(DRYRUN) '}Posting new Client to Halo: {str(client)}")

        sql_backup_table.insert(
            session_id=session_id,
            action="insert",
            old="",
            new=payload) #################### Only post if backup successful

        halo_client_api.post(
            session=halo_client_session,
            payload=payload,
            log_name="clients",
            fatal=False)            # Don't abort, even if one post request fails

        # halo_requests.post_client(
        #     session=halo_session,
        #     url=env_parameters["HALO_API_URL"],
        #     endpoint=ini_parameters["HALO_CLIENT_ENDPOINT"],
        #     payload=payload,
        #     log_name=log_name,
        #     dryrun=env_parameters["DRYRUN"])

        logger.info(f"{bool(env_parameters['DRYRUN'])*'(DRYRUN) '}Client backed up: {str(client)}")




########################### restore test
# json.dumps(sql_backup_table.read()[-1])
