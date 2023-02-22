
# standard
from datetime import datetime
import json
import logging
import os
import sqlite3
# external
import xml.etree.ElementTree as xml_ET      # xml parser
# local
from authenticate import get_halo_token
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
    def __init__(self, client_xml, toplevel_id=""):
        self.client_id = client_xml.find("./clientid").text
        self.name = client_xml.find("./name").text
        self.toplevel_id = toplevel_id

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
    "DRYRUN": os.environ["DRYRUN"]
}


###############
# Set logging #
###############

session_id = general.generate_random_hex(8)

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
sql_session = {
    "id": session_id,
    "time_unix": datetime.now().timestamp(),
    "status": "started"}
sql_sessions_table.insert(sql_session)


##################################
# Get read clients authorization #
##################################

read_client_token = get_halo_token(
    scope="read:customers",
    url=env_parameters["HALO_API_AUTHENTICATION_URL"],
    tenant=env_parameters["HALO_API_TENANT"],
    client_id=env_parameters["HALO_API_CLIENT_ID"],
    secret=env_parameters["HALO_API_CLIENT_SECRET"])


###############
# Get clients #
###############

# Get N-Sight clients
nsight_clients_response = nsight_requests.get_clients(
    url=env_parameters["NSIGHT_BASE_URL"],
    api_key=env_parameters["NSIGHT_API_KEY"],
    log_name="halo_clients")

nsight_clients_raw = xml_ET.fromstring(nsight_clients_response.text).findall("./items/client")
nsight_clients = [NsightClient(client) for client in nsight_clients_raw]

# Set toplevel id for N-sight clients if toplevel name is provided in .ini, and it exists in Halo
nsight_toplevel = str(ini_parameters.get("HALO_NSIGHT_CLIENTS_TOPLEVEL", "")).strip()
if nsight_toplevel:
    Client.comparison_variables += ["toplevel_id"]
    toplevel_response = halo_requests.get_clients(
        url=env_parameters["HALO_API_URL"],
        endpoint=ini_parameters["HALO_TOPLEVEL_ENDPOINT"],
        token=read_client_token)

    toplevels_raw = toplevel_response.json()["tree"]
    toplevels = [HaloToplevel(toplevel) for toplevel in toplevels_raw]

    nsight_toplevel_ids = [toplevel.toplevel_id for toplevel in toplevels if toplevel.name == nsight_toplevel]
    if nsight_toplevel_ids:
        for client in nsight_clients:
            client.toplevel_id = nsight_toplevel_ids[0]

# Get Halo clients
halo_clients_response = halo_requests.get_clients(
    url=env_parameters["HALO_API_URL"],
    endpoint=ini_parameters["HALO_CLIENT_ENDPOINT"],
    token=read_client_token)

halo_clients_raw = halo_clients_response.json()["clients"]
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


# Get edit:customers token
edit_client_token = get_halo_token(
    scope="edit:customers",
    url=os.environ["HALO_API_AUTHENTICATION_URL"],
    tenant=os.environ["HALO_API_TENANT"],
    client_id=os.environ["HALO_API_CLIENT_ID"],
    secret=os.environ["HALO_API_CLIENT_SECRET"])

logger.debug(f"{bool(env_parameters['DRYRUN'])*'(DRYRUN) '}Found N-sight Clients that are not synced to Halo: " 
             "{len(missing_clients)}")

with halo_requests.create_session(edit_client_token) as edit_client_session:
    for client in missing_clients:
        if env_parameters["DRYRUN"]:
            logger.debug(f"(DRYRUN) Client posted to Halo: {str(client)}")
        else:
            halo_requests.post_client(
                session=edit_client_session,
                url=env_parameters["HALO_API_URL"],
                endpoint=ini_parameters["HALO_CLIENT_ENDPOINT"],
                payload=client.json_payload(),
                log_name="halo_clients")

        if env_parameters["DRYRUN"]:
            # backup_client(client, "new", DRYRUN)
            logger.debug(f"(DRYRUN) Client backed up: {str(client)}")
        else:
            pass
            # backup_client(client, "new")


#################### Create backup_client + rest of the backup logic







