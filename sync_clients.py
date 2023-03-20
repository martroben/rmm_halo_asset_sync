
# standard
from datetime import datetime
import json
import logging
import os
import sqlite3
# external
import xml.etree.ElementTree as xml_ET      # xml parser
# local
import client_classes
import general
import halo_requests
import log
import nsight_requests
from sql_operations import SqlTableBackup, SqlTableSessions


###########################################
# Import environmental / config variables #
###########################################

ini_file_path = ".ini"
ini_parameters = general.parse_input_file(ini_file_path)

env_file_path = ".env"
general.parse_input_file(
    env_file_path,
    parse_values=False,
    set_environmental_variables=True)
DRYRUN = bool(int(os.getenv("DRYRUN", 1)))

required_env_variables = [
    "HALO_API_URL",
    "HALO_API_AUTHENTICATION_URL",
    "HALO_API_TENANT",
    "HALO_API_CLIENT_ID",
    "HALO_API_CLIENT_SECRET",
    "NSIGHT_BASE_URL",
    "NSIGHT_API_KEY"]

missing_env_variables = [variable for variable in required_env_variables if os.getenv(variable, None) is None]


###############
# Set logging #
###############

session_id = general.generate_random_hex(8)
logger = log.setup_logger(
    name=ini_parameters["LOGGER_NAME"],
    level=ini_parameters["LOG_LEVEL"],
    indicator=ini_parameters["LOG_STRING_INDICATOR"],
    session_id=session_id,
    dryrun=DRYRUN)


#############
# Setup SQL #
#############   ################################ tests

# Use in-memory SQL for dry-run
sql_database_path = ":memory:" if DRYRUN else ini_parameters["SQL_DATABASE_PATH"]

sql_sessions_table = SqlTableSessions(sql_database_path)
sql_backup_table = SqlTableBackup(sql_database_path)

# Insert session info to SQL
sql_sessions_table.insert(
    session_id=session_id,
    time_unix=int(datetime.now().timestamp()),
    status="started")


##################
# Get Halo token #
##################

halo_authorizer = halo_requests.HaloAuthorizer(
    url=env_parameters["HALO_API_AUTHENTICATION_URL"],
    tenant=env_parameters["HALO_API_TENANT"],
    client_id=env_parameters["HALO_API_CLIENT_ID"],
    secret=env_parameters["HALO_API_CLIENT_SECRET"],
    log_name=log_name)

halo_client_token = halo_authorizer.get_token(
    scope="edit:customers",
    fatal=True)                                         # Abort if there is error in authorization


#######################
# Get N-sight clients #
#######################

nsight_clients_response = nsight_requests.get_clients(
    url=env_parameters["NSIGHT_BASE_URL"],
    api_key=env_parameters["NSIGHT_API_KEY"],
    log_name=log_name,
    fatal=False)                                        # Continue even if unable to retrieve N-sight clients

nsight_clients_raw = xml_ET.fromstring(nsight_clients_response.text).findall("./items/client")
nsight_clients = [client_classes.NsightClient(client) for client in nsight_clients_raw]


####################
# Get Halo clients #
####################

halo_client_session = halo_requests.HaloSession(halo_client_token)

halo_client_api = halo_requests.HaloInterface(
    url=env_parameters["HALO_API_URL"],
    endpoint=ini_parameters["HALO_CLIENT_ENDPOINT"],
    log_name=log_name,
    dryrun=bool(env_parameters["DRYRUN"]))

halo_client_parameters = {"includeinactive": False}
halo_clients_raw = halo_client_api.get_all(
    field="clients",
    session=halo_client_session,
    parameters=halo_client_parameters,
    fatal=True)             # Abort if a request fails, otherwise missing clients will be incorrectly determined

halo_clients = [client_classes.HaloClient(client) for client in halo_clients_raw]


#######################################
# Handle N-sight toplevel if supplied #
#######################################

# Set toplevel id for N-sight clients if toplevel name is provided in .ini, and it exists in Halo
nsight_toplevel = str(ini_parameters.get("NSIGHT_CLIENTS_TOPLEVEL", "")).strip()
if nsight_toplevel:
    # Get Halo toplevels to get the id of the input toplevel
    halo_toplevel_api = halo_requests.HaloInterface(
        url=env_parameters["HALO_API_URL"],
        endpoint=ini_parameters["HALO_TOPLEVEL_ENDPOINT"],
        log_name=log_name,
        dryrun=bool(env_parameters["DRYRUN"]))

    halo_toplevel_parameters = {"includeinactive": False}
    halo_toplevels_raw = halo_toplevel_api.get_all(
        field="tree",
        session=halo_client_session,
        parameters=halo_toplevel_parameters)

    halo_toplevels = [client_classes.HaloToplevel(toplevel) for toplevel in halo_toplevels_raw]

    nsight_toplevel_id = [toplevel.toplevel_id for toplevel in halo_toplevels if toplevel.name == nsight_toplevel][0]
    for client in nsight_clients:
        client.toplevel_id = nsight_toplevel_id

    # Add toplevel_id as a comparison variable for Client class
    # (only Clients with matching toplevel_id's are counted as equal)
    client_classes.Client.comparison_variables += ["toplevel_id"]


#################################
# Get clients missing from Halo #
#################################

missing_clients = [client for client in nsight_clients if client not in halo_clients]


########################
# Post missing clients #
########################

if not missing_clients:
    exit(0)

logger.info(f"Found N-sight Clients that are not synced to Halo. " 
            f"Count: {len(missing_clients)}")

for client in missing_clients:
    logger.info(f"Adding new Client to Halo: {str(client)}")
    backup_id = general.generate_random_hex(8)

    try:                                        # Only post if backup is successful
        logger.info(f"Backing up Client. Backup id: {backup_id}")
        backup_data = json.dumps([client.get_post_payload()])
        sql_backup_table.insert(
            session_id=session_id,
            backup_id=backup_id,
            action="insert",
            old="",
            new=backup_data,
            post_successful=0)
        logger.debug(f"End of Client backup action.")

        logger.debug(f"Posting Client to Halo.")
        response = halo_client_api.post(
            session=halo_client_session,
            json=[client.get_post_payload()],   # Halo post accepts dict wrapped in a list
            fatal=False)                        # Don't abort, even if one post request fails

        if response:
            logger.debug(f"Post successful.")
            sql_backup_table.update(
                column="post_successful",
                value=1,
                where=f'backup_id == "{backup_id}"')
        else:
            logger.warning(f"Posting Client failed. {client}, "
                           f"Action id: {backup_id}")

    except sqlite3.Error as sql_error:         # Catch cases where Client is not posted, because backup action failed
        logger.warning(f"SQL error while adding new Client to Halo. Client: {str(client)}. Error: {sql_error}")

    logger.debug(f"End of adding new Client to Halo action.")



########################### restore test
# json.dumps(sql_backup_table.read()[-1])
