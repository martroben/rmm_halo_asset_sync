# standard
from datetime import datetime
import http.client
import json
import logging
import os
import re
import sqlite3
import sys
# external
import xml.etree.ElementTree as xml_ET              # xml parser
# local
import client_classes
import general
import halo_requests
import log
import nsight_requests
from sql_operations import SqlTableBackup, SqlTableSessions


#############################################
# Environmental / global / config variables #
#############################################

# Config variables
ini_file_path = ".ini"
ini_parameters = general.parse_input_file(ini_file_path)

# Env variables
env_file_path = ".env"
general.parse_input_file(
    env_file_path,
    parse_values=False,
    set_environmental_variables=True)

required_env_variables = [
    "HALO_API_URL",
    "HALO_API_AUTHENTICATION_URL",
    "HALO_API_TENANT",
    "HALO_API_CLIENT_ID",
    "HALO_API_CLIENT_SECRET",
    "NSIGHT_BASE_URL",
    "NSIGHT_API_KEY"]

missing_env_variables = [variable for variable in required_env_variables if os.getenv(variable, None) is None]
if missing_env_variables:
    log_entry = log.EnvVariablesMissing(missing_env_variables)
    log_entry.record("ERROR")
    exit(1)

# Global variables
DRYRUN = bool(int(os.getenv("DRYRUN", 1)))
SESSION_ID = general.generate_random_hex(8)


###############
# Set logging #
###############

# Redaction filter
redact_patterns = [
    re.compile(os.getenv("NSIGHT_API_KEY"), flags=re.IGNORECASE),
    re.compile(os.getenv("HALO_API_TENANT"), flags=re.IGNORECASE),
    re.compile(os.getenv("HALO_API_CLIENT_ID"), flags=re.IGNORECASE),
    re.compile(os.getenv("HALO_API_CLIENT_SECRET"), flags=re.IGNORECASE)]

redaction_filter = log.Redactor(patterns=redact_patterns)

# Formatter
formatter = log.StandardFormatter(
    indicator=ini_parameters["LOG_STRING_INDICATOR"],
    session_id=SESSION_ID,
    dryrun=DRYRUN)

# Handler
handler = logging.StreamHandler(sys.stdout)  # Direct logs to stdout
handler.setFormatter(formatter)

# Level
log_level = ini_parameters["LOG_LEVEL"].upper()

# initialize standard logger
os.environ["LOGGER_NAME"] = ini_parameters["LOGGER_NAME"]
logger = logging.getLogger(os.getenv("LOGGER_NAME", "root"))


# Apply level to loggers
# Capture HTTPConnection debug (replace print function in http.client.print)
logging.getLogger().setLevel(log_level)

if log_level == "DEBUG":
    def capture_print_output(*args) -> None:
        log_string = log.LogString(
            short=" ".join(args),
            context="http.client.HTTPConnection debug")
        log_string.record("DEBUG")

    http.client.print = capture_print_output
    http.client.HTTPConnection.debuglevel = 1

# Remove all alternative handlers and set redaction filter to all loggers
# Make sure only root logger has a handler
# logging.root.manager.loggerDict has all initialized loggers except root logger
all_active_loggers = [logger for logger in logging.root.manager.loggerDict.values()
                      if not isinstance(logger, logging.PlaceHolder)]

for logger in all_active_loggers:
    logger.handlers.clear()
logging.getLogger().addHandler(handler)

# Set redaction filter to loggers
if bool(int(os.getenv("REDACT_LOGS", 1))):
    for logger in all_active_loggers:
        logger.addFilter(redaction_filter)
    logging.getLogger().addFilter(redaction_filter)


#############
# Setup SQL #
#############

# Use in-memory SQL for dry-run
sql_database_path = ":memory:" if DRYRUN else ini_parameters["SQL_DATABASE_PATH"]

sql_sessions_table = SqlTableSessions(sql_database_path)
sql_backup_table = SqlTableBackup(sql_database_path)

# Insert session info to SQL
sql_sessions_table.insert(
    session_id=SESSION_ID,
    time_unix=int(datetime.now().timestamp()),
    status="started")


##################
# Get Halo token #
##################

halo_authorizer = halo_requests.HaloAuthorizer(
    url=os.getenv("HALO_API_AUTHENTICATION_URL"),
    tenant=os.getenv("HALO_API_TENANT"),
    client_id=os.getenv("HALO_API_CLIENT_ID"),
    secret=os.getenv("HALO_API_CLIENT_SECRET"))

halo_client_token = halo_authorizer.get_token(scope="edit:customers")

# Add token to redacted patterns in logs
halo_client_token_pattern = re.compile(rf"{halo_client_token['access_token']}")
redaction_filter.add_pattern(halo_client_token_pattern)


#######################
# Get N-sight clients #
#######################

nsight_clients_response = nsight_requests.get_clients(          # Uses non-fatal retry
    url=os.getenv("NSIGHT_BASE_URL"),
    api_key=os.getenv("NSIGHT_API_KEY"))

nsight_clients_raw = xml_ET.fromstring(nsight_clients_response.text).findall("./items/client")
nsight_clients = [client_classes.NsightClient(client) for client in nsight_clients_raw]


####################
# Get Halo clients #
####################

halo_client_session = halo_requests.HaloSession(halo_client_token)

halo_client_api = halo_requests.HaloInterface(
    url=os.getenv("HALO_API_URL"),
    endpoint=ini_parameters["HALO_CLIENT_ENDPOINT"],
    dryrun=DRYRUN,
    fatal_fail=True)             # Abort if a request fails, otherwise missing clients will be incorrectly determined

halo_client_parameters = {"includeinactive": False}
halo_client_pages = halo_client_api.get(
    session=halo_client_session,
    parameters=halo_client_parameters)

# Parse client data field from response json
client_data_field = "clients"
halo_clients_raw = list()
for page in halo_client_pages:
    client_data = page.json()[client_data_field]
    halo_clients_raw += client_data if isinstance(client_data, list) else [client_data]

# Convert json to client object
halo_clients = [client_classes.HaloClient(client) for client in halo_clients_raw]


########################################
# Handle N-sight toplevel, if supplied #
########################################

# Set toplevel id for N-sight clients if toplevel name is provided in .ini, and it exists in Halo
nsight_clients_toplevel = str(ini_parameters.get("NSIGHT_CLIENTS_TOPLEVEL", "")).strip()
if nsight_clients_toplevel:        # Get Halo toplevels to get the id of the input toplevel
    halo_toplevel_api = halo_requests.HaloInterface(
        url=os.getenv("HALO_API_URL"),
        endpoint=ini_parameters["HALO_TOPLEVEL_ENDPOINT"],
        dryrun=DRYRUN,
        fatal_fail=True)

    halo_toplevel_parameters = {"includeinactive": False}
    halo_toplevel_pages = halo_toplevel_api.get(
        session=halo_client_session,
        parameters=halo_client_parameters)

    toplevel_data_field = "tree"
    existing_toplevels_raw = list()
    for page in halo_toplevel_pages:
        toplevel_data = page.json()[toplevel_data_field]
        existing_toplevels_raw += toplevel_data if isinstance(toplevel_data, list) else [toplevel_data]

    existing_toplevels = [client_classes.HaloToplevel(toplevel) for toplevel in existing_toplevels_raw]
    nsight_toplevel_match = [toplevel for toplevel in existing_toplevels
                             if toplevel.name == nsight_clients_toplevel]

    if nsight_toplevel_match:
        nsight_toplevel_id = nsight_toplevel_match[0].toplevel_id
        for client in nsight_clients:
            client.toplevel_id = nsight_toplevel_id
        # Add toplevel_id as a comparison variable for Client class
        # so that when comparing Clients, only Clients with matching toplevel_id's are counted as equal
        client_classes.Client.comparison_variables += ["toplevel_id"]
    else:
        log.NoMatchingToplevel(nsight_clients_toplevel).record("WARNING")


#################################
# Get clients missing from Halo #
#################################

missing_clients = [client for client in nsight_clients if client not in halo_clients]
if not missing_clients:
    log.NoMissingClients().record("INFO")
    exit(0)


########################
# Post missing clients #
########################

log.InsertNClients(n_clients=len(missing_clients)).record("INFO")
halo_client_api.set_retry_policy(fatal_fail=False)      # If posting one Client to Halo fails, still try to post others

for client in missing_clients:
    log.ClientInsert(client=client.name).record("INFO")
    backup_id = general.generate_random_hex(8)

    try:                                        # Only post if backup is successful
        log.ClientInsertBackup(client=client.name, backup_id=backup_id).record("DEBUG")
        client_post_data = [client.get_post_payload()]          # Halo post accepts dict wrapped in a list
        sql_backup_table.insert(
            session_id=SESSION_ID,
            backup_id=backup_id,
            action="insert",
            old="",
            new=json.dumps(client_post_data),
            post_successful=0)
        log.ClientInsertBackupSuccess().record("DEBUG")

        response = halo_client_api.post(
            session=halo_client_session,
            json=client_post_data)

        if response:
            sql_backup_table.update(
                column="post_successful",
                value=1,
                where=f'backup_id == "{backup_id}"')
        else:
            log.ClientInsertFail(client=client.name).record("WARNING")
            continue

    except sqlite3.Error as sql_error:         # Catch cases where Client is not posted, because backup action failed
        log.ClientInsertBackupFail(client=client.name, backup_id=backup_id, error=sql_error).record("WARNING")
        continue

    log.ClientInsertSuccess().record("INFO")



########################### restore test
# json.dumps(sql_backup_table.read()[-1])
