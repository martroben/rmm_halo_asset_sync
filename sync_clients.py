# standard
from datetime import datetime
import json
import logging
import os
import re
import sqlite3
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
    log.EnvVariablesMissing(missing_env_variables).record("ERROR")
    exit(1)

# Global variables
DRYRUN = bool(int(os.getenv("DRYRUN", 1)))
SESSION_ID = general.generate_random_hex(8)
REDACT_FILTER: log.Redactor()


#################
# Setup logging #
#################

# logging.root.manager.loggerDict has all initialized loggers except root logger
all_active_loggers = [logger for logger in logging.root.manager.loggerDict.values()
                      if not isinstance(logger, logging.PlaceHolder)]

# Send all logs to stdout
log.set_logs_to_stdout(all_active_loggers)

# Set formatter to all logs
formatter = log.StandardFormatter(
    indicator=ini_parameters["LOG_STRING_INDICATOR"],
    session_id=SESSION_ID,
    dryrun=DRYRUN)
log.set_formatter(formatter, all_active_loggers)

# Set level
log.set_level(logging.getLevelName(ini_parameters["LOG_LEVEL"].upper()), all_active_loggers)

# Set redact filter
redact_patterns = [
    re.compile(os.getenv("NSIGHT_API_KEY"), flags=re.IGNORECASE),
    re.compile(os.getenv("HALO_API_TENANT"), flags=re.IGNORECASE),
    re.compile(os.getenv("HALO_API_CLIENT_ID"), flags=re.IGNORECASE),
    re.compile(os.getenv("HALO_API_CLIENT_SECRET"), flags=re.IGNORECASE)]
REDACT_FILTER = log.Redactor(patterns=redact_patterns)      # Used globally to add additional redacted patterns
if bool(int(os.getenv("REDACT_LOGS", 1))):
    log.set_filter(REDACT_FILTER, all_active_loggers)


#############
# Setup SQL #
#############

# Use in-memory SQL for dry-run
sql_database_path = ":memory:" if DRYRUN else ini_parameters["SQL_DATABASE_PATH"]

log.SqlSetupBegin(sql_database_path).record("INFO")
sql_sessions_table = SqlTableSessions(sql_database_path)
sql_backup_table = SqlTableBackup(sql_database_path)

# Insert session info to SQL
log.SqlInsertSessionInfo(SESSION_ID).record("INFO")
sql_sessions_table.insert(
    session_id=SESSION_ID,
    time_unix=int(datetime.now().timestamp()),
    status="started")


##################
# Get Halo token #
##################

# Inputs
halo_api_authentication_url = os.getenv("HALO_API_AUTHENTICATION_URL")
halo_api_tenant = os.getenv("HALO_API_TENANT")
halo_api_client_id = os.getenv("HALO_API_CLIENT_ID")
halo_api_client_secret = os.getenv("HALO_API_CLIENT_SECRET")
halo_api_token_scope = "edit:customers"

log.HaloTokenRequestBegin().record("INFO")
halo_authorizer = halo_requests.HaloAuthorizer(         # Uses fatal fail in retry
    url=halo_api_authentication_url,
    tenant=halo_api_tenant,
    client_id=halo_api_client_id,
    secret=halo_api_client_secret)

halo_client_token = dict()
try:
    halo_client_token = halo_authorizer.get_token(scope=halo_api_token_scope)
except ConnectionError as connection_error:
    log.HaloTokenRequestFail(connection_error).record("ERROR")
    exit(1)

if not halo_client_token:
    log.HaloTokenRequestFail()
    exit(1)

# Add Halo token to log redact patterns
halo_client_token_pattern = re.compile(rf"{halo_client_token['access_token']}")
REDACT_FILTER.add_pattern(halo_client_token_pattern)


#######################
# Get N-sight clients #
#######################

# Inputs
nsight_base_url = os.getenv("NSIGHT_BASE_URL")
nsight_api_key = os.getenv("NSIGHT_API_KEY")

log.NsightClientsRequestBegin().record("INFO")
nsight_clients_response = nsight_requests.get_clients(          # Uses non-fatal retry
    url=nsight_base_url,
    api_key=nsight_api_key)

if nsight_clients_response:
    nsight_client_data = nsight_requests.parse_clients(nsight_clients_response)
    nsight_clients = [client_classes.NsightClient(client_data) for client_data in nsight_client_data]
else:
    log.NsightClientsRequestFail(nsight_clients_response.status_code, nsight_clients_response.reason).record("WARNING")
    nsight_clients = list()


####################
# Get Halo clients #
####################

# Inputs
halo_api_url = os.getenv("HALO_API_URL")
halo_api_client_endpoint = ini_parameters["HALO_CLIENT_ENDPOINT"]
halo_api_client_parameters = {"includeinactive": False}
# halo_client_token (from HaloAuthorizer)

halo_client_session = halo_requests.HaloSession(halo_client_token)
halo_client_api = halo_requests.HaloInterface(
    url=halo_api_url,
    endpoint=halo_api_client_endpoint,
    fatal_fail=True)             # Abort if a request fails, otherwise missing clients will be incorrectly determined

log.HaloClientRequestBegin().record("INFO")
halo_client_pages = list()
try:
    halo_client_pages = halo_client_api.get(
        session=halo_client_session,
        parameters=halo_api_client_parameters)
except ConnectionError as connection_error:
    log.HaloClientRequestFail(connection_error).record("ERROR")
    exit(1)
if not halo_client_pages:
    log.HaloClientRequestFail().record("ERROR")
    exit(1)

halo_client_data = halo_requests.parse_clients(halo_client_pages)
halo_clients = [client_classes.HaloClient(client_data) for client_data in halo_client_data]


########################################
# Handle N-sight toplevel, if supplied #
########################################

# Set toplevel id for N-sight clients if toplevel name is provided in .ini, and it exists in Halo

# Get toplevel id for the N-sight clients toplevel name
nsight_toplevel = str(ini_parameters.get("NSIGHT_CLIENTS_TOPLEVEL", "")).strip()
if nsight_toplevel:
    halo_toplevel_api = halo_requests.HaloInterface(
        url=os.getenv("HALO_API_URL"),
        endpoint=ini_parameters["HALO_TOPLEVEL_ENDPOINT"],
        fatal_fail=True)

    halo_api_toplevel_parameters = {"includeinactive": False}
    log.HaloToplevelRequestBegin().record("INFO")
    halo_toplevel_pages = list()
    try:
        halo_toplevel_pages = halo_toplevel_api.get(
            session=halo_client_session,
            parameters=halo_api_toplevel_parameters)
    except ConnectionError as connection_error:
        log.HaloToplevelRequestFail(connection_error).record("ERROR")
        exit(1)
    if not halo_toplevel_pages:
        log.HaloToplevelRequestFail().record("ERROR")
        exit(1)

    existing_toplevels = halo_requests.parse_toplevels(halo_toplevel_pages)
    nsight_toplevel_match = [toplevel for toplevel in existing_toplevels
                             if toplevel.name == nsight_toplevel]

    if nsight_toplevel_match:
        nsight_toplevel_id = nsight_toplevel_match[0].toplevel_id
        for client in nsight_clients:
            client.toplevel_id = nsight_toplevel_id
    else:
        log.NoMatchingToplevel(nsight_toplevel).record("ERROR")
        exit(1)

    # Add toplevel_id as a comparison variable for Client class
    # so that when comparing Clients, only Clients with matching toplevel_id's are counted as equal
    client_classes.Client.comparison_variables += ["toplevel_id"]


#################################
# Get clients missing from Halo #
#################################

missing_clients = [client for client in nsight_clients if client not in halo_clients]
if not missing_clients:
    log.NoMissingClients().record("INFO")
    exit(0)

log.InsertNClients(n_clients=len(missing_clients)).record("INFO")


########################
# Post missing clients #
########################

# Inputs
halo_api_url = os.getenv("HALO_API_URL")
halo_api_client_endpoint = ini_parameters["HALO_CLIENT_ENDPOINT"]
# DRYRUN (boolean)
# halo_client_token (from HaloAuthorizer)

halo_client_session = halo_requests.HaloSession(halo_client_token)
halo_client_api = halo_requests.HaloInterface(
    url=halo_api_url,
    endpoint=halo_api_client_endpoint,
    dryrun=DRYRUN,
    fatal_fail=False)             # Continue if posting a client fails

client_post_success = list()
for client in missing_clients:
    log.ClientInsertBegin(client=client.name).record("INFO")

    client_post_data = [client.get_post_payload()]      # Halo accepts dict wrapped in a list
    response = halo_client_api.post(
        session=halo_client_session,
        json=client_post_data)

    client_post_success += [bool(response)]
    if not response:
        log.ClientInsertFail(client=client.name).record("WARNING")

log.ClientInsertResult(sum(client_post_success), len(client_post_success) - sum(client_post_success)).record("INFO")


##############################
# Backup post client actions #
##############################

for client, success in zip(missing_clients, client_post_success):
    if not success:
        continue

    backup_id = general.generate_random_hex(8)
    log.ClientInsertBackupBegin(client=client.name, backup_id=backup_id).record("INFO")

    client_post_data = [client.get_post_payload()]
    try:
        n_rows_inserted = sql_backup_table.insert(
            session_id=SESSION_ID,
            backup_id=backup_id,
            action="insert",
            old="",
            new=json.dumps(client_post_data))
    except sqlite3.Error as sql_error:
        log.ClientInsertBackupFail(client=client.name, error=sql_error).record("WARNING")
    else:
        if not n_rows_inserted:
            log.ClientInsertBackupFail(client=client.name).record("WARNING")
