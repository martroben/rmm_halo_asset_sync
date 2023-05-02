# standard
import logging
import os
import requests
import sqlite3


class LogString:
    """
    Parent class for log entries. Stores short and full versions of the log message and the logger to use.
    Subclasses have log messages for specific scenarios, that can be easily translated.
    """
    exception: Exception                                    # Exception to be included in log message
    logger: logging.Logger                                  # Logger to use for logging the message
    context: str                                            # Function name or source of log
    exception_type: str                                     # Exception type name
    short: str                                              # Short log message for http responses
    full: str                                               # Long log message for saved logs

    def __init__(self, short: str, full: str = None, context: str = None,
                 exception: Exception = None, logger_name: str = None):

        logger = logging.getLogger(logger_name) if logger_name else logging.getLogger(os.getenv("LOGGER_NAME", "root"))
        self.logger = logger

        if exception:
            self.exception = exception
            self.exception_type = exception.__class__.__name__
        self.short = short
        self.full = full or short
        self.context = context

    def __str__(self):
        return self.full

    def record(self, level: str) -> None:
        """"Execute" the log message - i.e. send it to the specified logger"""
        context = f"[{self.context}] " if self.context else ""
        log_message = f"{context}{self.full}"
        self.logger.log(
            level=logging.getLevelName(level.upper()),
            msg=log_message)


############################
# sync_clients log strings #
############################

class EnvVariablesMissing(LogString):
    """Error string for critical env variables not being present."""
    def __init__(self, missing_variables: list):
        short = "Critical env variables not found."
        full = f"{short} Missing variables: {', '.join(missing_variables)}. Exiting."
        super().__init__(short, full)


class SqlSetupBegin(LogString):
    """Info about beginning SQL setup."""
    def __init__(self, path):
        short = "Connecting to SQL."
        full = f"{short} SQL path: {path}."
        super().__init__(short, full)


class SqlInsertSessionInfo(LogString):
    """Info about inserting session info to SQL."""
    def __init__(self, session_id):
        short = "Inserting session info to SQL."
        full = f"{short} Session ID: {session_id}."
        super().__init__(short, full)


class HaloTokenRequestBegin(LogString):
    """Info about requesting Halo token."""
    def __init__(self):
        short = "Requesting Halo token."
        super().__init__(short)


class HaloTokenRequestFail(LogString):
    """Token request fail message."""
    def __init__(self, connection_error: Exception = None):
        short = "Failed to get Halo API token."
        full = f"{short} Error: {connection_error}. Exiting." or ""
        super().__init__(short, full)


class NsightClientsRequestBegin(LogString):
    """Info about requesting N-sight clients."""
    def __init__(self):
        short = "Requesting N-sight clients."
        super().__init__(short)


class NsightClientsRequestFail(LogString):
    """N-sight api client request failed."""
    def __init__(self, code: int, reason: str):
        short = "Failed to get N-sight clients."
        full = f"{short} Code: {code} ({reason})."
        super().__init__(short, full)


class HaloClientRequestBegin(LogString):
    """Info about requesting Halo clients."""
    def __init__(self):
        short = "Requesting Halo clients."
        super().__init__(short)


class HaloClientRequestFail(LogString):
    """Halo clients request fail message."""
    def __init__(self, connection_error: Exception = None):
        short = "Failed to get Halo clients."
        full = f"{short} Error: {connection_error}. Exiting."
        super().__init__(short, full)


class HaloToplevelRequestBegin(LogString):
    """Info about requesting Halo toplevels."""
    def __init__(self):
        short = "Requesting Halo toplevels."
        super().__init__(short)


class HaloToplevelRequestFail(LogString):
    """Halo toplevels request fail message."""
    def __init__(self, connection_error: Exception = None):
        short = "Failed to get Halo toplevels."
        full = f"{short} Error: {connection_error}. Exiting." or ""
        super().__init__(short, full)


class NoMatchingToplevel(LogString):
    """No matching toplevel found in Halo."""
    def __init__(self, toplevel_name: str):
        short = "Toplevel designated for N-sight clients does not exist in Halo."
        full = f"{short} Toplevel: {toplevel_name}. Exiting."
        super().__init__(short, full)


class NoMissingClients(LogString):
    """All N-sight clients are already present in Halo."""
    def __init__(self):
        short = "No new N-sight clients found. Exiting with no action."
        super().__init__(short)


class InsertNClients(LogString):
    """Info about n un-synced clients found from N-sight."""
    def __init__(self, n_clients: int):
        short = "Found N-sight clients that are not synced to Halo."
        full = f"{short} Count: {n_clients}."
        super().__init__(short, full)


class ClientInsertBegin(LogString):
    """Info about adding new client to Halo."""
    def __init__(self, client: str):
        short = "Adding new client to Halo."
        full = f"{short} Client: {client}."
        super().__init__(short, full)


class ClientInsertFail(LogString):
    """Adding client to Halo failed."""
    def __init__(self, client: str):
        short = "Failed to add new client to Halo."
        full = f"{short} Client: {client}."
        super().__init__(short, full)


class ClientInsertResult(LogString):
    """Client sync result."""
    def __init__(self, n_success: int, n_fail: int):
        short = "Finished inserting clients to Halo."
        full = f"{short} Successfully inserted: {n_success}. Failed to insert: {n_fail}."
        super().__init__(short, full)


class ClientInsertBackupBegin(LogString):
    """Info about backup action starting."""
    def __init__(self, client: str, backup_id: str):
        short = "Inserting client to SQL backup table."
        full = f"{short} Client: {client}. Backup id: {backup_id}."
        super().__init__(short, full)


class ClientInsertBackupFail(LogString):
    """Client backup failed."""
    def __init__(self, client: str, error: sqlite3.Error = None):
        short = "Failed to backup client insert action to SQL."
        full = f"{short} Client: {client}. Error: {error}"
        super().__init__(short, full)


##############################
# sql_operations log strings #
##############################

class SqlCreateTableSyntax(LogString):
    """Debug string to record SQL create table syntax."""
    def __init__(self, sql_statement: str):
        super().__init__(f"Creating table by SQL statement: {sql_statement}")


#######################
# general log strings #
#######################

class RetryAttempt(LogString):
    """Detailed log string for retry attempt."""
    def __init__(self, function, exception: Exception, n_retries: int, interval_sec: float, attempt: int):
        short = f"Retrying function '{function.__name__}' in {round(interval_sec, 2)} seconds, " \
                f"because {type(exception).__name__} occurred. Attempt {attempt} of {n_retries}."
        full = f"{short} Exception: {exception}"
        super().__init__(short=short, full=full, exception=exception)


class RetryFailed(LogString):
    """Failed retry log entry."""
    def __init__(self, function, exception: Exception, n_retries: int):
        short = f"Retrying function '{function.__module__}.{function.__name__}' failed after {n_retries} attempts, " \
                 f"because {type(exception).__name__} occurred."
        super().__init__(short=short, exception=exception)


#############################
# halo_requests log strings #
#############################

class BadResponse(LogString):
    """Bad response from http request."""
    def __init__(self, method: str, url: str, response: requests.Response, context: str):
        short = f"{method} request failed with {response.status_code} ({response.reason}). URL: {url}"
        super().__init__(short=short, context=context)


###################
# log log strings #
###################

class RedactFail(LogString):
    """Redactor couldn't process and redact input object."""
    def __init__(self, input_object: object, exception: Exception):
        short = f"Failed to redact log entry."
        full = f"{short} Input object: {input_object}. Object type: {type(input_object)}. Value error: {exception}."
        super().__init__(short=short, full=full, exception=exception)


# sqlite3.OperationalError

# def sql_read_table_syntax(sql_statement: str) -> str:
#     return f"Selecting from table by SQL statement: {sql_statement}"
#
#
# def sql_insert_row_syntax(sql_statement: str) -> str:
#     return f"Inserting row by SQL statement: {sql_statement}"
#
#
# def sql_update_row_syntax(sql_statement: str) -> str:
#     return f"Updating row by SQL statement: {sql_statement}"
#
#
# def sql_get_table_info(sql_statement: str) -> str:
#     return f"Getting table info by SQL statement: {sql_statement}"
#
#
# def sql_get_table_row_count(sql_statement: str) -> str:
#     return f"Getting table row count by SQL statement: {sql_statement}"
#
#
# def sql_nonexisting_table(sql_statement: str) -> str:
#     return f"Tried to query non-existing table. SQL statement: {sql_statement}"
#
#
# def sql_table_initiation_error():
#     pass
