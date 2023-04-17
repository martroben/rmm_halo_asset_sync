
# standard
import http.client
import logging
import os
import re
import requests
import sqlite3
import sys


def set_logs_to_stdout(loggers: list[logging.Logger]) -> None:
    """
    Remove all alternative handlers and make sure only root logger has a handler.
    :return:
    """
    handler = logging.StreamHandler(sys.stdout)  # Direct logs to stdout
    for logger in loggers:
        logger.handlers.clear()
    logging.getLogger().addHandler(handler)


def set_formatter(formatter: logging.Formatter, loggers: list[logging.Logger]) -> None:
    """
    Set formatter to all loggers
    :param formatter:
    :param loggers:
    :return:
    """
    for logger in loggers:
        for handler in logger.handlers:
            handler.setFormatter(formatter)


def set_level(level: int, loggers: list[logging.Logger]) -> None:
    """
    Set log level globally.
    :param loggers:
    :param level:
    :return:
    """
    for logger in loggers:
        logger.setLevel(level)
    logging.getLogger().setLevel(level)

    if level <= logging.getLevelName("DEBUG"):
        # Replace print method to capture http request logs
        def capture_print_output(*args) -> None:
            log_string = LogString(
                short=" ".join(args),
                context="http.client.HTTPConnection debug")
            log_string.record("DEBUG")

        http.client.print = capture_print_output
        http.client.HTTPConnection.debuglevel = 1


def set_filter(log_filter: logging.Filter, loggers: list[logging.Logger]) -> None:
    """
    Apply filter to all loggers.
    :param log_filter:
    :param loggers:
    :return:
    """
    for logger in loggers:
        logger.addFilter(log_filter)
    logging.getLogger().addFilter(log_filter)


class Redactor(logging.Filter):
    """logging.Filter subclass to redact patterns from logs"""
    redact_replacement_string = "<REDACTED_INFO>"

    def __init__(self, patterns: list[re.Pattern] = None):
        super().__init__()
        self.patterns = patterns or list()

    def __str__(self):
        return " | ".join(pattern.pattern for pattern in self.patterns)

    def add_pattern(self, pattern: re.Pattern):
        self.patterns += [pattern]

    def filter(self, record: logging.LogRecord) -> bool:
        """
        Overriding the original filter method to redact, rather than filter.
        :return: Always true - i.e. always apply filter
        """
        record.msg = self.redact(record.msg)
        record.args = self.redact(record.args)
        return True

    def redact_string(self, log_string: str):
        for pattern in self.patterns:
            log_string = pattern.sub(self.redact_replacement_string, log_string)
        return log_string

    def redact_number(self, log_number: (int | float)):
        """Only redact numbers that match a redaction pattern exactly, not part of the number."""
        for pattern in self.patterns:
            if pattern.fullmatch(str(log_number)):
                return 1234567890
        return log_number

    def redact(self, log_object):
        if log_object is None:
            return
        if isinstance(log_object, str):
            return self.redact_string(log_object)
        if isinstance(log_object, (int, float)):
            return self.redact_number(log_object)
        if isinstance(log_object, tuple):
            return tuple(self.redact(value) for value in log_object)
        if isinstance(log_object, dict):
            return {key: self.redact(value) for key, value in log_object.items()}
        # For custom types try to typecast to str -> redact -> typecast back to original type
        # Return original object if typecasting results in an error
        try:
            log_object_type = type(log_object)
            redacted_object = self.redact_string(str(log_object))
            return log_object_type(redacted_object)
        except ValueError:
            return log_object


class StandardFormatter(logging.Formatter):
    def __init__(self, indicator: str, session_id: str, dryrun: bool = False):
        dryrun_indicator = dryrun*f" | ---DRYRUN---"
        super().__init__(
            fmt=f"{indicator}{{asctime}} | {session_id}{dryrun_indicator} | {{levelname}}: {{message}}",
            datefmt="%m/%d/%Y %H:%M:%S",
            style="{")


# def setup_logger(name: str, level: (str | int), formatter: logging.Formatter = None,
#                  redaction_filter: logging.Filter = None) -> logging.Logger:
#     """
#     Create a logger with custom format, that can be parsed by external log receiver
#     :param name: Name for the log messages emitted by the application
#     :param level: Level of log messages that the logger sends (DEBUG/INFO/WARNING/ERROR) or (10/20/...)
#     :param formatter:
#     :param redaction_filter:
#     :return: a logging.Logger object with assigned handler and formatter
#     """
#     logger = logging.getLogger(name)
#     level = logging.getLevelName(level.upper()) if isinstance(level, str) else level
#     logger.setLevel(level)
#     handler = logging.StreamHandler(sys.stdout)     # Direct logs to stdout
#     if formatter:
#         handler.setFormatter(formatter)
#     logger.addHandler(handler)
#     if redaction_filter:
#         logger.addFilter(redaction_filter)
#     return logger


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
        """"Execute" the log message - i.e. send it to the specified handler"""
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


class HaloClientRequestBegin(LogString):
    """Info about requesting Halo clients."""
    def __init__(self):
        short = "Requesting Halo clients."
        super().__init__(short)


class HaloClientRequestFail(LogString):
    """Halo clients request fail message."""
    def __init__(self, connection_error: Exception = None):
        short = "Failed to get Halo clients."
        full = f"{short} Error: {connection_error}. Exiting." or ""
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
        short = f"Retrying function '{function.__name__}' failed after {n_retries} attempts, " \
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
