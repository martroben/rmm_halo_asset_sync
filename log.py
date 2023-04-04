
# standard
import logging
import os
import re
import sys

import requests


class Redactor(logging.Filter):
    redact_replacement_string = "<REDACTED_INFO>"

    def __init__(self, patterns: list[re.Pattern] = None):
        super().__init__()
        self.patterns = patterns or list()

    def filter(self, record: logging.LogRecord) -> bool:
        """
        Override the filter method to redact data from logs
        :param record: log record object
        :return: Always true - i.e. apply filter
        """
        for pattern in self.patterns:
            record.msg = pattern.sub(self.redact_replacement_string, record.msg)
        return True


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

        if context:
            self.context = context

        if full is None:
            full = short

        self.short = short
        self.full = full

    def __str__(self):
        return self.full

    def record(self, level: str) -> None:
        """"Execute" the log message - i.e. send it to the specified handler"""
        log_message = f"[{self.context}] {self.full}"
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
    def __init__(self, function, exception: Exception, n_retries: int, interval_sec: float, attempt: int, context: str):
        short = f"Retrying function '{function.__name__}' in {round(interval_sec, 2)} seconds, " \
                f"because {type(exception).__name__} occurred. Attempt {attempt} of {n_retries}."
        full = f"{short} Exception: {exception}"
        super().__init__(short=short, full=full, exception=exception, context=context)


class RetryFailed(LogString):
    """Failed retry log entry."""
    def __init__(self, function, exception: Exception, n_retries: int, context: str):
        short = f"Retrying function '{function.__name__}' failed after {n_retries} attempts, " \
                 f"because {type(exception).__name__} occurred."
        super().__init__(short=short, exception=exception, context=context)


#############################
# halo_requests log strings #
#############################

class BadResponse(LogString):
    """Bad response from http request."""
    def __init__(self, method: str, url: str, response: requests.Response, context: str):
        short = f"{method} request failed with {response.status_code} ({response.reason}). URL: {url}"
        super().__init__(short=short, context=context)





# sqlite3.OperationalError

def sql_read_table_syntax(sql_statement: str) -> str:
    return f"Selecting from table by SQL statement: {sql_statement}"


def sql_insert_row_syntax(sql_statement: str) -> str:
    return f"Inserting row by SQL statement: {sql_statement}"


def sql_update_row_syntax(sql_statement: str) -> str:
    return f"Updating row by SQL statement: {sql_statement}"


def sql_get_table_info(sql_statement: str) -> str:
    return f"Getting table info by SQL statement: {sql_statement}"


def sql_get_table_row_count(sql_statement: str) -> str:
    return f"Getting table row count by SQL statement: {sql_statement}"


def sql_nonexisting_table(sql_statement: str) -> str:
    return f"Tried to query non-existing table. SQL statement: {sql_statement}"


def sql_table_initiation_error():
    pass
