
# standard
import logging
import os
import requests


def setup_logger(name: str, level: str, indicator: str, session_id: str, dryrun: bool) -> logging.Logger:
    """
    Create a logger with custom format, that can be parsed by external log receiver
    :param name: Name for the log messages emitted by the application
    :param level: Level of log messages that the logger sends (DEBUG/INFO/WARNING/ERROR)
    :param indicator: Unique indicator to let external log receiver distinguish which stdout entries came from the app
    :param session_id: Session id to add to log messages.
    :param dryrun: If program is running in dryrun mode. Logger indicates it in log messages
    :return: a logging.Logger object with assigned handler and formatter
    """
    logger = logging.getLogger(name)
    logger.setLevel(level.upper())
    handler = logging.StreamHandler()              # Direct logs to stdout
    formatter = logging.Formatter(
        fmt=f"{indicator}{{asctime}} | {session_id} | {{funcName}} | {{levelname}}: {dryrun*'(DRYRUN) '}{{message}}",
        datefmt="%m/%d/%Y %H:%M:%S",
        style="{")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


class LogString:
    """
    Parent class for log entries. Stores short and full versions of the log message and the logger to use.
    Subclasses have log messages for specific scenarios, that can be easily translated.
    """
    exception: Exception                                    # Exception to be included in log message
    logger: logging.Logger                                  # Logger to use for logging the message
    exception_type: str                                     # Exception type name
    short: str                                              # Short log message for http responses
    full: str                                               # Long log message for saved logs

    def __init__(self, short: str, full: str = None, exception: Exception = None, logger_name: str = None):
        if logger_name is None:
            logger = logging.getLogger(os.getenv("LOGGER_NAME", "root"))
        else:
            logger = logging.getLogger(logger_name)
        self.logger = logger

        if exception is not None:
            self.exception = exception
            self.exception_type = exception.__class__.__name__

        if full is None:
            full = short

        self.short = short
        self.full = full

    def __str__(self):
        return self.full

    def record(self, level: str) -> None:
        """"Execute" the log message - i.e. send it to the specified handler"""
        self.logger.log(level=logging.getLevelName(level.upper()), msg=self.full)


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
    def __init__(self, method: str, url: str, response: requests.Response):
        short = f"{method} request failed with {response.status_code} ({response.reason}). URL: {url}"
        super().__init__(short)





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
