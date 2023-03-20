
# standard
import logging

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



# sqlite3.OperationalError
def sql_create_table_syntax(sql_statement: str) -> str:
    return f"Creating table by SQL statement: {sql_statement}"


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
