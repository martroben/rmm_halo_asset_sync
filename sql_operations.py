
# standard
from abc import ABC, abstractmethod
from functools import partial
import logging
import sqlite3


class SqlInterface(ABC):

    @property
    @abstractmethod
    def name(self):
        """Name of the SQL table"""

    @property
    @abstractmethod
    def columns(self):
        """Names of columns in the SQL table"""

    def __init__(self, connection, active=True, log_name="root"):
        self.log_name = log_name
        self.connection = connection
        self.active = active
        if not self.active:
            # If active=False, replace editing functions with mock action.
            self.insert = partial(self.mock_query, sql_action="insert")
            self.update = partial(self.mock_query, sql_action="update")
        else:
            self.check()

    def check(self) -> None:
        if not table_exists(self.name, self.connection):
            create_table(
                table=self.name,
                columns=self.columns,
                connection=self.connection,
                log_name=self.log_name)
        if not table_exists(table=self.name, connection=self.connection, log_name=self.log_name):
            error_string = f"SQL table not created. Table: '{self.name}'."
            raise self.connection.Error(error_string)

    def read(self, where: str = "") -> list[dict]:
        result = read_table(
            table=self.name,
            connection=self.connection,
            where=where,
            log_name=self.log_name)
        return result

    def insert(self, **kwargs) -> None:
        insert_row(
            table=self.name,
            connection=self.connection,
            log_name=self.log_name,
            **kwargs)

    def update(self, column: str, value: (int, float, str, bool), where: str = "") -> None:
        update_row(
            column=column,
            value=value,
            table=self.name,
            connection=self.connection,
            where=where,
            log_name=self.log_name)

    def info(self) -> dict:
        if not self.active:
            return {"active": 0}
        result = get_table_info(
            table=self.name,
            connection=self.connection,
            log_name=self.log_name)
        result["active"] = 1
        return result

    def mock_query(self, *args, **kwargs) -> None:
        logger = logging.getLogger(self.log_name)

        sql_action = kwargs.pop("sql_action", "")
        arguments = ", ".join([argument for argument in args])
        keyword_arguments = ", ".join([f"{key}: {value}" for key, value in kwargs.items()])
        logger.debug(f"MOCK SQL QUERY. Table: {self.name}. "
                     f"Received action: {sql_action}. "
                     f"Received positional arguments: {arguments}. "
                     f"Received keyword arguments: {keyword_arguments}.")
        logger.info("MOCK SQL QUERY. No action taken.")


class SqlTableSessions(SqlInterface):
    name = "sessions"
    columns = {
        "session_id": "TEXT",
        "time_unix": "INTEGER",
        "status": "TEXT",
        "clients_synced": "INTEGER",
        "sites_synced": "INTEGER",
        "assets_synced": "INTEGER"}


class SqlTableBackup(SqlInterface):
    name = "backup"
    columns = {
        "session_id": "TEXT",
        "backup_id": "TEXT",
        "action": "TEXT",
        "old": "TEXT",
        "new": "TEXT",
        "post_successful": "INTEGER"}
    backup_actions = ["insert", "update", "remove"]

    def insert(self, session_id: str, backup_id: str, action: str, old: str, new: str,
               post_successful: (bool, int), **kwargs) -> None:
        """
        Function to enter row to SQL backup table with structured parameters.

        :param session_id: Session id, hexadecimal string
        :param backup_id: Id of the specific backup row. Hexadecimal string
        :param action: Backup action type (e.g. insert, remove...)
        :param old: Old value that can be restored
        :param new: New value to recognize what was changed
        :param post_successful: Indication that the backed up change was successfully posted to Halo
        :param kwargs: Needs free arguments to provide log_name to mock insert function.
        :return: None
        """
        if action not in self.backup_actions:
            raise sqlite3.Error(f"Not a valid backup action: '{action}'. "
                                f"Allowed actions: {', '.join(self.backup_actions)}.")
        insert_row(
            table=self.name,
            connection=self.connection,
            session_id=session_id,
            backup_id=backup_id,
            action=action,
            old=old,
            new=new,
            post_successful=post_successful,
            log_name=self.log_name)


def table_exists(table: str, connection: sqlite3.Connection, **kwargs) -> bool:
    """
    Check if table exists in SQLite.
    :param table: Table name to search.
    :param connection: SQL connection object.
    :return: True/False whether the table exists
    """
    log_name = kwargs.pop("log_name", "root")       # Get log name.
    logger = logging.getLogger(log_name)            # Use root if no log name provided.

    check_table_query = f"SELECT EXISTS (SELECT name FROM sqlite_master WHERE type='table' AND name='{table}');"
    sql_cursor = connection.cursor()
    logger.debug(f"SQL query to execute: {check_table_query}")
    query_result = sql_cursor.execute(check_table_query)
    table_found = bool(query_result.fetchone()[0])
    return table_found


def create_table(table: str, columns: dict, connection: sqlite3.Connection, **kwargs) -> None:
    """
    Creates table in SQLite
    :param table: table name
    :param columns: A dict in the form of {column name: column type}
    :param connection: SQL connection object.
    :return: None
    """
    log_name = kwargs.pop("log_name", "root")       # Get log name.
    logger = logging.getLogger(log_name)            # Use root if no log name provided.

    columns_string = ",\n\t".join([f"{key} {value}" for key, value in columns.items()])
    create_table_command = f"CREATE TABLE {table} (\n\t{columns_string}\n);"
    sql_cursor = connection.cursor()
    logger.debug(f"SQL query to execute: {create_table_command}")
    sql_cursor.execute(create_table_command)
    connection.commit()
    return


def read_table(table: str, connection: sqlite3.Connection, where: str = "", **kwargs) -> list[dict]:
    """
    Get data from a SQL table.
    :param table: SQL table name.
    :param connection: SQL connection.
    :param where: Optional SQL WHERE filtering clause: e.g. "column = value" or "column IN (1,2,3)".
    :return: A list of column_name:value dicts.
    """
    log_name = kwargs.pop("log_name", "root")       # Get log name.
    logger = logging.getLogger(log_name)            # Use root if no log name provided.

    where_statement = f" WHERE {where}" if where else ""
    get_data_command = f"SELECT * FROM {table}{where_statement};"
    sql_cursor = connection.cursor()
    logger.debug(f"SQL query to execute: {get_data_command}")
    response = sql_cursor.execute(get_data_command)
    data = response.fetchall()
    data_column_names = [item[0] for item in response.description]

    data_rows = list()
    for row in data:
        data_row = {key: value for key, value in zip(data_column_names, row)}
        data_rows += [data_row]
    return data_rows


def insert_row(table: str, connection: sqlite3.Connection, **kwargs) -> None:
    """
    Inserts a Listing to SQL table.
    :param table: Name of SQL table where the data should be inserted to.
    :param connection: SQL connection object.
    :param kwargs: Key-value pairs to insert.
    :return: None
    """
    log_name = kwargs.pop("log_name", "root")           # Get log name.
    logger = logging.getLogger(log_name)                # Use root if no log name provided.

    column_names = list()
    values = list()
    for column_name, value in kwargs.items():
        column_names += [column_name]
        if isinstance(value, str):
            value = value.replace("'", "''")            # Change single quotes to double single quotes
            value = f"'{value}'"                        # Add quotes to string variables
        values += [str(value)]
    column_names_string = ",".join(column_names)
    values_string = ",".join(values)
    insert_data_command = f"INSERT INTO {table} ({column_names_string})\n" \
                          f"VALUES\n\t({values_string});"
    sql_cursor = connection.cursor()
    logger.debug(f"SQL query to execute: {insert_data_command}")
    sql_cursor.execute(insert_data_command)
    connection.commit()
    return


def update_row(column: str, value: str, table: str,
               connection: sqlite3.Connection, where: str = "", **kwargs) -> None:
    """
    Sets the specified column value in SQL table.
    :param table: Table name in SQL database.
    :param connection: SLQ connection object.
    :param column: Column/parameter to change in table.
    :param value: Value to assign to the column.
    :param where: SQL WHERE statement string.
    :return: None
    """
    log_name = kwargs.pop("log_name", "root")           # Get log name.
    logger = logging.getLogger(log_name)                # Use root if no log name provided.

    where_statement = f" WHERE {where}" if where else ""
    update_table_command = f"UPDATE {table} SET {column} = {value}{where_statement};"
    sql_cursor = connection.cursor()
    logger.debug(f"SQL query to execute: {update_table_command}")
    sql_cursor.execute(update_table_command)
    connection.commit()
    return


def get_table_info(table: str, connection: sqlite3.Connection, **kwargs) -> dict:
    """
    Get dict with basic table info
    :param table: Table name in SQL database.
    :param connection: SLQ connection object.
    :return: Dict with values columns (column  names and types) and n_rows.
    """
    log_name = kwargs.pop("log_name", "root")           # Get log name.
    logger = logging.getLogger(log_name)                # Use root if no log name provided.

    get_table_info_command = f"SELECT name, type FROM pragma_table_info('{table}');"
    get_n_rows_command = f"SELECT COUNT(id) FROM {table};"
    sql_cursor = connection.cursor()
    logger.debug(f"SQL query to execute: {get_table_info_command}")
    columns_response = sql_cursor.execute(get_table_info_command)
    columns = [{"name": name, "type": column_type} for name, column_type in columns_response.fetchall()]
    logger.debug(f"SQL query to execute: {get_n_rows_command}")
    n_rows_response = sql_cursor.execute(get_n_rows_command)
    n_rows = n_rows_response.fetchone()[0]
    return {"columns": columns, "n_rows": n_rows}
