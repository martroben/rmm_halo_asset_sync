
import sqlite3
from abc import ABC, abstractmethod


class SqlInterface(ABC):

    active = 0

    @property
    @abstractmethod
    def name(self):
        """Name of the SQL table"""

    @property
    @abstractmethod
    def columns(self):
        """Names of columns in the SQL table"""

    def __init__(self, connection, active=1):
        self.connection = connection
        if active:
            self.check()
            self.active = 1
        else:
            self.insert = self.null_function
            self.update = self.null_function

    def check(self) -> None:
        if not table_exists(self.name, self.connection):
            create_table(
                table=self.name,
                columns=self.columns,
                connection=self.connection)
        if not table_exists(self.name, self.connection):
            error_string = f"SQL table not created. Table: '{self.name}'."
            raise self.connection.Error(error_string)

    def read(self, where: str = "") -> list[dict]:
        result = read_table(
            table=self.name,
            connection=self.connection,
            where=where)
        return result

    def insert(self, **kwargs) -> None:
        insert_row(
            table=self.name,
            connection=self.connection,
            **kwargs)

    def update(self, column: str, value: (int, float, str, bool), where: str = "") -> None:
        update_row(
            column=column,
            value=value,
            table=self.name,
            connection=self.connection,
            where=where)

    def info(self) -> dict:
        if not self.active:
            return {"active": 0}
        result = get_table_info(
            table=self.name,
            connection=self.connection)
        result["active"] = 1
        return result

    def null_function(self, *args, **kwargs):
        pass


class SqlTableSessions(SqlInterface):

    name = "sessions"
    columns = {
        "id": "TEXT",
        "time_unix": "INTEGER",
        "status": "TEXT",
        "clients_synced": "INTEGER",
        "sites_synced": "INTEGER",
        "assets_synced": "INTEGER"}


class SqlTableBackup(SqlInterface):

    name = "backup"
    columns = {
        "session_id": "TEXT",
        "action": "TEXT",
        "old": "TEXT",
        "new": "TEXT"}
    backup_actions = ["insert", "update", "remove"]

    def insert(self, session_id, action, old, new) -> None:
        if action not in self.backup_actions:
            raise sqlite3.Error(f"Not a valid backup action: '{action}'. "
                                f"Allowed actions: {', '.join(self.backup_actions)}.")
        insert_row(
            table=self.name,
            connection=self.connection,
            session_id=session_id,
            action=action,
            old=old,
            new=new)


def table_exists(table: str, connection: sqlite3.Connection) -> bool:
    """
    Check if table exists in SQLite.
    :param table: Table name to search.
    :param connection: SQL connection object.
    :return: True/False whether the table exists
    """
    check_table_query = f"SELECT EXISTS (SELECT name FROM sqlite_master WHERE type='table' AND name='{table}');"
    sql_cursor = connection.cursor()
    query_result = sql_cursor.execute(check_table_query)
    table_found = bool(query_result.fetchone()[0])
    return table_found


def create_table(table: str, columns: dict, connection: sqlite3.Connection) -> None:
    """
    Creates table in SQLite
    :param table: table name
    :param columns: A dict in the form of {column name: column type}
    :param connection: SQL connection object.
    :return: None
    """
    columns_string = ",\n\t".join([f"{key} {value}" for key, value in columns.items()])
    create_table_command = f"CREATE TABLE {table} (\n\t{columns_string}\n);"
    sql_cursor = connection.cursor()
    sql_cursor.execute(create_table_command)
    connection.commit()
    return


def read_table(table: str, connection: sqlite3.Connection, where: str = "") -> list[dict]:
    """
    Get data from a SQL table.
    :param table: SQL table name.
    :param connection: SQL connection.
    :param where: Optional SQL WHERE filtering clause: e.g. "column = value" or "column IN (1,2,3)".
    :return: A list of column_name:value dicts.
    """
    where_statement = f" WHERE {where}" if where else ""
    get_data_command = f"SELECT * FROM {table}{where_statement};"
    sql_cursor = connection.cursor()
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
    # log debug: insert_data_command
    sql_cursor = connection.cursor()
    sql_cursor.execute(insert_data_command)
    connection.commit()
    return


def update_row(column: str, value: str, table: str,
               connection: sqlite3.Connection, where: str = "") -> None:
    """
    Sets the specified column value in SQL table.
    :param table: Table name in SQL database.
    :param connection: SLQ connection object.
    :param column: Column/parameter to change in table.
    :param value: Value to assign to the column.
    :param where: SQL WHERE statement string.
    :return: None
    """
    where_statement = f" WHERE {where}" if where else ""
    update_table_command = f"UPDATE {table} SET {column} = {value}{where_statement};"
    sql_cursor = connection.cursor()
    sql_cursor.execute(update_table_command)
    connection.commit()
    return


def get_table_info(table: str, connection: sqlite3.Connection) -> dict:
    """
    Get dict with basic table info
    :param table: Table name in SQL database.
    :param connection: SLQ connection object.
    :return: Dict with values columns (column  names and types) and n_rows.
    """
    get_table_info_command = f"SELECT name, type FROM pragma_table_info('{table}');"
    get_n_rows_command = f"SELECT COUNT(id) FROM {table};"
    cursor = connection.cursor()
    columns_response = cursor.execute(get_table_info_command)
    columns = [{"name": name, "type": column_type} for name, column_type in columns_response.fetchall()]
    n_rows_response = cursor.execute(get_n_rows_command)
    n_rows = n_rows_response.fetchone()[0]
    return {"columns": columns, "n_rows": n_rows}
