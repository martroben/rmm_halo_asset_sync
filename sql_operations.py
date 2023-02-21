
import sqlite3


class SqlInterfaceSessions:

    name = "sessions"
    columns = {
        "id": "TEXT",
        "status": "TEXT",
        "clients_synced": "INTEGER",
        "sites_synced": "INTEGER",
        "assets_synced": "INTEGER"}

    def __init__(self, connection):
        self.connection = connection
        self.check()

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

    def insert(self, data: dict) -> None:
        insert_row(
            data=data,
            table=self.name,
            connection=self.connection)

    def update(self, column: str, value: (int, float, str, bool), where: str = "") -> None:
        update_row(
            column=column,
            value=value,
            table=self.name,
            connection=self.connection,
            where=where)

    def info(self):
        # columns, types, nrow
        pass


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


def insert_row(data: dict, table: str, connection: sqlite3.Connection) -> None:
    """
    Inserts a Listing to SQL table.
    :param data: Key-value pairs in dict.
    :param table: Name of SQL table where the data should be inserted to.
    :param connection: SQL connection object.
    :return: None
    """
    insert_data_command = f"INSERT INTO {table} ({','.join(data.keys())})\n" + \
                          f"VALUES\n\t({','.join(data.values())});"
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

