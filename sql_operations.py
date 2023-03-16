
# standard
import logging
import os
import re
import sqlite3


def get_connection(path: str) -> sqlite3.Connection:
    """
    Get SQLite connection to a given database path.
    If database doesn't exist, creates a new database and path directories to it (unless path is :memory:).
    :param path: Path to SQLite database
    :return: sqlite3 Connection object to input path
    """
    if path != ":memory:":
        if not os.path.exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))
    connection = sqlite3.connect(path)
    return connection


def create_table(table: str, columns: dict, connection: sqlite3.Connection) -> None:
    """
    Creates a table in SQLite
    :param table: table name
    :param columns: A dict in the form of {column name: SQLite type name}
    :param connection: SQLite connection object.
    :return: None
    """
    sql_cursor = connection.cursor()
    columns_string = ",".join([f"{key} {value}" for key, value in columns.items()])
    sql_statement = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            {columns_string})
        """
    sql_cursor.execute(sql_statement)
    print(sql_cursor.fetchall())
    connection.commit()
    return


def get_sql_type(python_type: str) -> str:
    """Get SQLite data type name that corresponds to input python data type name."""
    sql_type_reference = {
        "int": "INTEGER",
        "float": "REAL",
        "str": "TEXT",
        "NoneType": "NULL"}
    return sql_type_reference.get(python_type, "BLOB")


def parse_where_parameter(statement: str) -> tuple:
    """
    Take a single SQL-like "where"-statement and parse it to components.
    E.g. "column_name < 99" --> ("column_name", "<", "99")
    Supported operators: =, !=, <, >, IN
    :return: A tuple with the following values: (column_name, operator, value)
    """
    operators = ["=", "!=", "<", ">", r"\sin\s"]
    operators_pattern = '|'.join(operators)
    statement = statement.strip()

    column_pattern = re.compile(rf"^.+?(?=({operators_pattern}))", re.IGNORECASE)
    column_match = column_pattern.search(statement)
    if column_match:
        column = column_match.group(0).strip()
    else:
        raise ValueError(
            f"Can't parse the column name from the where statement. "
            f"Problematic statement: '{statement}'")

    operator_pattern = re.compile(operators_pattern, re.IGNORECASE)
    operator_match = operator_pattern.search(statement)
    if operator_match:
        operator_raw = operator_match.group(0)
        operator = operator_raw.strip()
    else:
        raise ValueError(
            f"Can't parse the operator part from the where statement. "
            f"Problematic statement: '{statement}'")

    value_pattern = re.compile(rf"^.+?{operator_raw}(.+$)", re.IGNORECASE)
    value_match = value_pattern.search(statement)
    if value_match:
        value = value_match.group(1).strip()
    else:
        raise ValueError(
            f"Can't parse a searchable value from the where statement. "
            f"Problematic statement: '{statement}'")

    return column, operator, value


def typecast_input_value(value: str) -> (int | float | str):
    """
    Typecast "where"-string value component from string to a more proper type
    :param value: "where"-string value component. E.g. 99 in "column_name < 99"
    :return: Int or float if value is convertible to number. Otherwise, returns input string.
    """
    try:                                                    # Convert to float if no error
        value = float(value)
    except ValueError:
        pass
    if isinstance(value, float) and value == int(value):    # Convert to int, if value doesn't change upon conversion
        value = int(value)
    return value


def parse_in_values(value: str) -> list:
    """
    Parses arguments of sql "IN" statement.
    E.g. '("virginica","setosa")' -> ['virginica', 'setosa']
    :param value: Value component from SQL "IN" statement
    :return: A list of parsed values
    """
    return [string.strip(r"'\"()") for string in value.split(",")]


def compile_where_statement(parsed_inputs: list[tuple]) -> tuple[str, list]:
    """
    Take a list of "where"-statements. Return a tuple in the following form:
    ("where"-string formatted with placeholders, list of corresponding values).
    E.g. [("column1", "<", 99), ("column2", "IN", ["value1", "value2"])] to
    ("WHERE column1 < ? AND column2 IN (?,?)", [99, "value1", "value2"] )
    :param parsed_inputs: List of parsed "where"-statements in the form of (column, operator, value)
    """
    statement_strings = list()
    values = list()
    for statement in parsed_inputs:
        if statement[1].lower() == "in":
            in_values = [typecast_input_value(value) for value in parse_in_values(statement[2])]
            placeholders = ','.join(["?"] * len(in_values))          # String in the form ?,?,?,...
            statement_strings += [f"{statement[0]} {statement[1]} ({placeholders})"]
            values += in_values
        else:
            statement_strings += [f"{statement[0]} {statement[1]} ?"]
            values += [typecast_input_value(statement[2])]
    where_string = f" WHERE {' AND '.join(statement_strings)}"
    return where_string, values


def read_table(table: str, connection: sqlite3.Connection, where: tuple = None) -> list[dict]:
    """
    Get rows from SQLite table. If no "where" argument is supplied, returns all data from table
    :param table: Name of table
    :param connection: SQLite connection object
    :param where: SQL-like "where"-statement. See sql_operations.parse_where_parameter for supported operators
    :return: List of dicts corresponding to the rows returned in the form of {column_name: value, ...}
    """
    # Add where statement values, if given
    sql_statement = f"SELECT * FROM {table};"
    where_values = tuple()
    if where:
        sql_statement = sql_statement.replace(";", f"{where[0]};")
        where_values = where[1]
    # Execute query
    sql_cursor = connection.cursor()
    response = sql_cursor.execute(sql_statement, where_values)
    data = response.fetchall()
    # Format the response as a list of dicts
    data_column_names = [item[0] for item in response.description]
    data_rows = list()
    for row in data:
        data_row = {key: value for key, value in zip(data_column_names, row)}
        data_rows += [data_row]
    return data_rows


def insert_row(table: str, connection: sqlite3.Connection, **kwargs) -> int:
    """
    Inserts a single row to a SQLite table
    :param table: Name of the table to insert to
    :param connection: SQLite connection object
    :param kwargs: Key-value (column name: value) pairs to insert
    :return: Number of rows inserted (1 or 0)
    """
    column_names = list()
    values = tuple()
    for column_name, value in kwargs.items():
        column_names += [column_name]
        values += (value,)
    column_names_string = ",".join(column_names)
    placeholder_string = ", ".join(["?"] * len(column_names))  # As many placeholders as columns. E.g (?, ?, ?, ?)
    sql_statement = f"""
        INSERT INTO {table}
            ({column_names_string})
        VALUES
            ({placeholder_string});
        """
    sql_cursor = connection.cursor()
    sql_cursor.execute(sql_statement, values)
    connection.commit()
    return sql_cursor.rowcount


test_connection = get_connection(":memory:")
create_table("test_table2", {"column1": "TEXT", "column2": "INTEGER"}, test_connection)
insert_row("test_table", test_connection, column1="nimi", column2=11)
update_row("test_table", test_connection, column1="teine_nimi", where=("column1 = ?", ("nimi",)))


def update_row(table: str, connection: sqlite3.Connection, where: tuple = None, **kwargs) -> None:
    """
    """
    sql_cursor = connection.cursor()
    column_assignments = list()
    column_values = tuple()
    for column_name, value in kwargs.items():
        column_assignments += [f"{column_name} = ?"]
        column_values += (value,)
    column_assignments_string = ",".join(column_assignments)

    values = column_values + where[1]
    sql_statement = f"""
        UPDATE {table}
        SET {column_assignments_string}
        WHERE {where[0]};
        """
    sql_cursor.execute(sql_statement, values)
    connection.commit()
    print("update rowcount: ", sql_cursor.rowcount)
    return ############################### See if it's possible to get changed rows or smth


class SqlInterface:

    def __init__(self, path: str, table: str, columns: dict, log_name: str, dryrun: bool):
        self.table = table
        self.columns = columns
        self.logger = logging.getLogger(log_name)
        self.dryrun = dryrun

        ################################### catch connection error
        self.connection = get_connection(path)
        ################################### catch operation error
        create_table(
            table=self.table,
            columns={column_name: get_sql_type(column_type) for column_name, column_type in columns.items()},
            connection=self.connection)

    def select(self, where: (str | list[str]) = None) -> list[dict]:
        """
        Get data from the table.
        Data is filtered if "where"-statements are given. Otherwise, all data from the table is returned.
        :param where: "where"-statements. A single string or a list of strings. E.g. "WHERE column1 != 'red'"
        :return: Selected data
        """
        if where:  # Parse where inputs
            where = [where] if not isinstance(where, list) else where  # Make sure where variable is a list
            where_parsed = [parse_where_parameter(parameter) for parameter in where]
            where_statement, where_values = compile_where_statement(where_parsed)
            where = (where_statement, where_values)
        # Read
        result = read_table(
            table=self.table,
            connection=self.connection,
            where=where)
        return result

    def insert(self, **kwargs) -> int:
        """Insert a row to the table. Returns the number of rows inserted (0 or 1)"""
        n_rows_inserted = insert_row(           ########################## Add logic to log drylog actions
            table=self.table,
            connection=self.connection,
            **kwargs)
        return n_rows_inserted

    def update(self, column: str, value, where: (str | list[str]) = None):
        if where:  # Parse where inputs
            where = [where] if not isinstance(where, list) else where  # Make sure where variable is a list
            where_parsed = [parse_where_parameter(parameter) for parameter in where]
            where_statement, where_values = compile_where_statement(where_parsed)
            where = (where_statement, where_values)

        update_row(
            column=column,
            value=value,
            table=self.name,
            connection=self.connection,
            where=where,
            log_name=self.log_name)



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
    :return: Dict with values columns (column names and types) and n_rows.
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
