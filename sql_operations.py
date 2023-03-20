
# standard
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
    sql_statement = f"CREATE TABLE IF NOT EXISTS {table} ({columns_string})"
    sql_cursor.execute(sql_statement)
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
    return sql_cursor.rowcount


def update_rows(table: str, connection: sqlite3.Connection, where: tuple = None, **kwargs) -> int:
    """
    Returns number of changed rows.
    """
    sql_cursor = connection.cursor()
    column_assignments = list()
    column_values = tuple()
    for column_name, value in kwargs.items():
        column_assignments += [f"{column_name} = ?"]
        column_values += (value,)
    column_assignments_string = ",".join(column_assignments)

    placeholder_values = column_values + where[1]
    sql_statement = f"""
        UPDATE {table}
        SET {column_assignments_string}
        WHERE {where[0]};
        """
    sql_cursor.execute(sql_statement, placeholder_values)
    connection.commit()
    return sql_cursor.rowcount


def get_columns_types(table: str, connection: sqlite3.Connection) -> dict:
    """
    Gives {column name: column SQLite type}
    """
    sql_cursor = connection.cursor()
    sql_statement = f"PRAGMA table_info({table})"
    # Gives tuples where 2nd and 3rd element is column name and type
    response = sql_cursor.execute(sql_statement)
    columns = {column_info[1]: column_info[2] for column_info in response.fetchall()}
    return columns


def count_rows(table: str, connection: sqlite3.Connection) -> int:
    """
    Get the number of rows a table has.
    :param table:
    :param connection:
    :return:
    """
    sql_cursor = connection.cursor()
    sql_statement = f"SELECT COUNT(*) FROM {table}"
    response = sql_cursor.execute(sql_statement)
    n_rows = response.fetchone()[0]
    return n_rows


class SqlInterface:
    def __init__(self, path: str, table: str, columns: dict):
        self.table = table
        self.columns = columns
        self.connection = get_connection(path)
        self.create_table()

    def __repr__(self):
        return str(self.info())

    def create_table(self):
        create_table(
            table=self.table,
            columns=self.columns,
            connection=self.connection)
        self.connection.commit()

    def select(self, where: (str | list[str]) = None) -> list[dict]:
        """
        Get data from the table.
        Data is filtered if "where"-statements are given. Otherwise, all data from the table is returned.
        :param where: "where"-statements. A single string or a list of strings. E.g. "WHERE column1 != 'red'"
        :return: Selected data
        """
        # Parse where inputs
        if where:
            where = [where] if not isinstance(where, list) else where       # Make sure where variable is a list
            where_parsed = [parse_where_parameter(parameter) for parameter in where]
            where_statement, where_values = compile_where_statement(where_parsed)
            where = (where_statement, where_values)
        # Read
        selected_data = read_table(
            table=self.table,
            connection=self.connection,
            where=where)
        return selected_data

    def insert(self, **kwargs) -> int:
        """Insert a row to the table. Returns the number of rows inserted (0 or 1)"""
        n_rows_inserted = insert_row(
            table=self.table,
            connection=self.connection,
            **kwargs)
        self.connection.commit()
        return n_rows_inserted

    def update(self, where: (str | list[str]) = None, **kwargs):
        """
        Column name - value pairs that are to be changed.
        :param where:
        :param kwargs:
        :return:
        """
        # Parse where inputs
        if where:
            where = [where] if not isinstance(where, list) else where  # Make sure where variable is a list
            where_parsed = [parse_where_parameter(parameter) for parameter in where]
            where_statement, where_values = compile_where_statement(where_parsed)
            where = (where_statement, where_values)
        # Update
        n_rows_changed = update_rows(
            table=self.table,
            connection=self.connection,
            where=where,
            **kwargs)
        self.connection.commit()
        return n_rows_changed

    def info(self):
        """
        :return: {"n_rows": 5, "columns": {"column1": "INTEGER", "column2": "FLOAT", ...}}
        """
        columns_types = get_columns_types(self.table, self.connection)
        n_rows = count_rows(self.table, self.connection)
        return {"n_rows": n_rows, "columns": columns_types}


class SqlTableSessions(SqlInterface):
    default_table = "sessions"
    default_columns = {
        "session_id": "TEXT",
        "time_unix": "INTEGER",
        "status": "TEXT",
        "clients_synced": "INTEGER",
        "sites_synced": "INTEGER",
        "assets_synced": "INTEGER"}

    def __init__(self, path: str):
        super().__init__(path, table=self.default_table, columns=self.default_columns)


class SqlTableBackup(SqlInterface):
    default_table = "backup"
    default_columns = {
        "session_id": "TEXT",
        "backup_id": "TEXT",
        "action": "TEXT",
        "old": "TEXT",
        "new": "TEXT",
        "post_successful": "INTEGER"}
    allowed_backup_actions = ["insert", "update", "remove"]

    def __init__(self, path: str):
        super().__init__(path, table=self.default_table, columns=self.default_columns)

    def insert(self, session_id: str, backup_id: str, action: str, old: str, new: str,
               post_successful: (bool, int)) -> None:
        """
        Function to enter row to SQL backup table with structured parameters.
        :param session_id: Session id, hexadecimal string
        :param backup_id: Id of the specific backup row. Hexadecimal string
        :param action: Backup action type (e.g. insert, remove...)
        :param old: Old value that can be restored
        :param new: New value to recognize what was changed
        :param post_successful: Indication that the backed up change was successfully posted to Halo
        :return: None
        """
        insert_row(
            table=self.table,
            connection=self.connection,
            session_id=session_id,
            backup_id=backup_id,
            action=action.lower(),
            old=old,
            new=new,
            post_successful=post_successful)
