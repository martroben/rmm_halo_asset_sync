
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
    """
    Get SQLite data type name that corresponds to input python data type name.
    :param python_type: Name of Python type to convert
    :return: SQLite data type name
    """
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
    Typecast "where"-string value component from str type to a more proper type
    :param value: "where"-string value component. E.g. 99 from "column1 < 99"
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
    :param value: Value component from SQL "IN" statement. E.g. (99, 100) from "column1 IN (99, 100)"
    :return: A list of parsed values
    """
    separated_at_comma = [string.strip(r"'\"()") for string in value.split(",")]
    parsed_values = list()
    for string in separated_at_comma:           # Strip whitespaces if the string isn't entirely made of whitespaces.
        parsed_values += [string.strip(" ") if len(string.strip(" ")) else string]
    return parsed_values


def compile_where_statement(parsed_inputs: list[tuple]) -> tuple[str, list]:
    """
    Takes a list of parsed "where"-statement tuples. Return a tuple with "?"-placeholders in the following form:
    ("where"-string formatted with "?"-placeholders, list of corresponding values).
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
    :return: List of dicts corresponding to the rows, in the form of {column1: value1, column2: value2, ...}
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
    :param kwargs: Key-value pairs to insert. In the form of column name: value
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
    Update values in existing rows, specified by the "where"-statement.
    If no "where"-statement is given, updates all rows
    :param table: Name of the table to update
    :param connection: SQLite connection object
    :param where: SQL-like "where"-statement. See sql_operations.parse_where_parameter for supported operators
    :param kwargs: Key-value pairs to update. In the form of column name: value
    :return: Number of changed rows.
    """
    sql_cursor = connection.cursor()
    column_assignments = list()
    column_values = tuple()
    for column_name, value in kwargs.items():
        column_assignments += [f"{column_name} = ?"]
        column_values += (value,)
    column_assignments_string = ",".join(column_assignments)

    placeholder_values = column_values
    sql_statement = f"""
        UPDATE {table}
        SET {column_assignments_string};
        """

    if where:
        where_statement = where[0]
        where_placeholder_values = tuple(where[1])
        placeholder_values += where_placeholder_values
        sql_statement = sql_statement.replace(";", f"WHERE {where_statement};")

    sql_cursor.execute(sql_statement, placeholder_values)
    connection.commit()
    return sql_cursor.rowcount


def get_columns_types(table: str, connection: sqlite3.Connection) -> dict:
    """
    Query SQLite table for column names and their types
    :param table: Name of the table
    :param connection: SQLite connection object
    :return: A dict in the form of {column_name1: column_SQLite_type1, column_name2: ...}
    """
    sql_cursor = connection.cursor()
    sql_statement = f"PRAGMA table_info({table})"
    # Gives tuples where 2nd and 3rd element is column name and type
    response = sql_cursor.execute(sql_statement)
    columns = {column_info[1]: column_info[2] for column_info in response.fetchall()}
    return columns


def count_rows(table: str, connection: sqlite3.Connection) -> int:
    """
    Query a SQLite table for the number of rows.
    :param table: Name of the table
    :param connection: SQLite connection object
    :return: Number of rows in the table. If table doesn't exist, returns 0.
    """
    sql_cursor = connection.cursor()
    sql_statement = f"SELECT COUNT(*) FROM {table}"
    try:
        response = sql_cursor.execute(sql_statement)
        n_rows = response.fetchone()[0]
    except sqlite3.OperationalError as error:
        if "no such table" in str(error).lower():
            n_rows = 0
        else:
            raise
    return n_rows


class SqlInterface:
    """
    Base class for managing SQLite queries.
    Instance variables:
    table: str - name of table
    columns: dict - Column info in the form {"column1_name": "column1 SQLite type", "column2_name": ...}
    connection: sqlite3.Connection - SQLite connection object
    """

    def __init__(self, path: str, table: str, columns: dict):
        """Save necessary instance variables and create a table if it doesn't exist."""
        self.table = table
        self.columns = columns
        self.connection = get_connection(path)
        self.create_table()

    def __repr__(self):
        return str(self.info())

    def create_table(self):
        """Create a table by the name specified in instance attributes."""
        create_table(
            table=self.table,
            columns=self.columns,
            connection=self.connection)
        self.connection.commit()

    def select(self, where: (str | list[str]) = None) -> list[dict]:
        """
        Get data from the table.
        Data is also filtered if "where"-statements are given. Otherwise, all data from the table is returned
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

    def update(self, where: (str | list[str]) = None, **kwargs) -> int:
        """
        Update values in the table specified in kwargs.
        Only specific rows are changed if a "where"-statement is provided.
        :param where: "where"-statements. A single string or a list of strings. E.g. "WHERE column1 != 'red'"
        :param kwargs: Values to update in the form of column_name=new_value
        :return: Number of rows updated
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
        Get general info about table: number of rows + columns and their types
        :return: {"n_rows": 5, "columns": {"column1": "INTEGER", "column2": "FLOAT", ...}}
        """
        columns_types = get_columns_types(self.table, self.connection)
        n_rows = count_rows(self.table, self.connection)
        return {"n_rows": n_rows, "columns": columns_types}


class SqlTableSessions(SqlInterface):
    """Interface for sessions table"""
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
    """Interface for backup table."""
    default_table = "backup"
    default_columns = {
        "session_id": "TEXT",
        "backup_id": "TEXT",
        "action": "TEXT",
        "old": "TEXT",
        "new": "TEXT",
        "post_successful": "INTEGER"}
    allowed_backup_actions = ["insert", "update", "remove"]     # Other values can't be inserted

    def __init__(self, path: str):
        super().__init__(path, table=self.default_table, columns=self.default_columns)

    def insert(self, session_id: str, backup_id: str, action: str, old: (str | None), new: str,
               post_successful: (bool, int)) -> int:
        """
        Function to enter row to SQL backup table with structured parameters.
        :param session_id: Session id, hexadecimal string
        :param backup_id: Id of the specific backup row. Hexadecimal string
        :param action: Backup action type (e.g. insert, remove...)
        :param old: Old value that can be restored
        :param new: New value to recognize what was changed
        :param post_successful: Indication that the backed up change was successfully posted to Halo
        :return: Number of rows inserted (0 or 1)
        """
        if action.lower() not in self.allowed_backup_actions:
            raise sqlite3.Error(f"Invalid backup action: '{action}'. "
                                f"Allowed actions: {', '.join(self.allowed_backup_actions)}.")
        n_rows_inserted = insert_row(
            table=self.table,
            connection=self.connection,
            session_id=session_id,
            backup_id=backup_id,
            action=action.lower(),
            old=old,
            new=new,
            post_successful=int(post_successful))
        return n_rows_inserted
