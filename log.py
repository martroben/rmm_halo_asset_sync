

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


def sql_table_initiation_error
