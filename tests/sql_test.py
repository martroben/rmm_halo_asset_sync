
# standard
import pytest
import sqlite3
# local
import sql_operations


create_table_dict = {"text_column": "TEXT", "integer_column": "INTEGER", "float_column": "FLOAT"}
insert_row_dict1 = {"text_column": "text1", "integer_column": 1}
insert_row_dict2 = {"integer_column": 2, "float_column": 3.4}
insert_row_dict3 = {"text_column": "text3", "integer_column": 1, "float_column": 5.6}


def test_create_table():
    test_connection = sqlite3.connect(":memory:")
    sql_operations.create_table("test_table", create_table_dict, test_connection)


def test_insert_rows():
    test_connection = sqlite3.connect(":memory:")
    sql_operations.create_table("test_table", create_table_dict, test_connection)
    row1_result = sql_operations.insert_row("test_table", test_connection, **insert_row_dict1)
    row2_result = sql_operations.insert_row("test_table", test_connection, **insert_row_dict2)
    row3_result = sql_operations.insert_row("test_table", test_connection, **insert_row_dict3)
    assert row3_result == row2_result == row1_result == 1

    cursor = test_connection.cursor()
    cursor.execute("SELECT * FROM test_table")
    row_tuples = list()
    for row in [insert_row_dict1, insert_row_dict2, insert_row_dict3]:
        row_tuples += [tuple(row.get(column, None) for column in ["text_column", "integer_column", "float_column"])]
    data = cursor.fetchall()
    assert row_tuples == data


def test_update_rows_single_column():
    test_connection = sqlite3.connect(":memory:")
    sql_operations.create_table("test_table", create_table_dict, test_connection)
    sql_operations.insert_row("test_table", test_connection, **insert_row_dict1)
    sql_operations.insert_row("test_table", test_connection, **insert_row_dict2)
    sql_operations.insert_row("test_table", test_connection, **insert_row_dict3)

    n_updated_rows = sql_operations.update_rows(
        table="test_table",
        connection=test_connection,
        text_column="updated_text",
        where=("integer_column = ?", (1,)))
    assert n_updated_rows == 2

    cursor = test_connection.cursor()
    cursor.execute("SELECT * FROM test_table")
    updated_row_tuples = (row[0] for row in cursor.fetchall() if row[1] == "integer_column")
    assert all(text_column == "updated_text" for text_column in updated_row_tuples)


def test_update_rows_multiple_columns():
    test_connection = sqlite3.connect(":memory:")
    sql_operations.create_table("test_table", create_table_dict, test_connection)
    sql_operations.insert_row("test_table", test_connection, **insert_row_dict1)
    sql_operations.insert_row("test_table", test_connection, **insert_row_dict2)
    sql_operations.insert_row("test_table", test_connection, **insert_row_dict3)

    n_updated_rows = sql_operations.update_rows(
        table="test_table",
        connection=test_connection,
        text_column="updated_text2",
        float_column=123.45,
        where=("integer_column = ?", (1,)))
    assert n_updated_rows == 2

    cursor = test_connection.cursor()
    cursor.execute("SELECT * FROM test_table")
    updated_row_tuples = (row[0] for row in cursor.fetchall() if row[1] == "integer_column")
    assert all(text_column == "updated_text2" for text_column in updated_row_tuples)


def test_update_rows_null_changed_rows():
    test_connection = sqlite3.connect(":memory:")
    sql_operations.create_table("test_table", create_table_dict, test_connection)
    sql_operations.insert_row("test_table", test_connection, **insert_row_dict1)
    sql_operations.insert_row("test_table", test_connection, **insert_row_dict2)
    sql_operations.insert_row("test_table", test_connection, **insert_row_dict3)

    n_updated_rows = sql_operations.update_rows(
        table="test_table",
        connection=test_connection,
        text_column="updated_text",
        where=("integer_column = ?", (999,)))
    assert n_updated_rows == 0


def test_column_info():
    test_connection = sqlite3.connect(":memory:")
    sql_operations.create_table("test_table", create_table_dict, test_connection)
    sql_operations.insert_row("test_table", test_connection, **insert_row_dict1)
    sql_operations.insert_row("test_table", test_connection, **insert_row_dict2)
    sql_operations.insert_row("test_table", test_connection, **insert_row_dict3)

    expected_result = {
        "text_column": "TEXT",
        "integer_column": "INTEGER",
        "float_column": "FLOAT"}

    received_result = sql_operations.get_columns_types("test_table", test_connection)
    assert received_result == expected_result


def test_column_info_nonexisting_table():
    test_connection = sqlite3.connect(":memory:")
    result = sql_operations.get_columns_types("nonexisting_table", test_connection)
    assert result == {}


def test_row_count():
    test_connection = sqlite3.connect(":memory:")
    sql_operations.create_table("test_table", create_table_dict, test_connection)
    sql_operations.insert_row("test_table", test_connection, **insert_row_dict1)
    sql_operations.insert_row("test_table", test_connection, **insert_row_dict2)
    sql_operations.insert_row("test_table", test_connection, **insert_row_dict3)

    n_rows = sql_operations.count_rows("test_table", test_connection)
    assert n_rows == 3


def test_row_count_nonexisting_table():
    test_connection = sqlite3.connect(":memory:")
    n_rows = sql_operations.count_rows("nonexisting_table", test_connection)
    assert n_rows == 0


def test_read_table():
    table_interface = sql_operations.SqlInterface(
        path=":memory:",
        table="test_table",
        columns={"text_column": "TEXT", "integer_column": "INTEGER", "float_column": "FLOAT"})

    rows_to_insert = [insert_row_dict1, insert_row_dict2, insert_row_dict3, insert_row_dict1]
    for row in rows_to_insert:
        table_interface.insert(**row)

    read_rows = sql_operations.read_table("test_table", table_interface.connection)
    read_rows_existing_values = list()
    read_rows_existing_values += [{key: value for key, value in row.items() if value is not None} for row in read_rows]
    assert rows_to_insert == read_rows_existing_values


def test_read_table_where():
    table_interface = sql_operations.SqlInterface(
        path=":memory:",
        table="test_table",
        columns={"text_column": "TEXT", "integer_column": "INTEGER", "float_column": "FLOAT"})

    rows_to_insert = [insert_row_dict1, insert_row_dict2, insert_row_dict3, insert_row_dict1]
    for row in rows_to_insert:
        table_interface.insert(**row)

    where1 = table_interface.select()
    where1_existing_values = [{key: value for key, value in row.items() if value is not None} for row in where1]
    assert where1_existing_values == rows_to_insert

    where2 = table_interface.select(where="integer_column = 1")
    where2_existing_values = [{key: value for key, value in row.items() if value is not None} for row in where2]
    assert where2_existing_values == [insert_row_dict1, insert_row_dict3, insert_row_dict1]

    where3 = table_interface.select(where="integer_column != 1")
    where3_existing_values = [{key: value for key, value in row.items() if value is not None} for row in where3]
    assert where3_existing_values == [insert_row_dict2]

    where4 = table_interface.select(where="integer_column > 1")
    where4_existing_values = [{key: value for key, value in row.items() if value is not None} for row in where4]
    assert where4_existing_values == [insert_row_dict2]

    where5 = table_interface.select(where=["integer_column < 2", "float_column > 5"])
    where5_existing_values = [{key: value for key, value in row.items() if value is not None} for row in where5]
    assert where5_existing_values == [insert_row_dict3]

    where6 = table_interface.select(where=["text_column IN (text1, text3)"])
    where6_existing_values = [{key: value for key, value in row.items() if value is not None} for row in where6]
    assert where6_existing_values == [insert_row_dict1, insert_row_dict3, insert_row_dict1]


def test_read_data_where_error():
    table_interface = sql_operations.SqlInterface(
        path=":memory:",
        table="test_table",
        columns={"text_column": "TEXT", "integer_column": "INTEGER", "float_column": "FLOAT"})

    rows_to_insert = [insert_row_dict1, insert_row_dict2, insert_row_dict3, insert_row_dict1]
    for row in rows_to_insert:
        table_interface.insert(**row)

    with pytest.raises(ValueError):
        where_result = table_interface.select(where="integer_column =")


def test_sessions_table():
    sessions_table = sql_operations.SqlTableSessions(":memory:")
    assert sessions_table.info()["columns"] == sessions_table.default_columns


def test_backup_table():
    backup_table = sql_operations.SqlTableBackup(":memory:")
    assert backup_table.info()["columns"] == backup_table.default_columns


def test_backup_table_insert():
    backup_table = sql_operations.SqlTableBackup(":memory:")
    backup_table.insert(
        session_id="ABC123",
        backup_id="CDE456",
        action="insert",
        old=None,
        new="{'new_value': 11111}",
        post_successful=False)

    cursor = backup_table.connection.cursor()
    cursor.execute(f"SELECT * FROM {backup_table.table}")
    data = cursor.fetchall()
    assert data == [('ABC123', 'CDE456', 'insert', None, "{'new_value': 11111}", 0)]

    # Check inserting forbidden action
    with pytest.raises(sqlite3.Error):
        backup_table.insert(
            session_id="a",
            backup_id="a",
            action="forbidden_action",
            old=None,
            new="a",
            post_successful=False)


def test_backup_table_update():
    backup_table = sql_operations.SqlTableBackup(":memory:")
    backup_table.insert(
        session_id="ABC123",
        backup_id="CDE456",
        action="update",
        old=None,
        new="{'new_value': 11111}",
        post_successful=False)

    backup_table.update(post_successful=True)

    cursor = backup_table.connection.cursor()
    cursor.execute(f"SELECT * FROM {backup_table.table}")
    data = cursor.fetchall()
    assert data == [('ABC123', 'CDE456', 'update', None, "{'new_value': 11111}", 1)]
