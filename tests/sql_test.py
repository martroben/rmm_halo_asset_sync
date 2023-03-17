
import sqlite3
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
    assert row3_result == row2_result == row3_result == 1

    cursor = test_connection.cursor()
    cursor.execute("SELECT * FROM test_table")
    row_tuples = list()
    for row in [insert_row_dict1, insert_row_dict2, insert_row_dict3]:
        row_tuples += [tuple(row.get(column, None) for column in ["text_column", "integer_column", "float_column"])]
    data = cursor.fetchall()
    assert row_tuples == data


def test_update_rows():
    test_connection = sqlite3.connect(":memory:")
    sql_operations.create_table("test_table", create_table_dict, test_connection)
    sql_operations.insert_row("test_table", test_connection, **insert_row_dict1)
    sql_operations.insert_row("test_table", test_connection, **insert_row_dict2)
    sql_operations.insert_row("test_table", test_connection, **insert_row_dict3)

    sql_operations.update_rows(
        table="test_table",
        connection=test_connection,
        text_column="updated_text",
        where=("integer_column = ?", (1,)))

    cursor = test_connection.cursor()
    cursor.execute("SELECT * FROM test_table")
    updated_row_tuples = (row[0] for row in cursor.fetchall() if row[1] == "integer_column")
    assert all(text_column == "updated_text" for text_column in updated_row_tuples)