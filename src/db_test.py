import os
import sqlite3

from src.db import DB
from src.utils import load_data_from_file

SAMPLE_FILE_PATH = (
    os.path.dirname(os.path.abspath(__file__)) + "/test-resources/samples-votes.jsonl"
)


def test_sqlite3_connection():
    with sqlite3.connect("warehouse.db") as con:
        cursor = con.cursor()
        assert list(cursor.execute("SELECT 1")) == [(1,)]


def test_init_db():
    db = DB(":memory:")
    cursor = db.conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='posts'")
    result = cursor.fetchall()
    assert ("posts",) in result


def test_insert_row():
    db = DB(":memory:")
    data = load_data_from_file(SAMPLE_FILE_PATH)
    for item in data:
        db.insert_new_row(item)
    expected_rows = [
        (9, None, 2, 2, "2022-01-23T00:00:00.000"),
        (10, None, 2, 2, "2022-01-23T00:00:00.000"),
    ]
    cursor = db.conn.cursor()
    cursor.execute("SELECT * FROM posts WHERE post_id = 2")
    result = cursor.fetchall()
    assert result == expected_rows
    cursor.execute("SELECT count(*) FROM posts")
    result = cursor.fetchone()
    assert result == (16,)


def test_select_row_by_id():
    db = DB(":memory:")
    data = load_data_from_file(SAMPLE_FILE_PATH)
    for item in data:
        db.insert_new_row(item)
    result = db.select_row_by_id(9)
    expected_result = (9, None, 2, 2, "2022-01-23T00:00:00.000")
    assert result == expected_result


def test_select_rows_by_post_id():
    db = DB(":memory:")
    data = load_data_from_file(SAMPLE_FILE_PATH)
    for item in data:
        db.insert_new_row(item)
    result = db.select_rows_by_post_id(2)
    expected_result = [
        (9, None, 2, 2, "2022-01-23T00:00:00.000"),
        (10, None, 2, 2, "2022-01-23T00:00:00.000"),
    ]
    assert result == expected_result


def test_run_query():
    db = DB(":memory:")
    data = load_data_from_file(SAMPLE_FILE_PATH)
    for item in data:
        db.insert_new_row(item)
    result = db.run_query("SELECT * FROM posts WHERE post_id = 2")
    expected_result = [
        (9, None, 2, 2, "2022-01-23T00:00:00.000"),
        (10, None, 2, 2, "2022-01-23T00:00:00.000"),
    ]
    assert result == expected_result
