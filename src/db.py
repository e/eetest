import sqlite3
from os.path import dirname, abspath


DB_PATH = dirname(dirname(abspath(__file__))) + "/warehouse.db"

CREATE_TABLE_QUERY = """CREATE TABLE IF NOT EXISTS posts (
   id INTEGER PRIMARY KEY,
   user_id INTEGER,
   post_id INTEGER NOT NULL,
   vote_type_id INTEGER NOT NULL,
   creation_date TEXT
);"""
INSERT_NEW_ROW_QUERY = """INSERT OR REPLACE INTO posts
    (id, user_id, post_id, vote_type_id, creation_date)
    VALUES (:Id, :UserId, :PostId, :VoteTypeId, :CreationDate);"""
SELECT_ROW_BY_ID_QUERY = "SELECT * FROM posts WHERE id = :row_id"
SELECT_ROWS_BY_POST_ID_QUERY = "SELECT * FROM posts WHERE post_id = :post_id"
OUTLIERS_QUERY = """SELECT Strftime('%Y', creation_date) AS year,
       Strftime('%W', creation_date) AS week_number,
       Count(*)                      AS number_of_votes
FROM   posts
GROUP  BY creation_date
HAVING Abs(1 - number_of_votes / (SELECT Avg(number_of_votes)
                                  FROM   (SELECT Count(*) AS number_of_votes
                                          FROM   posts
                                          GROUP  BY creation_date) AS
                                         avg_number_of_votes)) > 0.2"""


class DB:
    def __init__(self, db_path=DB_PATH):
        self.conn = sqlite3.connect(db_path)
        cursor = self.conn.cursor()
        cursor.execute(CREATE_TABLE_QUERY)

    def insert_new_row(self, values_dict):
        cursor = self.conn.cursor()
        cursor.execute(INSERT_NEW_ROW_QUERY, values_dict)
        self.conn.commit()

    def select_row_by_id(self, row_id):
        cursor = self.conn.cursor()
        cursor.execute(SELECT_ROW_BY_ID_QUERY, {"row_id": row_id})
        return cursor.fetchone()

    def select_rows_by_post_id(self, post_id):
        cursor = self.conn.cursor()
        cursor.execute(SELECT_ROWS_BY_POST_ID_QUERY, {"post_id": post_id})
        return cursor.fetchall()

    def outliers_query(self):
        cursor = self.conn.cursor()
        cursor.execute(OUTLIERS_QUERY)
        return cursor.fetchall()

    def run_query(self, query):
        cursor = self.conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()
