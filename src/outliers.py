from os.path import dirname, abspath

from src.db import DB

DB_PATH = dirname(dirname(abspath(__file__))) + "/warehouse.db"


class OutLiersQuery:
    def __init__(self, db_path=DB_PATH):
        self.db = DB(db_path)

    def get_outlier_generator(self):
        data = self.db.outliers_query()
        return ((int(item[0]), int(item[1]), int(item[2])) for item in data)


if __name__ == "__main__":
    outlier_generator = OutLiersQuery().get_outlier_generator()
    for week_data in outlier_generator:
        print(", ".join([str(item) for item in week_data]))
