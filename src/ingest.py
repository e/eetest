import sys
from os.path import dirname, abspath

from src.db import DB
from src.utils import load_data_from_file


SRC_PATH = dirname(dirname(abspath(__file__))) + "/uncommitted/votes.jsonl"
DB_PATH = dirname(dirname(abspath(__file__))) + "/warehouse.db"


class DataImporter:
    def __init__(self, filename=SRC_PATH, db_path=DB_PATH):
        self.db = DB(db_path)
        # TODO: Use a generator if necessary
        try:
            self.data = load_data_from_file(filename)
        except FileNotFoundError:
            print("No data found. Please download the dataset using 'make fetch_data'")
            self.data = []

    def import_data(self):
        for item in self.data:
            self.db.insert_new_row(item)


if __name__ == "__main__":
    DataImporter(sys.argv[1]).import_data()
