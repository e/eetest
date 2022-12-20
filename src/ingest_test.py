import json
import os

from src.ingest import DataImporter

SAMPLE_FILE_PATH = (
    os.path.dirname(os.path.abspath(__file__)) + "/test-resources/samples-votes.jsonl"
)


def test_import_data():
    data_importer = DataImporter(SAMPLE_FILE_PATH, ":memory:")
    data_importer.import_data()
    result = data_importer.db.run_query(
        "SELECT id, post_id, vote_type_id, creation_date FROM posts"
    )
    expected_result = []
    with open(SAMPLE_FILE_PATH, "r") as f:
        for line in f.readlines():
            data = list(json.loads(line).values())
            data = (int(data[0]), int(data[1]), int(data[2]), data[3])
            expected_result.append(data)
    assert result == expected_result
