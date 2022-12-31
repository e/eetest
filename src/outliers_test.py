import os

from src.outliers import OutLiersQuery
from src.utils import load_data_from_file

SAMPLE_FILE_PATH = (
    os.path.dirname(os.path.abspath(__file__)) + "/test-resources/samples-votes.jsonl"
)


def test_outliers():
    detect_outliers = OutLiersQuery(":memory:")
    data = load_data_from_file(SAMPLE_FILE_PATH)
    for item in data:
        detect_outliers.db.insert_new_row(item)
    result = list(detect_outliers.get_outlier_generator())
    expected_result = [
        (2022, 0, 1),
        (2022, 1, 3),
        (2022, 2, 3),
        (2022, 5, 1),
        (2022, 6, 1),
        (2022, 8, 1),
    ]
    assert result == expected_result
