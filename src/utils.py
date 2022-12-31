import json


def load_data_from_file(filename):
    rows = []
    expected_keys = ["Id", "PostId", "VoteTypeId", "CreationDate"]
    with open(filename, "r") as f:
        for line in f:
            data = json.loads(line)
            if all(key in data.keys() for key in expected_keys):
                if "UserId" not in data.keys():
                    data["UserId"] = None
                rows.append(data)
        return rows
