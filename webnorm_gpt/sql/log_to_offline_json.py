import json
import os
import pickle
import ast
import zstandard
import copy
import traceback
from tqdm import tqdm
from ..file_types.log_file import LogFile, LogItem
from ..file_types.proj_desc_file import ProjDescFile
from ..file_types.compute_context import compute_context_train_ticket


def main():
    with zstandard.open(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "../../traffic-new-collect/night-instru-logs.pkl.zst",
        ),
        "rb",
    ) as f:
        logs: LogFile = pickle.load(f)

    proj_desc = ProjDescFile()
    proj_desc.load_from_file_path(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "../../generated/train-ticket-projdesc.json.zst",
        )
    )

    # open "../../generated/sqls.json"
    with open(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "../../generated/sqls.json"
        ),
        "r",
    ) as f:
        sqls = json.load(f)

    # create offline_json folder in generated if not exists
    offline_json_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "../../generated/offline_json"
    )
    os.makedirs(offline_json_dir, exist_ok=True)

    db_config = {
        "driver": "com.mysql.cj.jdbc.Driver",
        "url": "jdbc:mysql://127.0.0.1:53306/ts",
        "username": "ts",
        "password": "Ts_123456",
    }

    for log_idx, log in enumerate(logs):
        print(log_idx)
        log_dict = log.content
        api_sqls = sqls.get(log_dict["api"], {})
        # api_sqls is a dict
        i = 0
        for sql_name in api_sqls:
            offline_json = {}
            offline_json["dbConfig"] = db_config
            auth = log_dict["headers"].get("authorization", "")
            context = compute_context_train_ticket(auth)
            eval_context = {
                "request": log_dict["arguments"],
                "response": log_dict["response"],
                "headers": log_dict["headers"],
                "context": context,
            }
            offline_json["evalContext"] = eval_context
            offline_json["sql"] = api_sqls[sql_name]

            # write input json for each sql query
            with open(
                os.path.join(
                    os.path.dirname(os.path.abspath(__file__)),
                    f"../../generated/offline_json/{log_idx:08d}---{log_dict['api']}_input_{i}.json",
                ),
                "w",
            ) as f:
                json.dump(offline_json, f, indent=4)
            i += 1


if __name__ == "__main__":
    main()
