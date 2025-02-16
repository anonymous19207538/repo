import json
import os
import pickle
import sys
from collections import defaultdict

import mysql.connector
import tqdm
import zstandard

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))


from webnorm_gpt import logger
from webnorm_gpt.file_types.binlog_file import process_binlog_file
from webnorm_gpt.file_types.compute_context import compute_context_train_ticket_log
from webnorm_gpt.file_types.log_file import (
    LogFile,
    load_from_log_receiver_file,
    load_from_mitm_file,
    split_attacks,
)
from webnorm_gpt.file_types.proj_desc_file import ProjDescFile
from webnorm_gpt.gen_inv.base import Invariant
from webnorm_gpt.gen_inv.check_inv import run_py_predicate_new_json_format
from webnorm_gpt.schema_induction.db import DbDump, DbSchema, db_merge_logs_and_db
from webnorm_gpt.schema_induction.expansion import DbExpander
from webnorm_gpt.schema_induction.from_db import dump_tables_with_schema
from webnorm_gpt.schema_induction.from_log import dump_log_with_schema
from webnorm_gpt.schema_induction.join_all import join_all


def load_log_file(logs: LogFile, schema: DbSchema) -> DbDump:
    for log in logs.log_items:
        compute_context_train_ticket_log(log)

    db = dump_log_with_schema(logs, schema)

    expander = DbExpander()
    for table in db.tables:
        expander.expand_table(table)

    return db


def check_inv_in(
    log_file: LogFile,
    db_dump: DbDump,
    log_schema: DbSchema,
    foreign_key_results,
    dataflow_map,
    invariants,
    attack_file_name: str,
    binlog_file,
) -> bool:
    cur_path = os.path.dirname(os.path.abspath(__file__))

    output_file_idx = 0

    # logger.info("Loading log file...")
    log_db = load_log_file(log_file, log_schema)

    # logger.info("Merging log and db...")
    db = db_merge_logs_and_db(log_db, db_dump)

    # logger.info("Do join all...")
    logs, _ = join_all(db, foreign_key_results, dataflow_map, binlog_file)

    detected = False

    os.makedirs(
        os.path.join(
            cur_path,
            f"../find-bug-dump/{attack_file_name}/",
        ),
        exist_ok=True,
    )

    processed_apis = set()

    for inv in invariants:
        api = inv.domain[0].api
        processed_apis.add(api)

        log_target: LogFile = logs.get(api, None)  # type: ignore
        if log_target is None:
            continue

        for log_item in log_target.log_items:
            assert log_item.api == api

            is_atack = False

            headers = log_item.headers
            attack_name = None
            if "x-att-int" in headers:
                if "x-att-file" in headers:
                    attack_name = headers["x-att-file"]
                else:
                    attack_name = headers["x-att-name"]
                # print(f"ATTACK: {api} {attack_name}")
                # print(f"Arguments: {log_item.arguments}")
                is_atack = True

            check_res, e = run_py_predicate_new_json_format(
                inv.predicate, [log_item], [inv.domain[0].related_fields]  # type: ignore
            )

            if not check_res:
                detected = True
                os.makedirs(
                    os.path.join(
                        cur_path,
                        f"../find-bug-dump/{attack_file_name}/detected",
                    ),
                    exist_ok=True,
                )

            if (
                not check_res
                or is_atack
                or (not check_res and attack_file_name == "NORMAL")
            ):
                dump_file_name = f"{output_file_idx:04d}.md"
                output_file_idx += 1
                dump_file_path = os.path.join(
                    os.path.dirname(__file__),
                    f"../find-bug-dump/{attack_file_name}/",
                    dump_file_name,
                )

                os.makedirs(os.path.dirname(dump_file_path), exist_ok=True)

                api = log_item.api
                arguments = json.dumps(
                    log_item.to_execute_json(inv.domain[0].related_fields), indent=4  # type: ignore
                )
                inv_code = inv.predicate.py_code
                out_txt = DUMP_TEMPLATE.format(
                    api=api,
                    arguments=arguments,
                    inv=inv_code,
                    attack_name=attack_name,
                    is_attack=is_atack,
                    detected=not check_res,
                    err=e,
                )

                with open(dump_file_path, "wt") as f:
                    f.write(out_txt)

    for api, log_target in logs.items():
        if api in processed_apis:
            continue
        for log_item in log_target.log_items:
            is_atack = False

            headers = log_item.headers
            attack_name = None
            if "x-att-int" in headers:
                if "x-att-file" in headers:
                    attack_name = headers["x-att-file"]
                else:
                    attack_name = headers["x-att-name"]
                # print(f"ATTACK: {api} {attack_name}")
                # print(f"Arguments: {log_item.arguments}")
                is_atack = True

            if is_atack:
                dump_file_name = f"{output_file_idx:04d}.md"
                output_file_idx += 1
                dump_file_path = os.path.join(
                    os.path.dirname(__file__),
                    f"../find-bug-dump/{attack_file_name}/",
                    dump_file_name,
                )

                os.makedirs(os.path.dirname(dump_file_path), exist_ok=True)

                api = log_item.api
                arguments = json.dumps(
                    log_item.to_execute_json(inv.domain[0].related_fields), indent=4  # type: ignore
                )
                inv_code = ""
                out_txt = DUMP_TEMPLATE.format(
                    api=api,
                    arguments=arguments,
                    inv=inv_code,
                    attack_name=attack_name,
                    is_attack=is_atack,
                    detected=False,
                    err=None,
                )

                with open(dump_file_path, "wt") as f:
                    f.write(out_txt)

    return detected


DUMP_TEMPLATE = """
# Status

is_attack: {is_attack}
detected: {detected}

# API
{api}

# Attack
{attack_name}

# Request
```json
{arguments}
```

# Invariant
```python
{inv}
```

# ErrMsg
{err}

"""


def load_dataflow_map():
    cur_path = os.path.abspath(os.path.dirname(__file__))
    with zstandard.open(
        os.path.join(
            cur_path, "../generated/hmm_deduction_result_pred_filtered.json.zst"
        ),
        "rt",
    ) as f:
        hmm_deduction_result_pred_filtered = json.load(f)

    results = defaultdict(list)

    for k, v in hmm_deduction_result_pred_filtered.items():
        for t in v:
            if t != k:
                results[k].append(t)

    return results


def load_invariants():
    inv_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "ivn_gen_output.jsonl",
    )

    invariants: list[Invariant] = []
    with open(inv_path, "rt") as f:
        for line in f:
            inv = json.loads(line)
            i = Invariant()
            i.load_from_json(inv)
            invariants.append(i)

    return invariants


def get_splitted_attacks() -> list[LogFile]:
    logger.info("Splitting attacks...")

    proj_desc_file = ProjDescFile()
    proj_desc_file.load_from_file_path(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "../generated/train-ticket-projdesc.json.zst",
        )
    )

    logger.info("Loading MITM logs...")
    logs_mitm = load_from_mitm_file(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "../traffic-new-collect/train-ticket-attack-2/mitm.mitm",
        ),
        proj_desc_file,
        True,
    )

    logs_instru = load_from_log_receiver_file(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "../traffic-new-collect/train-ticket-attack-2/logs.jsonl.zst",
        ),
        proj_desc_file,
    )

    attacks = split_attacks(logs_mitm, logs_instru)

    return attacks


def main():
    cur_path = os.path.dirname(os.path.abspath(__file__))

    logger.info("Loading db and schemas...")
    with zstandard.open(os.path.join(cur_path, "db_and_schemas.pickle.zst"), "rb") as f:
        _, db_schema, log_schema, db_binlogs = pickle.load(f)

    with zstandard.open(
        os.path.join(cur_path, "current_db_and_schemas.pkl.zst"), "rb"
    ) as f:
        db_dump, _, table_keys = pickle.load(f)

    expander = DbExpander()
    for table in db_dump.tables:
        expander.expand_table(table)

    logger.info("Loading training data...")
    training = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../traffic-new-collect/train-ticket-train-2/processed-logs.pkl.zst",
    )
    with zstandard.open(training, "rb") as f:
        training_logs = pickle.load(f)

    logger.info("Loading foreign keys...")
    with zstandard.open(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "sql-foreign-keys.json.zst",
        ),
        "rt",
    ) as f:
        foreign_key_results = json.load(f)

    logger.info("Loading dataflow map...")
    dataflow_map = load_dataflow_map()
    logger.info("Loading invariants...")
    invariants = load_invariants()

    logger.info("Detecting invariants in training data...")
    detected = check_inv_in(
        training_logs,
        db_dump,
        log_schema,
        foreign_key_results,
        dataflow_map,
        invariants,
        "NONE",
        db_binlogs,
    )

    if detected:
        raise Exception("Invariant detected in training data")

    attack_data = get_splitted_attacks()

    detected_num = 0
    total_num = 0

    t = tqdm.tqdm(attack_data, desc="0/0")
    detected = set()

    for i, file in enumerate(t):
        try:
            d = check_inv_in(
                file,
                db_dump,
                log_schema,
                foreign_key_results,
                dataflow_map,
                invariants,
                f"attack-{i:04d}",
                db_binlogs,
            )
        except Exception as e:
            raise
            d = False
        if d:
            detected_num += 1
            detected.add(i)
        total_num += 1

        t.set_description(f"{detected_num}/{total_num} {detected_num/total_num:.03f}")

    logger.info(f"Detected: {detected_num}/{total_num}")
    logger.info(f"Recall: {detected_num/total_num:.03f}")

    detected = sorted(detected)
    logger.info(f"All detected: {detected}")
    undeteced = set(range(len(attack_data))) - set(detected)
    undeteced = sorted(undeteced)
    logger.info(f"All undeteced: {undeteced}")


if __name__ == "__main__":
    main()
