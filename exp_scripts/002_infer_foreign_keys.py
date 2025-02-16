import json
import os
import pickle
import sys

import zstandard

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from webnorm_gpt import logger
from webnorm_gpt.gpt_invoker import GPTInvoker
from webnorm_gpt.schema_induction.foreign_key_infer_force import (
    extract_relavant_tables,
    filter_columns,
    gpt_foreign_key_filter,
    infer_foreign_key,
)


def main():
    cur_path = os.path.dirname(os.path.abspath(__file__))

    logger.info("Loading db and schemas")
    with zstandard.open(os.path.join(cur_path, "db_and_schemas.pickle.zst"), "rb") as f:
        db, _, _, _ = pickle.load(f)

    logger.info("Loading related entity dict")
    with open(
        os.path.join(
            cur_path,
            "../webnorm_gpt/sql/microservice_static_analysis/method_related_entity_train_ticket.json",
        ),
        "r",
    ) as f:
        related_entity_dict = json.load(f)

    gpt_invoker = GPTInvoker(
        model="gpt-4o",
        turns=30,
        dump_gpt_log=True,
    )

    filtered_tables_columns = filter_columns(gpt_invoker, db)

    table_foreign_keys_pairs = [
        (table, foreign_keys_columns)
        for table, foreign_keys_columns, _ in filtered_tables_columns
    ]

    logger.info("Extracting relevant tables")
    related_tables_dict = extract_relavant_tables(
        gpt_invoker, table_foreign_keys_pairs, related_entity_dict
    )

    # for table, related_tables in related_tables_dict.items():
    #     print(table)
    #     print('*'*20)
    #     for related_table in related_tables:
    #         print(related_table)
    #     print()

    logger.info("Inferring foreign keys")
    foreign_keys = infer_foreign_key(filtered_tables_columns, related_tables_dict)

    logger.info("Filtering foreign key results")
    foreign_key_results = gpt_foreign_key_filter(foreign_keys, gpt_invoker)

    for table_from, column_from, table_to, column_to in foreign_key_results:
        print(f"{table_from}.{column_from} -> {table_to}.{column_to}")

    logger.info("Dumping to file")
    with zstandard.open(
        os.path.join(
            cur_path,
            "sql-foreign-keys.json.zst",
        ),
        "wt",
    ) as f:
        json.dump(foreign_key_results, f)

    logger.info("Done")


if __name__ == "__main__":
    main()
