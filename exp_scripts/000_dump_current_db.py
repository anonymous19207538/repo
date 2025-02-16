import os
import pickle
import sys

import mysql.connector
import zstandard

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from webnorm_gpt import logger
from webnorm_gpt.file_types.binlog_file import dump_all_table_keys
from webnorm_gpt.file_types.merge_query_orders import (
    merge_db_table_keys,
    merge_db_tables,
    train_ticket_db_merge_info,
)
from webnorm_gpt.schema_induction.from_db import dump_tables_dump_schema


def main():
    cur_path = os.path.dirname(os.path.abspath(__file__))

    logger.info("Connecting to db")
    conn = mysql.connector.connect(
        host="127.0.0.1",
        user="ts",
        password="Ts_123456",
        database="ts",
        port=53306,
    )

    logger.info("Dumping current db and schemas")
    db_dump, db_schema = dump_tables_dump_schema(conn)
    db_dump, db_schema = merge_db_tables(
        db_dump, db_schema, train_ticket_db_merge_info()
    )

    logger.info("Dumping table keys")
    table_keys = dump_all_table_keys(conn, "ts")
    table_keys = merge_db_table_keys(table_keys, train_ticket_db_merge_info())

    logger.info("Dumping to file")
    with zstandard.open(
        os.path.join(cur_path, "current_db_and_schemas.pkl.zst"), "wb"
    ) as f:
        pickle.dump((db_dump, db_schema, table_keys), f)

    logger.info("Done")


if __name__ == "__main__":
    main()
