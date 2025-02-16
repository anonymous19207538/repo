import os
import pickle
import sys

import zstandard

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from webnorm_gpt import logger
from webnorm_gpt.file_types.binlog_file import process_binlog_file
from webnorm_gpt.file_types.log_file import LogFile
from webnorm_gpt.file_types.merge_query_orders import train_ticket_db_merge_info
from webnorm_gpt.schema_induction.db import DbDump, DbSchema, db_merge_logs_and_db
from webnorm_gpt.schema_induction.expansion import DbExpander
from webnorm_gpt.schema_induction.from_log import dump_log_dump_schema


def main():
    cur_path = os.path.dirname(os.path.abspath(__file__))

    logger.info("Loading current db and schemas")
    db_dump: DbDump
    db_schema: DbSchema
    with zstandard.open(
        os.path.join(cur_path, "current_db_and_schemas.pkl.zst"),
        "rb",
    ) as f:
        db_dump, db_schema, table_keys = pickle.load(f)

    logger.info("Expanding db")
    expander = DbExpander()
    for table in db_dump.tables:
        expander.expand_table(table)

    logger.info("Loading logs from file")
    with zstandard.open(
        os.path.join(
            cur_path,
            "../traffic-new-collect/train-ticket-train-2/processed-logs.pkl.zst",
        ),
        "rb",
    ) as f:
        logs: LogFile = pickle.load(f)

    logger.info("Dumping logs to schema")
    log_db, logs_schema = dump_log_dump_schema(logs)

    logger.info("Expanding logs")
    for table in log_db.tables:
        expander.expand_table(table)

    logger.info("Merging db and logs")
    db = db_merge_logs_and_db(log_db, db_dump)

    logger.info("Processing binlog file")
    binlog_path = os.path.join(
        cur_path, "../traffic-new-collect/binlogs/binlog-train-ticket-new.pkl.zst"
    )
    db_binlogs = process_binlog_file(
        table_keys,
        binlog_path,
        "ts",
        train_ticket_db_merge_info(),
    )

    logger.info("Dumping to file")
    with zstandard.open(os.path.join(cur_path, "db_and_schemas.pickle.zst"), "wb") as f:
        pickle.dump((db, db_schema, logs_schema, db_binlogs), f)

    logger.info("Done")


if __name__ == "__main__":
    main()
