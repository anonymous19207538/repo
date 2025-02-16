import json
from collections import defaultdict
from copy import deepcopy

from .. import logger
from ..file_types.log_file import LogFile
from .db import DbColumn, DbDump, DbSchema, DbTable, DbValue
from .expansion import DbExpander
from .induction import JsonSchemaInducer


def _log_to_log_by_api(logs: LogFile) -> dict[str, list[DbValue]]:
    log_by_api = defaultdict(list)

    for i, log in enumerate(logs.log_items):
        try:
            api = log.api
            if api == "":
                continue
            content = deepcopy(log.content)
            content["seq"] = i
            content["time_parsed"] = log.parse_time()
            content["time_response_parsed"] = log.parse_response_time()
            log_by_api[api].append(content)
        except Exception as e:
            logger.warning(
                "Error while processing log item %d: %s",
                i,
                json.dumps(log.content),
                exc_info=e,
            )

    return log_by_api


def dump_log_dump_schema(
    logs: LogFile, inducer: JsonSchemaInducer | None = None
) -> tuple[DbDump, DbSchema]:
    db_dump = DbDump(tables=[])
    db_schema = DbSchema()

    if inducer is None:
        inducer = JsonSchemaInducer()

    log_by_api = _log_to_log_by_api(logs)
    for api, log_contents in log_by_api.items():
        column = DbColumn(
            name="log_data",
            schema=inducer.induce_json_schema(log_contents),
            values=log_contents,
        )
        table = DbTable(name=api, columns=[column], expanded_columns=[])
        db_dump.tables.append(table)

        db_schema.schemas[api] = {"log_data": column.schema}

    return db_dump, db_schema


def dump_log_with_schema(logs: LogFile, db_schema: DbSchema) -> DbDump:
    db_dump = DbDump(tables=[])
    log_by_api = _log_to_log_by_api(logs)

    for api, table_schemas in db_schema.schemas.items():
        log_contents = log_by_api.get(api, [])
        column = DbColumn(
            name="log_data",
            schema=table_schemas["log_data"],
            values=log_contents,
        )
        table = DbTable(name=api, columns=[column], expanded_columns=[])
        db_dump.tables.append(table)

    return db_dump
