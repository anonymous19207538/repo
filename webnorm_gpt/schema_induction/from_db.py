from collections import defaultdict
from datetime import datetime

from .. import logger
from .db import DbColumn, DbDump, DbSchema, DbTable, DbValue
from .induction import JsonSchemaInducer


def preprocess_db_value(value) -> DbValue:
    if isinstance(value, datetime):
        value = value.strftime("%Y-%m-%d %H:%M:%S")
    return value


def dump_tables_dump_schema(
    conn, inducer: None | JsonSchemaInducer = None
) -> tuple[DbDump, DbSchema]:
    cursor = conn.cursor()
    cursor.execute("show tables;")

    all_tables = []
    for table in cursor.fetchall():
        all_tables.append(table[0])

    db_dump = DbDump(tables=[])
    db_schema = DbSchema()

    if inducer is None:
        inducer = JsonSchemaInducer()

    for table in all_tables:
        cursor.execute(f"select * from {table};")
        columns = [column[0] for column in cursor.description]
        columns_data = [[] for _ in columns]

        has_data = False
        for row in cursor.fetchall():
            for i, value in enumerate(row):
                value = preprocess_db_value(value)
                columns_data[i].append(value)
                has_data = True

        if not has_data:
            continue

        db_columns = []
        for column, values in zip(columns, columns_data):
            db_column = DbColumn(
                name=column, schema=inducer.induce_json_schema(values), values=values
            )
            db_columns.append(db_column)

        if len(db_columns) == 0:
            continue

        db_table = DbTable(name=table, columns=db_columns, expanded_columns=[])
        db_dump.tables.append(db_table)

        db_schema.schemas[table] = {}
        for column in db_columns:
            db_schema.schemas[table][column.name] = column.schema

    return db_dump, db_schema


def dump_tables_with_schema(conn, schema: DbSchema) -> DbDump:
    cursor = conn.cursor()
    db_dump = DbDump(tables=[])

    for table, column_schemas in schema.schemas.items():
        cursor.execute(f"select * from {table};")
        columns = [column[0] for column in cursor.description]
        columns_data: list[list] = [[] for _ in columns]

        columns_schema_to_columns: dict[str, None | int] = defaultdict(lambda: None)

        for i, column in enumerate(columns):
            if column not in column_schemas:
                logger.warning("Column %s not in schema for table %s", column, table)
            else:
                columns_schema_to_columns[column] = i

        count_rows = 0
        for row in cursor.fetchall():
            count_rows += 1
            for i, value in enumerate(row):
                value = preprocess_db_value(value)
                columns_data[i].append(value)

        db_columns = []
        for column in column_schemas.keys():
            columns_data_index = columns_schema_to_columns[column]
            if columns_data_index is None:
                logger.warning("Column %s not found in table %s", column, table)
                columns_data_cur = [None] * count_rows
            else:
                columns_data_cur = columns_data[columns_data_index]
            db_column = DbColumn(
                name=column, schema=column_schemas[column], values=columns_data_cur  # type: ignore
            )
            db_columns.append(db_column)

        if len(db_columns) == 0:
            continue

        db_table = DbTable(name=table, columns=db_columns, expanded_columns=[])
        db_dump.tables.append(db_table)
    return db_dump
