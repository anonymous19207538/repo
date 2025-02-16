from collections import defaultdict

from .. import logger
from ..file_types.binlog_file import DbTableBinlog
from ..file_types.log_file import LogFile
from .back_to_log import db_table_to_log
from .db import DbDump, DbTable
from .join import (
    ColumnRelation,
    ColumnRelationTypes,
    do_join_foreign_key,
    do_join_nearest_related_before,
)


def join_all(
    db: DbDump,
    foreign_key_results: list,
    dataflow_map: dict[str, list[str]],
    binlog: dict[str, DbTableBinlog],
) -> tuple[dict[str, LogFile], dict[str, DbTable]]:
    foreign_key_map = defaultdict(list)
    for from_table, from_column, to_table, to_column in foreign_key_results:
        foreign_key_map[from_table].append((from_column, to_table, to_column))

    log_tables = list()
    for table in db.tables:
        if table.name.startswith("log::"):
            log_tables.append(table)

    has_dups = set()

    log_results = {}
    table_results = {}

    for i, table in enumerate(log_tables):
        table_prefix, real_table_name = table.name.split("::", 1)
        if table_prefix != "log":
            continue

        joins = foreign_key_map[table.name]
        table_joined = table.copy()

        dup_table_names = set()
        visited_tables = set()

        new_joins = []
        for from_column, to_table, to_column in joins:
            if "log_data.response." in from_column:
                continue
            new_joins.append((from_column, to_table, to_column))

        for _, to_table, _ in new_joins:
            if to_table in visited_tables:
                dup_table_names.add(to_table)
            visited_tables.add(to_table)

        dup_table_name_counter = defaultdict(int)

        for from_column, to_table, to_column in new_joins:

            if (to_table, to_column) in has_dups:
                continue

            to_values = db.find_table(to_table).find_expanded_column(to_column).values
            to_values_non_null = [v for v in to_values if v is not None]
            if len(to_values_non_null) == 0 or len(to_values_non_null) != len(
                set(to_values_non_null)
            ):
                has_dups.add((to_table, to_column))
                continue

            to_table_original_name = to_table.split("::", 1)[1]
            to_name = to_table_original_name
            if to_table in dup_table_names:
                # to_name = f"{to_table_original_name}_join_on_{from_column}_{to_column}"
                counter = dup_table_name_counter[to_table]
                dup_table_name_counter[to_table] += 1
                to_name = f"{to_table_original_name}#{counter}"
            try:
                relation = ColumnRelation(
                    ColumnRelationTypes.ForeignKey,
                    table_joined.name,
                    from_column,
                    to_table,
                    to_column,
                    to_name,
                )
                do_join_foreign_key(
                    table_joined,
                    from_column,
                    db,
                    relation,
                    f"{to_name}@",
                    binlog.get(to_table_original_name, None) if binlog is not None else None,
                )
            except Exception as e:
                logger.warning(
                    "Failed to join %s to %s", table_joined.name, to_table, exc_info=e
                )

        d_flows = dataflow_map[real_table_name]
        for d_flow in set(d_flows):
            try:
                relation = ColumnRelation(
                    ColumnRelationTypes.NearestRelatedBefore,
                    table_joined.name,
                    "",
                    f"log::{d_flow}",
                    "",
                    d_flow,
                )
                do_join_nearest_related_before(
                    table_joined, "", db, relation, f"log::{d_flow}@"
                )
            except Exception as e:
                logger.warning(
                    "Failed to join %s to log::%s",
                    table_joined.name,
                    d_flow,
                    exc_info=e,
                )

        log_file = db_table_to_log(table_joined)

        log_results[real_table_name] = log_file
        table_results[table.name] = table_joined
    return log_results, table_results
