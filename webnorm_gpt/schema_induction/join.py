# foreign key join
# nearest related before join
# nearest related after join

from dataclasses import dataclass

from ..file_types.binlog_file import DbTableBinlog
from .db import DbDump, DbTable


class ColumnRelationTypes:
    ForeignKey = "foreign_key"
    NearestRelatedBefore = "nearest_related_before"
    NearestRelatedAfter = "nearest_related_after"


@dataclass
class ColumnRelation:
    ty: str
    left_table: str
    left_column: str
    right_table: str
    right_column: str
    back_name: str


def do_join(
    left: DbTable,
    left_column_name: str,
    db: DbDump,
    relation: ColumnRelation,
    right_prefix: str,
) -> DbTable:
    pass


def do_join_foreign_key(
    left: DbTable,
    left_column_name: str,
    db: DbDump,
    relation: ColumnRelation,
    right_prefix: str,
    binlog: DbTableBinlog | None,
) -> DbTable:
    assert relation.ty == ColumnRelationTypes.ForeignKey

    right_table = db.find_table(relation.right_table)
    right_column = right_table.find_expanded_column(relation.right_column)

    right_idx_mapping = {}

    assert right_column.schema.is_primary()

    for i, value in enumerate(right_column.values):
        if value is None:
            continue
        if value in right_idx_mapping:
            raise ValueError(
                f"Duplicate value in foreign key: {value}, in {right_table.name} {right_column.name}"
            )
        right_idx_mapping[value] = i

    left_column = left.find_expanded_column(left_column_name)
    values = []

    has_null = False

    for value in left_column.values:
        if value is None:
            values.append(None)
            has_null = True
        else:
            if value not in right_idx_mapping:
                values.append(None)
                has_null = True
            else:
                values.append(right_idx_mapping[value])

    for column in right_table.expanded_columns:
        new_column = column.copy()
        new_column.name = right_prefix + column.name
        if has_null:
            new_column.schema = new_column.schema.copy()
            new_column.schema.can_null = True
        new_column.values = []
        for v in values:
            if v is None:
                new_column.values.append(None)
            else:
                new_column.values.append(column.values[v])

        left.add_expanded_column(new_column)

    # update with binlog
    if binlog is not None:
        primary_key_columns_binlog = binlog.columns.primary_keys
        all_columns_binlog = binlog.columns.all_columns

        time_parsed_column = left.find_expanded_column("log_data.time_parsed")

        primary_columns = []
        for column_name in primary_key_columns_binlog:
            primary_columns.append(
                left.find_expanded_column(right_prefix + column_name)
            )
        all_columns = []
        for column_name in all_columns_binlog:
            all_columns.append(left.find_expanded_column(right_prefix + column_name))

        for idx_left in range(left.value_length()):
            time_parsed = time_parsed_column.values[idx_left]
            primary_tuple = tuple(column.values[idx_left] for column in primary_columns)

            if primary_tuple in binlog.binlog_items:
                changes = binlog.binlog_items[primary_tuple]
                tuple_before = changes.get_before_time(time_parsed)

                if isinstance(tuple_before, str):
                    continue

                if tuple_before is None:
                    for column in all_columns:
                        column.values[idx_left] = None
                else:
                    for column, value in zip(all_columns, tuple_before):
                        column.values[idx_left] = value

    if left.join_info is None:
        left.join_info = []
    left.join_info.append((relation, right_prefix))

    return left


def do_join_nearest_related_before(
    left: DbTable,
    left_column_name: str,
    db: DbDump,
    relation: ColumnRelation,
    right_prefix: str,
) -> DbTable:
    right_table = db.find_table(relation.right_table)

    left_idx = -1
    right_idx = 0

    left_length = left.value_length()
    right_length = right_table.value_length()

    left_time_values: list[float] = left.find_expanded_column(
        "log_data.time_parsed"
    ).values  # type: ignore
    right_time_values: list[float] = right_table.find_expanded_column(
        "log_data.time_parsed"
    ).values  # type: ignore

    left_header_values: list[dict[str, str]] = left.find_expanded_column(
        "log_data.headers"
    ).values  # type: ignore
    right_header_values: list[dict[str, str]] = right_table.find_expanded_column(
        "log_data.headers"
    ).values  # type: ignore

    values = []
    has_null = False

    while True:
        left_idx += 1
        if left_idx >= left_length:
            break

        left_time = left_time_values[left_idx]
        left_header = left_header_values[left_idx]
        left_auth = left_header.get("authorization")

        right_idx_to_take = None

        while True:
            if right_idx >= right_length:
                break
            right_time = right_time_values[right_idx]
            if right_time > left_time:
                break
            right_idx += 1

        for right_to_check in range(right_idx - 1, max(-1, right_idx - 1 - 20), -1):
            right_time = right_time_values[right_to_check]
            if left_time - right_time > 600:
                break
            right_header = right_header_values[right_to_check]
            right_auth = right_header.get("authorization")
            if left_auth == right_auth:
                right_idx_to_take = right_to_check
                break

        if right_idx_to_take is None:
            has_null = True
        values.append(right_idx_to_take)

    for column in right_table.expanded_columns:
        new_column = column.copy()
        new_column.name = right_prefix + column.name
        if has_null:
            new_column.schema = new_column.schema.copy()
            new_column.schema.can_null = True
        new_column.values = []
        for v in values:
            if v is None:
                new_column.values.append(None)
            else:
                new_column.values.append(column.values[v])

        left.add_expanded_column(new_column)

    if left.join_info is None:
        left.join_info = []
    left.join_info.append((relation, right_prefix))

    return left
