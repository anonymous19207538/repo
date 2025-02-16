import pickle
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

import zstandard

from .. import logger


@dataclass
class DbTableColumns:
    primary_keys: list[str]
    all_columns: list[str]

    def __eq__(self, other):
        if not isinstance(other, DbTableColumns):
            return False
        return (
            self.primary_keys == other.primary_keys
            and self.all_columns == other.all_columns
        )


DB_TIMESTAMP_EARLIEST = 0


@dataclass
class DbColumnChanges:
    changes: list[tuple[int, tuple]]

    def find_at(self, timestamp: float) -> tuple[int, int] | None:
        if len(self.changes) == 0:
            raise ValueError("No changes")

        if timestamp > self.changes[-1][0]:
            return None

        timestamp_min = int(timestamp - 1)
        timestamp_max = int(timestamp + 2)

        left, right = 0, len(self.changes) - 1
        while left < right:
            mid = (left + right) // 2
            if self.changes[mid][0] < timestamp_min:
                left = mid + 1
            else:
                right = mid
        idx_min = left

        left, right = 0, len(self.changes) - 1
        while left < right:
            mid = (left + right + 1) // 2
            if self.changes[mid][0] > timestamp_max:
                right = mid - 1
            else:
                left = mid
        idx_max = right

        return (idx_min, idx_max)

    def get_before_time(self, timestamp: float) -> tuple | None | Literal["no_record"]:
        found_res = self.find_at(timestamp)
        if found_res is None:
            return "no_record"
        idx_min, idx_max = found_res
        if idx_max - idx_min > 0:
            logger.warning("Multiple changes at the same time %s", timestamp)
            # raise ValueError("Multiple changes at the same time")
        if idx_min == 0:
            logger.warning("No change before time %s", timestamp)
            return self.changes[0][1]
        return self.changes[idx_min - 1][1]


@dataclass
class DbTableBinlog:
    columns: DbTableColumns
    binlog_items: dict[tuple, DbColumnChanges]


def dump_all_table_keys(conn, db_name: str):
    cursor = conn.cursor()

    cursor.execute(
        "SELECT `TABLE_NAME`, `COLUMN_NAME` FROM `information_schema`.`COLUMNS` WHERE `TABLE_SCHEMA` = %s",
        (db_name,),
    )
    rows = cursor.fetchall()

    all_columns = defaultdict(list)

    for table_name, column_name in rows:
        all_columns[table_name].append(column_name)

    cursor.execute(
        "SELECT `TABLE_NAME`, `COLUMN_NAME` FROM `information_schema`.`KEY_COLUMN_USAGE` WHERE `TABLE_SCHEMA` = %s AND `CONSTRAINT_NAME` = 'PRIMARY'",
        (db_name,),
    )
    all_primary_columns = defaultdict(list)
    rows = cursor.fetchall()
    for table_name, column_name in rows:
        all_primary_columns[table_name].append(column_name)

    all_info = {}

    for table_name in all_columns:
        all_cols = all_columns[table_name]
        primary_cols = all_primary_columns.get(table_name, [])

        assert len(all_cols) == len(set(all_cols))
        assert len(primary_cols) == len(set(primary_cols))

        for k in primary_cols:
            if k not in all_cols:
                raise ValueError(f"Primary key {k} not found in table {table_name}")

        if len(primary_cols) == 0:
            primary_cols = None

        all_info[table_name] = (primary_cols, all_cols)

    return all_info


def timestamp_to_str(timestamp: int) -> str:
    dt = datetime.fromtimestamp(timestamp)
    return dt.isoformat()


def process_binlog_file(
    all_info,
    binlog_file_path: str,
    focus_schema_name: str,
    db_merge_info: list[tuple[str, str]] = [],
):
    from_to_dict = {k: v for k, v in db_merge_info}

    with zstandard.open(binlog_file_path, "rb") as f:
        binlog_items: list = pickle.load(f)

    focus_types = {"insert", "update", "delete"}

    binlog_to_tables = defaultdict(list)
    for item in binlog_items:
        ty = item["type"]
        if ty not in focus_types:
            continue

        schema_name = item["schema"]
        assert isinstance(schema_name, str)
        if schema_name != focus_schema_name:
            continue

        table_name = item["table"]
        if table_name in from_to_dict:
            table_name = from_to_dict[table_name]
        assert isinstance(table_name, str)
        binlog_to_tables[table_name].append(item)

    db_binlogs = {}

    for table_name, items in binlog_to_tables.items():
        if table_name not in all_info:
            raise ValueError(f"Table {table_name} not found in all_info")

        primary_keys, all_cols = all_info[table_name]
        if primary_keys is None or len(primary_keys) != 1:
            continue

        changelist = defaultdict(list)

        for item in items:
            ty = item["type"]
            rows = item["rows"]

            time_change = item["timestamp"]

            for row in rows:
                if ty == "update":
                    olddata = row["before_values"]
                    newdata = row["after_values"]

                    assert sorted(olddata.keys()) == sorted(newdata.keys())

                    if len(olddata) != len(all_cols):
                        raise ValueError(
                            f"Column count mismatch in table {table_name}: {olddata} vs {all_cols}"
                        )
                    if len(olddata) != len(newdata):
                        raise ValueError(
                            f"Column count mismatch in table {table_name}: {newdata} vs {all_cols}"
                        )

                    for key in olddata:
                        old = olddata[key]
                        new = newdata[key]
                        if key not in all_cols:
                            raise ValueError(
                                f"Column {key} not found in table {table_name}"
                            )
                        if old != new:
                            if key in primary_keys:
                                raise ValueError(
                                    f"Primary key {key} changed in table {table_name}"
                                )

                    primary_key_tuple = tuple(olddata[key] for key in primary_keys)
                    all_key_old_tuple = tuple(olddata[key] for key in all_cols)
                    all_key_new_tuple = tuple(newdata[key] for key in all_cols)
                elif ty == "insert":
                    data = row["values"]
                    if len(data) != len(all_cols):
                        raise ValueError(
                            f"Column count mismatch in table {table_name}: {data} vs {all_cols}"
                        )
                    for key in data:
                        if key not in all_cols:
                            raise ValueError(
                                f"Column {key} not found in table {table_name}"
                            )
                    primary_key_tuple = tuple(data[key] for key in primary_keys)
                    all_key_old_tuple = None
                    all_key_new_tuple = tuple(data[key] for key in all_cols)
                elif ty == "delete":
                    data = row["values"]
                    if len(data) != len(all_cols):
                        raise ValueError(
                            f"Column count mismatch in table {table_name}: {data} vs {all_cols}"
                        )
                    for key in data:
                        if key not in all_cols:
                            raise ValueError(
                                f"Column {key} not found in table {table_name}"
                            )
                    primary_key_tuple = tuple(data[key] for key in primary_keys)
                    all_key_old_tuple = tuple(data[key] for key in all_cols)
                    all_key_new_tuple = None
                else:
                    raise ValueError(f"Unknown type {ty}")

                if all_key_old_tuple is not None:
                    for val in all_key_old_tuple:
                        assert (
                            val == None
                            or isinstance(val, str)
                            or isinstance(val, int)
                            or isinstance(val, float)
                            or isinstance(val, bool)
                            or isinstance(val, bytes)
                            or isinstance(val, datetime)
                        )
                t = (time_change, all_key_old_tuple, all_key_new_tuple)
                changelist[primary_key_tuple].append(t)

        bin_log_change_list = {}
        for primary_key_tuple, changes in changelist.items():
            last_time_change = None
            last_value = None
            for time_change, old_value, new_value in changes:
                if last_time_change is not None:
                    if time_change < last_time_change:
                        raise ValueError(f"Time change order mismatch")
                    if old_value != last_value:
                        raise ValueError(
                            f"Old value mismatch: {old_value} vs {last_value} table {table_name} primary key {primary_key_tuple} at time {timestamp_to_str(time_change)}"
                        )
                last_time_change = time_change
                last_value = new_value

            changes_list = []
            _, old_value, _ = changes[0]
            changes_list.append((DB_TIMESTAMP_EARLIEST, old_value))
            for time_change, _, new_value in changes:
                changes_list.append((time_change, new_value))

            column_changes = DbColumnChanges(changes=changes_list)
            bin_log_change_list[primary_key_tuple] = column_changes

        db_columns = DbTableColumns(primary_keys=primary_keys, all_columns=all_cols)
        db_table_binlog = DbTableBinlog(
            columns=db_columns, binlog_items=bin_log_change_list
        )

        db_binlogs[table_name] = db_table_binlog

    return db_binlogs
