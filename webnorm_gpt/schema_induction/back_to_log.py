from copy import deepcopy

from ..file_types.log_file import LogFile, LogItem
from .db import DbTable, ExpandedColumn


def db_table_to_log(table: DbTable) -> LogFile:
    original_log_data: None | ExpandedColumn = None
    joined_log_data: dict[str, ExpandedColumn] = {}
    joined_sql_data: dict[str, dict[str, ExpandedColumn]] = {}

    for column in table.expanded_columns:
        # only keep original columns, ignore expanded columns
        if len(column.expand_ops) != 0:
            continue

        colunm_name = column.name
        if table.join_info is None:
            table.join_info = []

        for relationship, prefix in table.join_info:
            if colunm_name.startswith(prefix):
                original_column_name = colunm_name[len(prefix) :]
                original_table_name = relationship.back_name
                table_name = relationship.right_table
                if table_name.startswith("db::"):
                    # original_table_name = table_name[4:]
                    if original_table_name not in joined_sql_data:
                        joined_sql_data[original_table_name] = {}
                    if original_column_name in joined_sql_data[original_table_name]:
                        raise ValueError(
                            f"Duplicate column name {original_column_name}"
                        )
                    joined_sql_data[original_table_name][original_column_name] = column
                    break
                elif table_name.startswith("log::"):
                    # original_table_name = table_name[5:]
                    if original_column_name == "log_data":
                        if original_table_name in joined_log_data:
                            raise ValueError(
                                f"Duplicate table name {original_table_name}"
                            )
                        joined_log_data[original_table_name] = column
                        break
                    else:
                        raise ValueError(f"Unknown column name {original_column_name}")
                else:
                    raise ValueError(f"Unknown table name {table_name}")
        else:
            if colunm_name == "log_data":
                if original_log_data is not None:
                    raise ValueError("Duplicate log_data")
                original_log_data = column
            else:
                raise ValueError(f"Unknown column name {colunm_name}")

    if original_log_data is None:
        raise ValueError("log_data not found")

    result_logs = []
    for i in range(len(original_log_data.values)):
        original_log = original_log_data.values[i]
        original_log = deepcopy(original_log)
        assert isinstance(original_log, dict)

        if len(joined_sql_data) > 0:
            if "related_db_tables" not in original_log:
                original_log["related_db_tables"] = {}
            db_data_dict = original_log["related_db_tables"]
            assert isinstance(db_data_dict, dict)
            for table_name, columns in joined_sql_data.items():
                has_non_null = False
                for column in columns.values():
                    if column.values[i] is not None:
                        has_non_null = True
                        break
                if has_non_null or table_name in db_data_dict:
                    if table_name not in db_data_dict:
                        db_data_dict[table_name] = {}
                    table_dict = db_data_dict[table_name]
                    assert isinstance(table_dict, dict)
                    for column_name, column in columns.items():
                        if column_name in table_dict:
                            raise ValueError(f"Duplicate column name {column_name}")
                        table_dict[column_name] = column.values[i]
                else:
                    db_data_dict[table_name] = None

        if len(joined_log_data) > 0:
            if "related_event_logs" not in original_log:
                original_log["related_event_logs"] = {}
            log_data_dict = original_log["related_event_logs"]
            assert isinstance(log_data_dict, dict)
            for table_name, column in joined_log_data.items():
                if table_name in log_data_dict:
                    raise ValueError(f"Duplicate table name {table_name}")
                log_data_dict[table_name] = column.values[i]

        log_item = LogItem(original_log)
        result_logs.append(log_item)

    log_file = LogFile()
    log_file.log_items = result_logs

    return log_file
