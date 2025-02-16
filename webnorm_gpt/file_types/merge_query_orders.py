from copy import deepcopy
from datetime import datetime
from typing import Callable

from ..schema_induction.db import DbDump, DbSchema
from .log_file import LogFile, LogItem
from .binlog_file import DbTableBinlog


def same_dict_content(d1: dict, d2: dict) -> bool:
    if d1.keys() != d2.keys():
        return False
    for k in d1.keys():
        if d1[k] != d2[k]:
            return False
    return True


class MergeQueryInfo:
    api_name1: str
    api_name2: str
    new_session_api_name: str

    def log_append_func(self, src: LogItem, tgt: LogItem):
        raise NotImplementedError

    def log_append_func_basic(self, src: LogItem, tgt: LogItem):
        if "origin_response" not in src.content:
            src.content["origin_response"] = deepcopy(src.response)
        if "origin_response" not in tgt.content:
            tgt.content["origin_response"] = deepcopy(tgt.response)

    def related_check_func(self, log1: LogItem, log2: LogItem) -> bool:
        raise NotImplementedError

    def related_check_func_basic(self, log1: LogItem, log2: LogItem) -> bool:
        time1 = get_req_time(log1)
        time2 = get_req_time(log2)
        if abs((time1 - time2).total_seconds()) > 60:
            return False
        if get_authencation_header(log1) != get_authencation_header(log2):
            return False
        return True


def get_authencation_header(log: LogItem) -> str | None:
    auth_header_name_1 = "Authorization"
    auth_header_name_2 = "authorization"

    if auth_header_name_1 in log.headers:
        return log.headers[auth_header_name_1]
    if auth_header_name_2 in log.headers:
        return log.headers[auth_header_name_2]
    return None


def get_req_time(log: LogItem) -> datetime:
    return datetime.strptime(log.time, "%Y-%m-%d %H:%M:%S.%f")


class MergeQueryInfoTrainTicketOrder(MergeQueryInfo):
    def __init__(self):
        self.api_name1 = "order.service.OrderServiceImpl.queryOrdersForRefresh"
        self.api_name2 = "other.service.OrderOtherServiceImpl.queryOrdersForRefresh"
        self.new_session_api_name = "auth.service.impl.TokenServiceImpl.getToken"

    def log_append_func(self, src: LogItem, tgt: LogItem):
        self.log_append_func_basic(src, tgt)

        if "appended" in tgt.content and tgt.content["appended"]:
            return

        tgt.content["appended"] = True
        if "data" not in tgt.response:
            print(tgt.content)
        tgt.response["data"] += src.content["origin_response"]["data"]

    def related_check_func(self, log1: LogItem, log2: LogItem) -> bool:
        if not self.related_check_func_basic(log1, log2):
            return False

        if "qi" in log1.arguments:
            if not same_dict_content(log1.arguments["qi"], log2.arguments["qi"]):
                return False
        else:
            if not same_dict_content(log1.arguments, log2.arguments):
                return False

        return True


class MergeQueryInfoTrainTicketTravel(MergeQueryInfo):
    def __init__(self):
        self.api_name1 = "travel.service.TravelServiceImpl.queryByBatch"
        self.api_name2 = "travel2.service.TravelServiceImpl.queryByBatch"
        self.new_session_api_name = "auth.service.impl.TokenServiceImpl.getToken"

    def log_append_func(self, src: LogItem, tgt: LogItem):
        self.log_append_func_basic(src, tgt)

        if "appended" in tgt.content and tgt.content["appended"]:
            return

        tgt.content["appended"] = True
        tgt.response["data"] += src.content["origin_response"]["data"]

    def related_check_func(self, log1: LogItem, log2: LogItem) -> bool:
        if not self.related_check_func_basic(log1, log2):
            return False

        if "info" in log1.arguments:
            if not same_dict_content(log1.arguments["info"], log2.arguments["info"]):
                return False
        else:
            if not same_dict_content(log1.arguments, log2.arguments):
                return False

        return True


def merge_query_orders_inner(
    logs: LogFile,
    api_name1: str,
    api_name2: str,
    new_session_api_name: str,
    related_check_func: Callable[[LogItem, LogItem], bool],
    log_append_func: Callable[[LogItem, LogItem], None],
):
    last_api_2: LogItem | None = None
    next_api_2: LogItem | None = None
    pending_api_1: list[LogItem] = []

    count = 0

    def process_pending_api_1():
        nonlocal last_api_2, next_api_2, count

        if last_api_2 is None and next_api_2 is None:
            pending_api_1.clear()
            return

        if last_api_2 is not None:
            last_api_2_time = get_req_time(last_api_2)
        else:
            last_api_2_time = None
        if next_api_2 is not None:
            next_api_2_time = get_req_time(next_api_2)
        else:
            next_api_2_time = None

        for log1 in pending_api_1:
            if last_api_2 is None:
                assert next_api_2 is not None
                if related_check_func(log1, next_api_2):
                    log_append_func(log1, next_api_2)
                    count += 1
            elif next_api_2 is None:
                assert last_api_2 is not None
                if related_check_func(log1, last_api_2):
                    log_append_func(log1, last_api_2)
                    count += 1
            else:
                assert last_api_2_time is not None
                assert next_api_2_time is not None

                log1_time = get_req_time(log1)
                time_diff1 = abs((log1_time - last_api_2_time).total_seconds())
                time_diff2 = abs((log1_time - next_api_2_time).total_seconds())
                if time_diff1 < time_diff2:
                    if related_check_func(log1, last_api_2):
                        log_append_func(log1, last_api_2)
                        count += 1
                    elif related_check_func(log1, next_api_2):
                        log_append_func(log1, next_api_2)
                        count += 1
                else:
                    if related_check_func(log1, next_api_2):
                        log_append_func(log1, next_api_2)
                        count += 1
                    elif related_check_func(log1, last_api_2):
                        log_append_func(log1, last_api_2)
                        count += 1

        pending_api_1.clear()

    for log in logs:
        if "api_name" not in log.content:
            continue
        if log.content["api_name"] == api_name1:
            pending_api_1.append(log)
        elif log.content["api_name"] == api_name2:
            next_api_2 = log
            process_pending_api_1()
            last_api_2 = log
            next_api_2 = None
        elif log.content["api_name"] == new_session_api_name:
            next_api_2 = None
            process_pending_api_1()
            last_api_2 = None
            next_api_2 = None

    next_api_2 = None
    process_pending_api_1()

    print(f"Merge from {api_name1} to {api_name2}: {count}")


def merge_query_orders(
    logs: LogFile,
    info: MergeQueryInfo,
):
    merge_query_orders_inner(
        logs,
        info.api_name1,
        info.api_name2,
        info.new_session_api_name,
        info.related_check_func,
        info.log_append_func,
    )
    merge_query_orders_inner(
        logs,
        info.api_name2,
        info.api_name1,
        info.new_session_api_name,
        info.related_check_func,
        info.log_append_func,
    )


def merge_all_infos(logs: LogFile, infos: list[MergeQueryInfo]):
    for info in infos:
        merge_query_orders(logs, info)


def train_ticket_all_merge_infos():
    return [
        MergeQueryInfoTrainTicketOrder(),
        MergeQueryInfoTrainTicketTravel(),
    ]


def nicefish_all_merge_infos():
    return []


# list of (from_table_name, to_table_name)
DbMergeInfo = list[tuple[str, str]]


def train_ticket_db_merge_info():
    return [("orders_other", "orders"), ("trip2", "trip")]


def merge_db_tables(
    dump_db: DbDump, dump_schema: DbSchema, merge_info: DbMergeInfo
) -> tuple[DbDump, DbSchema]:
    schemas = dump_schema.schemas

    for from_table_name, to_table_name in merge_info:
        from_table = dump_db.find_table(from_table_name)
        to_table = dump_db.find_table(to_table_name)

        for col_s, col_t in zip(from_table.columns, to_table.columns):
            assert col_s.name == col_t.name
            # assert col_s.schema == col_t.schema
            if not col_s.schema.soft_eq(col_t.schema):
                print(f"Schema mismatch:\n From: ")
                print(col_s.schema)
                print(f" To: ")
                print(col_t.schema)
                raise ValueError("Schema mismatch")
            col_t.values.extend(col_s.values)

        tables = [t for t in dump_db.tables if t.name != from_table_name]
        dump_db.tables = tables
        del schemas[from_table_name]

    return dump_db, dump_schema


def merge_db_table_keys(table_keys, merge_info: DbMergeInfo):
    for from_table_name, to_table_name in merge_info:
        from_keys = table_keys[from_table_name]
        to_keys = table_keys[to_table_name]

        assert from_keys == to_keys

        del table_keys[from_table_name]

    return table_keys


def merge_db_binlog(
    binlogs: dict[str, DbTableBinlog], merge_info: DbMergeInfo
) -> dict[str, DbTableBinlog]:
    for from_table_name, to_table_name in merge_info:
        from_binlog = binlogs[from_table_name]
        to_binlog = binlogs[to_table_name]

        assert from_binlog.columns == to_binlog.columns

        for k in from_binlog.binlog_items.keys():
            assert k not in to_binlog.binlog_items
            to_binlog.binlog_items[k] = from_binlog.binlog_items[k]

        del binlogs[from_table_name]

    return binlogs
