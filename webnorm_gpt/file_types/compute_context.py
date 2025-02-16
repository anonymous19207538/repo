import base64
import json

from ..schema_induction.db import DbTable
from .binlog_file import DbTableBinlog
from .log_file import LogItem


def b64decode_ignore_padding(s):
    s_pad = s + "=" * (4 - len(s) % 4)
    return base64.b64decode(s_pad)


def compute_context_train_ticket_inner(auth_header: str | None) -> dict:
    if auth_header is None:
        return {"user_id": "", "is_user": "false", "is_admin": "false"}

    auth_starts = "Bearer "
    assert auth_header.startswith(auth_starts)
    token = auth_header[len(auth_starts) :]

    token_parts = token.split(".")
    assert len(token_parts) == 3

    dec = b64decode_ignore_padding(token_parts[1])
    dec = json.loads(dec)

    result = {}

    assert isinstance(dec["id"], str)
    result["user_id"] = dec["id"]

    assert isinstance(dec["roles"], list)
    result["is_user"] = "ROLE_USER" in dec["roles"]
    result["is_admin"] = "ROLE_ADMIN" in dec["roles"]

    return result


def compute_context_train_ticket(auth_header: str | None) -> dict:
    try:
        return compute_context_train_ticket_inner(auth_header)
    except Exception as e:
        return {"user_id": "", "is_user": "false", "is_admin": "false"}


def compute_context_train_ticket_log(log_item: LogItem):
    headers = log_item.headers
    if "Authorization" in headers:
        auth_header = headers["Authorization"]
    elif "authorization" in headers:
        auth_header = headers["authorization"]
    else:
        auth_header = None

    context = compute_context_train_ticket(auth_header)
    if "env" not in log_item.content:
        log_item.content["env"] = {}
    log_item.content["env"].update(context)


def compute_context_nicefish_log_wrapper(table: DbTable, table_binlogs: DbTableBinlog):
    table_key_mapping = {}
    session_id_column = table.find_column("session_id")
    for idx, value in enumerate(session_id_column.values):
        assert value not in table_key_mapping
        table_key_mapping[value] = idx

    def func(log_item: LogItem):
        compute_context_nicefish_log_inner(
            log_item, table, table_binlogs, table_key_mapping
        )

    return func


def compute_context_nicefish_log_inner(
    log_item: LogItem,
    table: DbTable,
    table_binlogs: DbTableBinlog,
    table_key_mapping: dict[str, int],
):
    return