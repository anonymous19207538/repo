import gzip
import json
import pickle
import typing
from dataclasses import dataclass
from datetime import datetime

import zstandard
from mitmproxy import http, io
from mitmproxy.http import HTTPFlow
from tqdm import tqdm

from .. import logger
from ..gen_inv.base import Field, RelatedFields
from .proj_desc_file import ProjDescFile


class LogItem:
    content: dict

    def __init__(self, content: dict = None):
        if content is None:
            content = {}
        self.content = content
        if self.content["api"] == "execute.service.ExecuteServiceImpl.ticketCollect":
            self.content["api"] = "execute.serivce.ExecuteServiceImpl.ticketCollect"
        if self.content["api"] == "execute.service.ExecuteServiceImpl.ticketExecute":
            self.content["api"] = "execute.serivce.ExecuteServiceImpl.ticketExecute"

    @property
    def time(self) -> str:
        return self.content.get("time", "")

    @property
    def response_time(self) -> str:
        return self.content.get("response_time", "")

    @property
    def api(self) -> str:
        return self.content.get("api", "")

    @property
    def arguments(self) -> dict[str, typing.Any]:
        return self.content.get("arguments", {})

    @property
    def response(self) -> dict[str, typing.Any]:
        return self.content.get("response", {})

    @property
    def headers(self) -> dict[str, str]:
        return self.content.get("headers", {})

    @property
    def env(self) -> dict[str, typing.Any]:
        return self.content.get("env", {})

    def to_check_dict(
        self,
        fields: typing.Optional[list[Field]],
    ) -> dict:
        result = dict()
        if fields is None:
            for field_name, field_value in self.arguments.items():
                result[f"argument.{field_name}"] = field_value
            for field_name, field_value in self.response.items():
                if field_name == "__str__":
                    result["response"] = field_value
                else:
                    result[f"response.{field_name}"] = field_value
            for field_name, field_value in self.headers.items():
                result[f"header.{field_name}"] = field_value
            for field_name, field_value in self.env.items():
                result[f"env.{field_name}"] = field_value
        else:
            for field in fields:
                if field.category == "argument":
                    result_dict = self.arguments
                elif field.category == "response":
                    result_dict = self.response
                elif field.category == "header":
                    result_dict = self.headers
                elif field.category == "env":
                    result_dict = self.env
                else:
                    raise ValueError(f"Invalid category: {field.category}")
                result_field_name = f"{field.category}.{field.name}"
                result[result_field_name] = result_dict.get(field.name, None)
        return result

    def to_prompt_string(
        self,
        include_arguments: bool = False,
        include_response: bool = False,
        include_headers: bool = False,
        include_env: bool = False,
    ) -> str:
        result = dict()
        if include_arguments:
            for field_name, field_value in self.arguments.items():
                result[f"argument.{field_name}"] = field_value
        if include_response:
            if isinstance(self.response, dict):
                for field_name, field_value in self.response.items():
                    result[f"response.{field_name}"] = field_value
            else:
                result["response"] = str(self.response)
        if include_headers:
            for field_name, field_value in self.headers.items():
                result[f"header.{field_name}"] = field_value
        if include_env:
            for field_name, field_value in self.env.items():
                result[f"env.{field_name}"] = field_value

        result = json.dumps(result)

        result = f"{result}"

        return result

    def to_execute_json(
        self,
        related_fields: RelatedFields,
    ) -> dict[str, typing.Any]:
        result = {}
        header_names = ["httpHeaders", "httpHeader", "headers", "header"]

        if related_fields.include_arguments:
            arguments = self.content.get("arguments", {})
            has_header = False
            for header_name in header_names:
                if header_name in arguments:
                    has_header = True
                    break
            if has_header:
                arguments = arguments.copy()
                for header_name in header_names:
                    if header_name in arguments:
                        arguments["headers"] = arguments[header_name]
                        del arguments[header_name]
            result["arguments"] = arguments
        if related_fields.include_response:
            result["response"] = self.content.get("response", {})
        if related_fields.include_headers:
            result["headers"] = self.content.get("headers", {})
        if related_fields.include_env:
            env = self.content.get("env", {})
            env = env.copy()
            if "is_user" in env:
                del env["is_user"]
            if "is_admin" in env:
                del env["is_admin"]
            result["env"] = self.content.get("env", {})
        if related_fields.include_db_info:
            result["db_info"] = self.content.get("related_db_tables", {})
        if related_fields.include_related_log:
            related_logs = self.content.get("related_event_logs", {})
            related_logs_to_add = {}
            for k, v in related_logs.items():
                related_event = {}
                if v is None:
                    related_logs_to_add[k] = None
                    continue
                if related_fields.related_include_arguments:
                    arguments = v.get("arguments", {})
                    has_header = False
                    for header_name in header_names:
                        if header_name in arguments:
                            has_header = True
                            break
                    if has_header:
                        arguments = arguments.copy()
                        for header_name in header_names:
                            if header_name in arguments:
                                arguments["headers"] = arguments[header_name]
                                del arguments[header_name]
                    related_event["arguments"] = arguments
                if related_fields.related_include_response:
                    related_event["response"] = v.get("response", {})
                if related_fields.related_include_headers:
                    related_event["headers"] = v.get("headers", {})
                if related_fields.related_include_env:
                    related_event["env"] = v.get("env", {})
                related_logs_to_add[k] = related_event
            result["related_events"] = related_logs_to_add

        return result

    def serialize_obj(self, obj):
        if isinstance(obj, dict):
            return {k: self.serialize_obj(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.serialize_obj(v) for v in obj]
        try:
            # Try to serialize the object normally (if it's a built-in type)
            json.dumps(obj)
            return obj
        except (TypeError, ValueError):
            # If it raises an error, convert the object to a string
            return str(obj)

    def __str__(self):
        serialized_content = self.serialize_obj(self.content)
        return json.dumps(serialized_content)

    def parse_time(self):
        return datetime.strptime(self.time, "%Y-%m-%d %H:%M:%S.%f").timestamp()

    def parse_response_time(self):
        if self.response_time == "":
            return None
        return datetime.strptime(self.response_time, "%Y-%m-%d %H:%M:%S.%f").timestamp()


# A log file should be a jsonl file.
# Each line should be a json object with the following keys:
# - time: str (e.g. 2024-05-02 17:34:56.705)
# - response_time: str
# - api: str (the full method name e.g. "extra.shop.ShopImpl.get" or full api name e.g. "/api/v1/shop/get")
# - arguments: dict[str, Any] (the arguments passed to the method)
# - response: Any (the response of the method)
# - headers: dict[str, str] (the headers of the request)
# - env: dict[str, Any] (the environment variables)
# - queries: dict[str, list[Any]] (the queries of the request)
# The json object can have other keys as extra information.
class LogFile:
    log_items: list[LogItem]

    def __init__(self):
        self.log_items = []

    def __iter__(self):
        return iter(self.log_items)

    def load_from_iterator(self, it: typing.Iterator[str]):
        for line in it:
            j = json.loads(line)
            self.log_items.append(LogItem(j))

    def load_from_file_path(self, file_path: str):
        if file_path.endswith(".gz"):
            open_func = gzip.open
        elif file_path.endswith(".zst"):
            open_func = zstandard.open
        else:
            open_func = open
        with open_func(file_path, "r") as f:
            self.load_from_iterator(f)

    def save_to_path(self, file_path: str):
        if file_path.endswith(".gz"):
            open_func = gzip.open
        elif file_path.endswith(".zst"):
            open_func = zstandard.open
        else:
            open_func = open
        with open_func(file_path, "w") as f:
            for item in self.log_items:
                f.write(json.dumps(item.content) + "\n")

    def filter_train(self) -> "LogFile":
        result = LogFile()
        for item in self.log_items:
            if item.content.get("split", "") == "train":
                result.log_items.append(item)
        return result

    def filter_test(self) -> "LogFile":
        result = LogFile()
        for item in self.log_items:
            if item.content.get("split", "") == "test":
                result.log_items.append(item)
        return result

    def __getitem__(self, item):
        return self.log_items[item]


def time_stamp_to_datetime_str(timestamp: int | float) -> str:
    dt = datetime.fromtimestamp(timestamp)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")


def load_from_log_receiver_file(file_path: str, proj_desc_file: ProjDescFile):
    logs = []
    pending_items = {}

    with zstandard.open(file_path, "r") as f:
        lines = f.readlines()
    for line in tqdm(lines):
        line = line.strip()
        if not line:
            continue
        line = json.loads(line)
        api = line["methodName"]
        is_enter = line["isEnter"]

        cur_time = line["time"]
        cur_time = time_stamp_to_datetime_str(cur_time)

        if is_enter:
            arguments = line["arguments"]
            arguments_des = []
            for arg in arguments:
                try:
                    a = json.loads(arg)
                except json.JSONDecodeError:
                    logger.warning("Failed to decode json: %s", arg)
                    a = arg
                arguments_des.append(a)
            arguments = arguments_des

            headers = line["headers"]
            real_headers = {}
            envs = {}
            queries = {}

            for k, v in headers.items():
                if k.startswith("__env__"):
                    envs[k[7:]] = v
                elif k.startswith("__query__"):
                    queries[k[9:]] = v
                else:
                    real_headers[k] = v

            api_desc = proj_desc_file.api_map[api]
            argument_names = api_desc.argument_names
            arguments_dict = dict(zip(argument_names, arguments))

            controller_path = api_desc.extra.get("controller_path", None)
            url_path = api_desc.extra.get("url_path", None)

            log_dict = {}
            log_dict["time"] = cur_time
            log_dict["api"] = api
            log_dict["arguments"] = arguments_dict
            log_dict["headers"] = real_headers
            log_dict["env"] = envs
            log_dict["queries"] = queries

            log_dict["controller_path"] = controller_path
            log_dict["url_path"] = url_path
            log_dict["api_name"] = api

            log_item = LogItem(log_dict)
            logs.append(log_item)
            if api not in pending_items:
                pending_items[api] = []
            pending_items[api].append(log_dict)
        else:
            if api not in pending_items or len(pending_items[api]) == 0:
                logger.warning("Warning: no enter for exit: %s. API: %s", line, api)
                continue

            response_time = line["time"]
            response_time = time_stamp_to_datetime_str(response_time)

            has_error = line["hasError"]
            if has_error:
                response = None
                throwable = line["throwable"]
                if throwable.strip():
                    throwable = json.loads(throwable)
                else:
                    throwable = None
            else:
                response = line["returnObj"]
                if response.strip():
                    response = json.loads(response)
                else:
                    response = None
                throwable = None

            log_item = pending_items[api].pop()
            log_item["response_time"] = response_time
            log_item["response"] = response
            log_item["throwable"] = throwable
            log_item["has_error"] = has_error

    for k, v in pending_items.items():
        for item in v:
            logger.warning("Warning: no exit for enter: %s, %s", k, item)

    log_items_filtered = []
    for log_item in logs:
        if "response_time" not in log_item.content:
            continue
        log_items_filtered.append(log_item)

    log_items_filtered.sort(key=lambda log: log.time)

    logfile = LogFile()
    logfile.log_items = log_items_filtered

    return logfile


def load_from_mitm_file(
    file_path: str, proj_desc_file: ProjDescFile, include_non_api=False
):
    with open(file_path, "rb") as fin:
        freader = io.FlowReader(fin)
        logs = []

        api_mapping = {}

        for api in proj_desc_file.apis:
            path = api.extra["url_path"]
            api_name = api.name

            if path.startswith("/"):
                path = path[1:]
            path_content = path.split("/")
            final_path = [""]
            has_param = False
            for path_item in path_content:
                if path_item.startswith("{") and path_item.endswith("}"):
                    if not has_param:
                        has_param = True
                else:
                    if has_param:
                        # raise ValueError(f"Invalid path: {path}")
                        continue
                    final_path.append(path_item)
            final_path = "/".join(final_path)
            api_mapping[final_path] = api_name

        for f in tqdm(freader.stream()):
            if not isinstance(f, HTTPFlow):
                continue
            log_dict = {}

            path = f.request.path
            current_api_name = None
            current_api_path_len = 0
            api_name = None

            api_desc = None

            for api in api_mapping:
                if path.startswith(api):
                    if len(api) > current_api_path_len:
                        current_api_name = api
                        current_api_path_len = len(api)
                        api_name = api_mapping[api]
                        api_desc = proj_desc_file.api_map[api_name]

            if current_api_name is None:
                if include_non_api:
                    log_dict = {}
                    cur_time = time_stamp_to_datetime_str(f.request.timestamp_end)
                    log_dict["time"] = cur_time
                    log_dict["path"] = path
                    log_dict["api"] = None
                    log_item = LogItem(log_dict)
                    logs.append(log_item)
                continue

            if "?" in path:
                path_actual = path[: path.index("?")]
            else:
                path_actual = path

            path_params = path_actual[current_api_path_len:]
            if path_params.startswith("/"):
                path_params = path_params[1:]
            if path_params.endswith("/"):
                path_params = path_params[:-1]

            if len(path_params) > 0:
                path_params = path_params.split("/")
            else:
                path_params = []

            cur_time = time_stamp_to_datetime_str(f.request.timestamp_end)

            argument_dict = {}

            body = f.request.content
            if body is not None and len(body) > 0:
                try:
                    body = json.loads(body)
                    if isinstance(body, dict):
                        argument_dict.update(body)
                    else:
                        argument_dict["body"] = body
                except json.JSONDecodeError:
                    body = body.decode("utf-8")
                    argument_dict["body"] = body

            for i, param in enumerate(path_params):
                argument_dict[f"path_param_{i}"] = param

            controller_path = api_desc.extra.get("controller_path", None)
            url_path = api_desc.extra.get("url_path", None)

            req_headers = f.request.headers
            headers = {}
            for k, v in req_headers.items():
                headers[str(k)] = str(v)

            log_dict = {}
            log_dict["time"] = cur_time
            log_dict["api"] = current_api_name
            log_dict["arguments"] = argument_dict
            log_dict["headers"] = headers
            log_dict["env"] = {}
            log_dict["queries"] = {}
            log_dict["api_name"] = api_name

            log_dict["controller_path"] = controller_path
            log_dict["url_path"] = url_path

            if f.response is None:
                logger.warning("Warning: no response for request: %s", f.request)
                continue

            response_time = time_stamp_to_datetime_str(f.response.timestamp_end)
            response_code = f.response.status_code
            has_error = response_code >= 400
            body = f.response.content
            if body is not None and len(body) > 0:
                try:
                    body = json.loads(body.decode("utf-8", errors="ignore"))
                except json.JSONDecodeError:
                    try:
                        body = body.decode("utf-8")
                    except UnicodeDecodeError:
                        body = None

            log_dict["response_time"] = response_time
            log_dict["response"] = body
            log_dict["has_error"] = has_error
            log_dict["throwable"] = None

            log_item = LogItem(log_dict)
            logs.append(log_item)

    logs.sort(key=lambda log: log.time)

    logfile = LogFile()
    logfile.log_items = logs

    return logfile


START_ATTACK_MARKER = "/attack_start_marker?uuid="
END_ATTACK_MARKER = "/attack_end_marker?uuid="


def filter_attack(logs: LogFile) -> bool:
    attack_names = set()
    attack_idxes = set()
    attack_ints = set()
    attack_files = set()

    for log in logs:
        headers = log.headers

        if "x-att-name" in headers:
            attack_names.add(headers["x-att-name"])
        if "x-att-idx" in headers:
            attack_idxes.add(headers["x-att-idx"])
        if "x-att-int" in headers:
            attack_ints.add(headers["x-att-int"])
        if "x-att-file" in headers:
            attack_files.add(headers["x-att-file"])
        if "X-Att-Name" in headers:
            attack_names.add(headers["X-Att-Name"])
        if "X-Att-Idx" in headers:
            attack_idxes.add(headers["X-Att-Idx"])
        if "X-Att-Int" in headers:
            attack_ints.add(headers["X-Att-Int"])
        if "X-Att-File" in headers:
            attack_files.add(headers["X-Att-File"])

    has_error = False

    if len(attack_names) != 1:
        has_error = True
    if len(attack_idxes) != 1:
        has_error = True
    if len(attack_ints) != 1:
        has_error = True

    if has_error:
        attack_names = sorted(attack_names)
        attack_idxes = sorted(attack_idxes)
        attack_ints = sorted(attack_ints)

        logger.warning(
            "Invalid attack with attack_names: %s, attack_idxes: %s, attack_ints: %s",
            attack_names,
            attack_idxes,
            attack_ints,
        )

    if len(attack_files) > 0:
        attack_file = next(iter(attack_files))
        if attack_file in IGNORE_ATTACK_FILES:
            return False

    return not has_error


IGNORE_ATTACK_FILES = {
    "attack_detail_likecollect.py",
    "attack_profile_information_others.py",
    "attack_profile_post_others.py",
    "attack_profile_saved_others.py",
    "attack_list_wrong_food.py",
    "attack_order_consign_wrong_date.py",
}


def split_attacks(mitm_log: LogFile, instru_log: LogFile) -> list[LogFile]:
    start_markers = {}
    end_markers = {}

    for log in mitm_log.log_items:
        path = None
        if "path" in log.content:
            path = log.content["path"]
        elif "url_path" in log.content:
            path = log.content["url_path"]
        if path is not None:
            if path.startswith(START_ATTACK_MARKER):
                attack_uuid = path[len(START_ATTACK_MARKER) :]
                start_markers[attack_uuid] = log.parse_time()
            if path.startswith(END_ATTACK_MARKER):
                attack_uuid = path[len(END_ATTACK_MARKER) :]
                end_markers[attack_uuid] = log.parse_time()

    attack_time_pairs = []

    for u in start_markers:
        if u not in end_markers:
            continue

        start_time = start_markers[u]
        end_time = end_markers[u]

        attack_time_pairs.append((start_time, end_time))

    attack_time_pairs.sort()

    attacks = []

    for start_time, end_time in attack_time_pairs:
        new_log_file = LogFile()
        for log in instru_log.log_items:
            if start_time <= log.parse_time() <= end_time:
                new_log_file.log_items.append(log)
        if len(new_log_file.log_items) > 0 and filter_attack(new_log_file):
            attacks.append(new_log_file)
        else:
            logger.warning("Warning: empty attack: %s, %s", start_time, end_time)

    return attacks

