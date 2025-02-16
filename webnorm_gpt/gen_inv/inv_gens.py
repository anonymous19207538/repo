import json
import random
import time
import traceback
from abc import ABC, abstractmethod
from copy import deepcopy

import tqdm

from .. import logger
from ..file_types.log_file import LogFile, LogItem
from ..file_types.proj_desc_file import APIDesc, ProjDescFile
from ..gpt_invoker import GPTInvoker
from ..schema_induction.db import DbSchema, DbValue
from ..schema_induction.schema import JsonSchema, JsonSchemaTypes, schema_json_to_string
from .base import APIDomain, Field, Invariant, Predicate, Premise, RelatedFields
from .check_inv import (
    find_nearest_related_event,
    run_py_predicate,
    run_py_predicate_new_json_format,
)
from .prompts import (
    COMMONSENSE_CONSTRAINT_FEEDBACK,
    COMMONSENSE_CONSTRAINT_FEEDBACK_FORMAT_JSON,
    COMMONSENSE_CONSTRAINT_SYSTEM,
    COMMONSENSE_CONSTRAINT_SYSTEM_FORMAT_JSON,
    COMMONSENSE_CONSTRAINT_SYSTEM_FORMAT_JSON_SCHEMA_ONLY,
    COMMONSENSE_CONSTRAINT_USER,
    COMMONSENSE_CONSTRAINT_USER_FORMAT_JSON,
    DATA_CONSTRAINT_FEEDBACK,
    DATA_CONSTRAINT_SYSTEM,
    DATA_CONSTRAINT_USER,
)


class InvGenerator(ABC):
    @abstractmethod
    def generate(self, logs: LogFile) -> list[Invariant]:
        pass


class InvGeneratorCommonSense(InvGenerator):
    def __init__(
        self,
        api: str,
        sample_size: int,
        gpt_invoker: GPTInvoker,
        random_seed=None,
        proj_desc: ProjDescFile = None,
        all_fields=False,
    ):
        self.api = api
        self.random_seed = random_seed
        self.gpt_invoker = gpt_invoker
        self.proj_desc = proj_desc
        if proj_desc is not None:
            self.api_desc = proj_desc.api_map[api]
        else:
            self.api_desc = None
        self.sample_size = sample_size
        self.all_fields = all_fields

    def get_api_desc(self) -> str:
        if self.api_desc is None:
            return "No API description provided."
        return self.api_desc.def_req

    def generate(self, logs: LogFile) -> list[Invariant]:
        if self.random_seed is not None:
            r = random.Random(self.random_seed)
        else:
            r = random.Random(time.time())

        logs_all: list[tuple[int, LogItem]] = []
        for log_idx, log in enumerate(logs.log_items):
            if log.api == self.api:
                logs_all.append((log_idx, log))

        if self.all_fields:
            all_fields = set()
            for log_idx, log in logs_all:
                all_fields.update(log.arguments)
        all_fields = sorted(all_fields)
        all_fields = [Field.construct_from("argument", f) for f in all_fields]

        if len(logs_all) < self.sample_size:
            logs_sample = list(logs_all)
        else:
            logs_sample = r.sample(logs_all, self.sample_size)

        logs_sample.sort(key=lambda x: x[0])

        logs_str = "\n".join(
            [l.to_prompt_string(include_arguments=True) for _, l in logs_sample]
        )

        prompt_system = COMMONSENSE_CONSTRAINT_SYSTEM
        prompt_user = COMMONSENSE_CONSTRAINT_USER.format(
            api_param_desc=self.get_api_desc(), logs=logs_str
        )

        prompt = [
            {"role": "system", "content": prompt_system},
            {"role": "user", "content": prompt_user},
        ]

        for idx_turn in range(self.gpt_invoker.turns):
            res = self.gpt_invoker.generate(prompt)
            prompt.append({"role": "assistant", "content": res})

            try:
                py_code = self.gpt_invoker.extract_code(res)
                invariants = self.gpt_invoker.extract_invs(res)
                invariants_desc = "\n".join(invariants)

                if self.all_fields:
                    related_fields = list(all_fields)
                else:
                    fields = self.gpt_invoker.extract_fields(res)
                    related_fields = []
                    for domain, field in fields:
                        if domain.api != self.api:
                            raise ValueError(
                                "API does not match. Expected: {self.api}, got: {domain.api}."
                            )
                        related_fields.append(field)
                api_domain = APIDomain.construct_from(
                    self.api, related_fields, "forall"
                )

                predicate = Predicate.construct_from_py_code(
                    invariants_desc, py_code, 1
                )

                premise = Premise.construct_from("derive", "true")

                invariant = Invariant.construct_from([api_domain], premise, predicate)

                for log_idx, log in logs_sample:
                    check_res, e = run_py_predicate(predicate, [log], [related_fields])
                    if not check_res:
                        raise ValueError(
                            f"Checker failed for log: {log.to_prompt_string(include_arguments=True)}.\nNested Error: {e}"
                        )

                return [invariant]
            except Exception as e:
                # print(traceback.format_exc())
                # print(str(e))
                # raise
                prompt_err = COMMONSENSE_CONSTRAINT_FEEDBACK.format(reasons=str(e))
                prompt.append({"role": "user", "content": prompt_err})

        return []


class InvGeneratorDataFlowTwo(InvGenerator):
    def __init__(
        self,
        api1: str,
        api2: str,
        sample_size: int,
        gpt_invoker: GPTInvoker,
        random_seed=None,
        proj_desc: ProjDescFile = None,
        all_fields: bool = False,
    ):
        self.api1 = api1
        self.api2 = api2
        self.random_seed = random_seed
        self.gpt_invoker = gpt_invoker
        self.proj_desc = proj_desc
        if proj_desc is not None:
            self.api_desc1 = proj_desc.api_map[api1]
            self.api_desc2 = proj_desc.api_map[api2]
        else:
            self.api_desc1 = None
            self.api_desc2 = None
        self.sample_size = sample_size
        self.all_fields = all_fields

    def generate(self, logs: LogFile) -> list[Invariant]:
        if self.random_seed is not None:
            r = random.Random(self.random_seed)
        else:
            r = random.Random(time.time())

        api2_logs: list[tuple[int, LogItem]] = []
        for log_idx, log in enumerate(logs.log_items):
            if log.api == self.api2:
                api2_logs.append((log_idx, log))
        api1_logs: list[tuple[int, LogItem]] = []
        for api2_log_idx, api2_log in api2_logs:
            api1_log_idx, api1_log = find_nearest_related_event(
                api2_log, api2_log_idx, logs, self.api1, "before"
            )
            api1_logs.append((api1_log_idx, api1_log))

        api_pairs = list(filter(lambda x: x[0][0] != -1, zip(api1_logs, api2_logs)))

        if len(api_pairs) < self.sample_size:
            print(
                f"Warning: Not enough pairs for {self.api1} and {self.api2}: {len(api_pairs)}"
            )
        if len(api_pairs) == 0:
            return []

        if self.all_fields:
            all_fields1 = set()
            all_fields2 = set()
            all_fields1_res = set()
            all_fields2_res = set()
            for (api1_log_idx, api1_log), (api2_log_idx, api2_log) in api_pairs:
                all_fields1.update(api1_log.arguments)
                all_fields2.update(api2_log.arguments)
                if isinstance(api1_log.response, dict):
                    all_fields1_res.update(api1_log.response)
                else:
                    all_fields1_res.add("__str__")
                if isinstance(api2_log.response, dict):
                    all_fields2_res.update(api2_log.response)
                else:
                    all_fields2_res.add("__str__")

        all_fields1 = sorted(all_fields1)
        all_fields1 = [Field.construct_from("argument", f) for f in all_fields1]
        all_fields2 = sorted(all_fields2)
        all_fields2 = [Field.construct_from("argument", f) for f in all_fields2]
        all_fields1_res = sorted(all_fields1_res)
        all_fields1_res = [Field.construct_from("response", f) for f in all_fields1_res]
        all_fields2_res = sorted(all_fields2_res)
        all_fields2_res = [Field.construct_from("response", f) for f in all_fields2_res]

        all_fields1.extend(all_fields1_res)
        all_fields2.extend(all_fields2_res)

        if len(api_pairs) > self.sample_size:
            api_pairs_sample = r.sample(api_pairs, self.sample_size)
        else:
            api_pairs_sample = list(api_pairs)
        api_pairs_sample.sort(key=lambda x: x[0][0])

        logs_str_join = []
        for (api1_log_idx, api1_log), (api2_log_idx, api2_log) in api_pairs_sample:
            logs_str_join.append("[A] ")
            logs_str_join.append(
                api1_log.to_prompt_string(include_arguments=True, include_response=True)
            )
            logs_str_join.append("\n\n[B] ")
            logs_str_join.append(
                api2_log.to_prompt_string(include_arguments=True, include_response=True)
            )
            logs_str_join.append("\n\n\n")
        logs_str_join.append("\n")
        logs_str_join = "".join(logs_str_join)

        prompt_system = DATA_CONSTRAINT_SYSTEM
        prompt_user = DATA_CONSTRAINT_USER.format(
            api_url1=self.api1,
            api_url2=self.api2,
            api_param_desc1=self.get_api_desc_1(),
            api_response_desc1=self.get_api_desc_1_res(),
            api_param_desc2=self.get_api_desc_2(),
            api_response_desc2=self.get_api_desc_2_res(),
            log_pairs=logs_str_join,
        )

        prompt = [
            {"role": "system", "content": prompt_system},
            {"role": "user", "content": prompt_user},
        ]

        for idx_turn in range(self.gpt_invoker.turns):
            res = self.gpt_invoker.generate(prompt)
            prompt.append({"role": "assistant", "content": res})

            try:
                py_code = self.gpt_invoker.extract_code(res)
                invariants = self.gpt_invoker.extract_invs(res)
                invariants_desc = "\n".join(invariants)

                fields = self.gpt_invoker.extract_fields(res)
                if self.all_fields:
                    related_fields1 = list(all_fields1)
                    related_fields2 = list(all_fields2)
                else:
                    related_fields1, related_fields2 = [], []
                    for domain, field in fields:
                        if domain.api != self.api1 and domain.api != self.api2:
                            raise ValueError(
                                "API does not match. Expected: {self.api1} or {self.api2}, got: {domain.api}."
                            )
                        if domain.api == self.api1:
                            related_fields1.append(field)
                        if domain.api == self.api2:
                            related_fields2.append(field)
                api_domain1 = APIDomain.construct_from(
                    self.api1, related_fields1, "exists"
                )
                api_domain2 = APIDomain.construct_from(
                    self.api2, related_fields2, "forall"
                )

                predicate = Predicate.construct_from_py_code(
                    invariants_desc, py_code, 2
                )

                premise = Premise.construct_from(
                    "and", "two_first_nearest_after_second"
                )

                invariant = Invariant.construct_from(
                    [api_domain2, api_domain1], premise, predicate
                )

                for (api1_log_idx, api1_log), (
                    api2_log_idx,
                    api2_log,
                ) in api_pairs_sample:
                    check_res, e = run_py_predicate(
                        predicate,
                        [api1_log, api2_log],
                        [related_fields1, related_fields2],
                    )
                    if not check_res:
                        raise ValueError(
                            f"Checker failed for logs:\n[A] {api1_log.to_prompt_string(include_arguments=True)}\n[B] {api2_log.to_prompt_string(include_arguments=True, include_response=True)}\nerror: {e}"
                        )

                return [invariant]
            except Exception as e:
                prompt_err = DATA_CONSTRAINT_FEEDBACK.format(reasons=str(e))
                prompt.append({"role": "user", "content": prompt_err})

        return []

    def get_api_desc_1(self) -> str:
        if self.api_desc1 is None:
            return "No API description provided."
        return self.api_desc1.def_req

    def get_api_desc_1_res(self) -> str:
        if self.api_desc1 is None:
            return "No API description provided."
        return self.api_desc1.def_resp

    def get_api_desc_2(self) -> str:
        if self.api_desc2 is None:
            return "No API description provided."
        return self.api_desc2.def_req

    def get_api_desc_2_res(self) -> str:
        if self.api_desc2 is None:
            return "No API description provided."
        return self.api_desc2.def_resp


class InvGeneratorControlFlowTwo(InvGenerator):
    def generate(self, logs: LogFile) -> list[Invariant]:
        pass


def _json_schema_replace_value(v):
    if isinstance(v, int):
        return "_gen_inv_placeholder_int_"
    elif isinstance(v, float):
        return "_gen_inv_placeholder_float_"
    elif isinstance(v, str):
        return "_gen_inv_placeholder_str_"
    elif isinstance(v, bool):
        return "_gen_inv_placeholder_bool_"
    elif v is None:
        return None
    elif isinstance(v, list):
        res = [_json_schema_replace_value(x) for x in v]
        res = [(len(str(x)), x) for x in res]
        res.sort(reverse=True)

        if len(res) == 0:
            return []
        elif len(res) == 1:
            return [res[0][1]]
        # elif len(res) == 2:
        #     return [res[0][1], res[1][1]]
        else:
            return [res[0][1], "_gen_inv_placeholder_list_extra_"]

    elif isinstance(v, dict):
        return {k: _json_schema_replace_value(v) for k, v in v.items()}


def json_to_schema(j):
    j_res = _json_schema_replace_value(j)
    json_dump = json.dumps(j_res, indent=2)

    json_dump = json_dump.replace('"_gen_inv_placeholder_int_"', "<some int value>")
    json_dump = json_dump.replace('"_gen_inv_placeholder_float_"', "<some float value>")
    json_dump = json_dump.replace('"_gen_inv_placeholder_str_"', "<some str value>")
    json_dump = json_dump.replace('"_gen_inv_placeholder_bool_"', "<some bool value>")
    json_dump = json_dump.replace(
        '"_gen_inv_placeholder_list_extra_"', "<extra list items . . .>"
    )

    return json_dump


DUMP_TEMPLATE = """
# API: {api}

# Logs
{logs}

# Invariant
```python
{inv}
```
"""


def json_to_trim_str(j):
    j = json_trim(j)
    j = json.dumps(j, indent=2)
    j = j.replace('"_gen_inv_placeholder_list_extra_"', "<extra list items . . .>")
    return j


def json_trim(j):
    if isinstance(j, dict):
        return {k: json_trim(v) for k, v in j.items() if v is not None}
    elif isinstance(j, list):
        if len(j) > 3:
            return j[:3] + ["_gen_inv_placeholder_list_extra_"]
        else:
            return j
    else:
        return j


def schema_original_to_schema(s: JsonSchema, include_response: bool) -> dict:
    j = s.to_schema_json()
    assert isinstance(j, dict)
    result = {}
    if "arguments" in j:
        if isinstance(j["arguments"], dict):
            d = j["arguments"]
            if "headers" in d:
                del d["headers"]
            if "header" in d:
                del d["header"]
            if "httpHeaders" in d:
                del d["httpHeaders"]
        result["arguments"] = j["arguments"]
    if "env" in j:
        result["env"] = j["env"]
    if include_response and "response" in j:
        result["response"] = j["response"]
    return result


def db_schema_to_schema_json(db_schema: DbSchema, table_name: str) -> dict:
    table_schema = db_schema.schemas[table_name]
    result = {}
    for key, schema in table_schema.items():
        result[key] = schema.to_schema_json()
    return result


def schema_to_json(
    api: str,
    related_apis: list[str],
    related_tables: list[str],
    db_schema: DbSchema,
    log_schema: DbSchema,
    no_log_db: bool = False,
    no_log_log: bool = False,
    no_env: bool = False,
):
    results = schema_original_to_schema(
        log_schema.schemas[api]["log_data"], include_response=False
    )
    if not no_log_db:
        for related_table in related_tables:
            if "db_info" not in results:
                results["db_info"] = {}
            if "#" in related_table:
                real_table_name = related_table.split("#")[0]
            else:
                real_table_name = related_table
            j = db_schema_to_schema_json(db_schema, real_table_name)
            results["db_info"][related_table] = j
    if not no_log_log:
        for related_api in related_apis:
            if related_api not in log_schema.schemas:
                logger.warning(
                    "Related API %s not found in log schema for API %s",
                    related_api,
                    api,
                )
                continue
            if "related_events" not in results:
                results["related_events"] = {}
            j = schema_original_to_schema(
                log_schema.schemas[related_api]["log_data"], include_response=True
            )
            results["related_events"][related_api] = j
    if no_env:
        if "env" in results:
            del results["env"]
        if not no_log_log:
            for related_api in related_apis:
                if related_api in results["related_events"]:
                    if "env" in results["related_events"][related_api]:
                        del results["related_events"][related_api]["env"]

    results = schema_json_to_string(results)

    return results


class InvGeneratorCommonSenseNewJsonFormat(InvGenerator):
    def __init__(
        self,
        api: str,
        sample_size: int,
        gpt_invoker: GPTInvoker,
        random_seed=None,
        proj_desc: ProjDescFile = None,
        predict_only_schema=False,
        output_file=None,
        log_schema: DbSchema | None = None,
        db_schema: DbSchema | None = None,
        no_log_db=False,
        no_log_log=False,
        no_env=False,
        no_schema=False,
    ):
        self.api = api
        self.random_seed = random_seed
        self.gpt_invoker = gpt_invoker
        self.proj_desc = proj_desc
        self.predict_only_schema = predict_only_schema
        self.output_file = output_file
        if proj_desc is not None:
            self.api_desc = proj_desc.api_map[api]
        else:
            self.api_desc = None
        self.sample_size = sample_size
        self.log_schema = log_schema
        self.db_schema = db_schema

        self.no_log_db = no_log_db
        self.no_log_log = no_log_log
        self.no_env = no_env
        self.no_schema = no_schema

    def get_api_desc(self) -> str:
        if self.api_desc is None:
            api_desc = "No API description provided."
        else:
            api_desc = self.api_desc.def_req
        result = f"API: {self.api}\n{api_desc}"
        return result

    def generate(self, logs: LogFile) -> tuple[list[Invariant], list]:
        if self.random_seed is not None:
            r = random.Random(self.random_seed)
        else:
            r = random.Random(time.time())

        related_fields = RelatedFields(
            include_arguments=True,
            include_env=not self.no_env,
            include_db_info=not self.no_log_db,
            include_related_log=not self.no_log_log,
            related_include_arguments=True,
            related_include_response=True,
            related_include_env=not self.no_env,
        )

        logs_all: list[tuple[int, LogItem]] = []
        for log_idx, log in enumerate(logs.log_items):
            if log.api == self.api:
                logs_all.append((log_idx, log))

        if self.predict_only_schema:
            assert self.db_schema is not None
            assert self.log_schema is not None

            log_strs = [log.to_execute_json(related_fields) for _, log in logs_all]
            schema_log_mappings = {}
            related_apis = set()
            related_tables = set()
            for log_str in log_strs:
                if "db_info" in log_str:
                    related_tables.update(log_str["db_info"].keys())
                if "related_events" in log_str:
                    related_apis.update(log_str["related_events"].keys())
                schema = json_to_schema(log_str)
                if schema not in schema_log_mappings:
                    schema_log_mappings[schema] = []
                schema_log_mappings[schema].append(log_str)
            schema_strs = list(schema_log_mappings.keys())
            schema_strs.sort(key=lambda x: len(x), reverse=True)
            if len(schema_strs) > self.sample_size:
                schema_strs = schema_strs[: self.sample_size]
            if not self.no_schema:
                log_sample = r.sample(schema_log_mappings[schema_strs[0]], 1)[0]
                log_sample = json_to_trim_str(log_sample)
            else:
                all_logs = []
                for v in schema_log_mappings.values():
                    all_logs.extend(v)
                num_sample = min(5, len(all_logs))
                log_sample = r.sample(all_logs, num_sample)

                log_sample = "\n\n".join(
                    [
                        json.dumps(
                            l,
                            indent=2,
                        )
                        for l in log_sample
                    ]
                )

            logs_str = "\n\n".join(schema_strs)

            related_apis = sorted(related_apis)
            related_tables = sorted(related_tables)
            schema_to_json_result = schema_to_json(
                self.api,
                related_apis,
                related_tables,
                self.db_schema,
                self.log_schema,
                no_log_db=self.no_log_db,
                no_log_log=self.no_log_log,
                no_env=self.no_env,
            )

            # logger.info("Schema from logs: %s", logs_str)
            # logger.info("Schema from schema: %s", schema_to_json_result)

            # raise ValueError("STOP")

            if not self.no_schema:
                logs_str = schema_to_json_result

                if len(log_sample) < 1024 * 4:
                    logs_str = (
                        logs_str
                        + "\n\nSome instances of this API can be found in these logs:\n"
                        + log_sample
                    )
            else:
                logs_str = log_sample
        else:
            if len(logs_all) < self.sample_size:
                logs_sample = list(logs_all)
            else:
                logs_sample = r.sample(logs_all, self.sample_size)

            logs_sample.sort(key=lambda x: x[0])

            logs_str = "\n\n".join(
                [
                    json.dumps(
                        l.to_execute_json(related_fields),
                        indent=2,
                    )
                    for _, l in logs_sample
                ]
            )

        prompt_system = (
            COMMONSENSE_CONSTRAINT_SYSTEM_FORMAT_JSON
            if not self.predict_only_schema
            else COMMONSENSE_CONSTRAINT_SYSTEM_FORMAT_JSON_SCHEMA_ONLY
        )
        prompt_user = COMMONSENSE_CONSTRAINT_USER_FORMAT_JSON.format(
            api_param_desc=self.get_api_desc(), logs=logs_str
        )

        prompt = [
            {"role": "system", "content": prompt_system},
            {"role": "user", "content": prompt_user},
        ]

        for idx_turn in range(self.gpt_invoker.turns):
            if idx_turn > 10:
                logger.warning(
                    "Too many rounds for API %s, round %d", self.api, idx_turn
                )
            # if idx_turn == 5:
            #     logger.warning(
            #         "Too many rounds for API %s. Error msg: %s",
            #         self.api,
            #         prompt[-1]["content"],
            #     )

            res = self.gpt_invoker.generate(prompt)
            prompt.append({"role": "assistant", "content": res})

            try:
                py_code = self.gpt_invoker.extract_code(res)
                invariants = self.gpt_invoker.extract_invs(res)
                invariants_desc = "\n".join(invariants)

                api_domain = APIDomain.construct_from(
                    self.api, related_fields, "forall"
                )

                predicate = Predicate.construct_from_py_code(
                    invariants_desc, py_code, 1
                )

                premise = Premise.construct_from("derive", "true")

                invariant = Invariant.construct_from([api_domain], premise, predicate)

                for _, log in logs_all:
                    check_res, e = run_py_predicate_new_json_format(
                        predicate, [log], [related_fields]
                    )
                    if not check_res:
                        log_str = json_to_trim_str(log.to_execute_json(related_fields))
                        raise ValueError(
                            f"Checker failed for log: {log_str}.\nNested Error: {e}"
                        )

                if self.output_file is not None:
                    out = DUMP_TEMPLATE.format(
                        api=self.api,
                        logs=logs_str,
                        inv=invariant.predicate.py_code,
                    )
                    self.output_file.write(out)
                    self.output_file.flush()

                return [invariant], prompt
            except Exception as e:
                prompt_err = COMMONSENSE_CONSTRAINT_FEEDBACK_FORMAT_JSON.format(
                    reasons=str(e)
                )
                prompt.append({"role": "user", "content": prompt_err})

        return [], prompt
