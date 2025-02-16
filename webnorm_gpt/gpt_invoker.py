import datetime
import hashlib
import json
import os
import re
import sqlite3
import traceback
import typing
from typing import Optional

from openai import OpenAI

from . import file_names, load_internal_config, logger
from .gen_inv.base import API_DOMAIN_ALL_PLACEHOLDER, APIDomainAllPlaceholder, Field

RESPONSE_CODE_PATTERN = re.compile("```(?:\w+\s+)?(.*?)```", re.DOTALL)
RESPONSE_INVARIANT_PATTERN = re.compile("<invariant>(.*?)</invariant>")
RESPONSE_FIELD_PATTERN = re.compile("<field>(.*?)</field>")

RESPONSE_JSON_PATTERN = re.compile("```json\n(.*?)```", re.DOTALL)


class GPTInvoker:
    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        top_p: Optional[float] = None,
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
        turns: Optional[int] = None,
        api_host: Optional[str] = None,
        always_disable_cache: bool = False,
        dump_gpt_log: bool = False,
    ) -> None:
        api_key = api_key if api_key is not None else load_internal_config.api_key
        model = model if model is not None else load_internal_config.api_model
        top_p = top_p if top_p is not None else load_internal_config.top_p
        max_tokens = (
            max_tokens if max_tokens is not None else load_internal_config.max_tokens
        )
        temperature = (
            temperature if temperature is not None else load_internal_config.temperature
        )
        turns = turns if turns is not None else load_internal_config.turns
        api_host = api_host if api_host is not None else load_internal_config.api_host

        self.client = OpenAI(api_key=api_key, base_url=api_host, timeout=1800)
        self.model = model
        self.model_args = {
            "top_p": top_p,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
        self.turns = turns
        self.dump_gpt_log = dump_gpt_log

        self.gpt_cache = sqlite3.connect(file_names.gpt_cache_sqlite)
        self._init_gpt_cache()

        self.always_disable_cache = always_disable_cache
        if os.environ.get("WEBNORM_ALWAYS_DISABLE_CACHE", ""):
            self.always_disable_cache = True

        self.gpt_log = open(file_names.gpt_log_file, "ab", buffering=0)

        self.prompt_organize_input_system = None
        self.prompt_organize_input_user = None
        self.prompt_organize_input_error = None

    def _init_gpt_cache(self):
        cursor = self.gpt_cache.cursor()
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS gpt_cache (cache_id PRIMARY KEY, cache_digest TEXT, cache_prompt TEXT, cache_response TEXT)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS cache_digest_index ON gpt_cache (cache_digest)"
        )
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS gpt_usage (input_tokens INT, output_tokens INT, timestamp TEXT, model TEXT)"
        )
        self.gpt_cache.commit()

        seperate_token = "|9ZPA|"
        model_args_sorted = sorted(self.model_args.items())
        msg_concat = []
        for key, value in model_args_sorted:
            msg_concat.append(seperate_token)
            msg_concat.append(key)
            msg_concat.append(seperate_token)
            msg_concat.append(repr(value))
        msg_concat.append(seperate_token)
        msg_concat.append(self.model)
        self.model_args_str = "".join(msg_concat)

    def add_to_gpt_usage(self, input_tokens: int, output_tokens: int):
        cursor = self.gpt_cache.cursor()
        now = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        cursor.execute(
            "INSERT INTO gpt_usage (input_tokens, output_tokens, timestamp, model) VALUES (?, ?, ?, ?)",
            (input_tokens, output_tokens, now, self.model),
        )
        self.gpt_cache.commit()

    def _query_gpt_cache(self, msg_digest: str, msg_concat: str) -> Optional[str]:
        cursor = self.gpt_cache.cursor()
        cursor.execute(
            "SELECT cache_prompt, cache_response FROM gpt_cache WHERE cache_digest = ? ",
            (msg_digest,),
        )
        response = cursor.fetchone()
        if response:
            cache_prompt_q, cache_response = response
            if cache_prompt_q == msg_concat:
                return cache_response
        return None

    def _put_gpt_cache(self, msg_digest: str, msg_concat: str, msg_response: str):
        cursor = self.gpt_cache.cursor()
        cursor.execute(
            "SELECT cache_prompt FROM gpt_cache WHERE cache_digest = ? ",
            (msg_digest,),
        )
        response = cursor.fetchone()
        if response:
            cache_prompt_q = response[0]
            if cache_prompt_q == msg_concat:
                cursor.execute(
                    "UPDATE gpt_cache SET cache_response = ? WHERE cache_digest = ?",
                    (msg_response, msg_digest),
                )
            else:
                return
        else:
            cursor.execute(
                "INSERT INTO gpt_cache (cache_digest, cache_prompt, cache_response) VALUES (?, ?, ?)",
                (msg_digest, msg_concat, msg_response),
            )
        self.gpt_cache.commit()

        now = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        log_record = (
            repr((now, msg_digest, msg_concat, msg_response)).encode("utf-8") + b"\n"
        )
        self.gpt_log.write(log_record)

    def _msg_digest(self, message: list[dict[str, str]]) -> tuple[str, str]:
        msg_concat = [self.model_args_str]
        seperate_token = "|s9Z69ZPA|"
        for msg in message:
            role = msg["role"]
            content = msg["content"]
            match role:
                case "system":
                    role = "S"
                case "user":
                    role = "U"
                case "assistant":
                    role = "A"
            msg_concat.append(seperate_token)
            msg_concat.append(role)
            msg_concat.append(content)
        msg_concat = "".join(msg_concat)
        msg_digest = hashlib.sha1(msg_concat.encode()).hexdigest()
        return msg_digest, msg_concat

    def generate_inner(self, messages: list[dict[str, str]]) -> str:
        response = self.client.chat.completions.create(
            model=self.model, messages=messages, **self.model_args
        )
        self.add_to_gpt_usage(
            response.usage.prompt_tokens, response.usage.completion_tokens
        )
        response = response.choices[0].message.content
        return response

    def generate_inner_stream(self, messages: list[dict[str, str]]) -> str:
        response = self.client.chat.completions.create(
            model=self.model, messages=messages, stream=True, **self.model_args
        )
        all_text = []
        for sse_chunk in response:
            # logger.warning("Chunk: %s", sse_chunk)
            # content = sse_chunk.choices[0].delta.content
            # logger.warning("Generating: %s", repr(content))
            # if content is not None:
            #     all_text.append(content)
            if len(sse_chunk.choices) > 0:
                content = sse_chunk.choices[0].delta.content
                if content is not None:
                    all_text.append(content)
        response = "".join(all_text)
        # logger.warning("Generated: %s", response)
        # raise
        return response

    def generate(self, messages: list[dict[str, str]], ignore_cache=False) -> str:
        # all_text = []
        # for msg in messages:
        #     all_text.append(msg["content"])
        # all_text = "\n".join(all_text)
        # all_text_sha1 = hashlib.sha1(all_text.encode()).hexdigest()
        # os.makedirs("content-dump", exist_ok=True)
        # with open(f"content-dump/{all_text_sha1}.txt", "w") as f:
        #     f.write(all_text)
        # raise Exception("STOP Generation")

        msg_digest, msg_concat = self._msg_digest(messages)
        if not ignore_cache and not self.always_disable_cache:
            gpt_cache = self._query_gpt_cache(msg_digest, msg_concat)
            if gpt_cache is not None:
                if self.dump_gpt_log:
                    self.dump_log(messages, gpt_cache, True)
                return gpt_cache

        logger.debug(
            "Generating response from %s with input length %d",
            self.model,
            len(msg_concat),
        )
        try:
            response = self.generate_inner(messages)
        except Exception:
            if self.dump_gpt_log:
                err_msg = traceback.format_exc()
                self.dump_log(messages, err_msg, False, True)
            raise
        # response = self.generate_inner_stream(messages)
        logger.debug(
            "Generated response from %s with output length %d",
            self.model,
            len(response),
        )

        self._put_gpt_cache(msg_digest, msg_concat, response)

        if self.dump_gpt_log:
            self.dump_log(messages, response, False)

        return response

    def extract_code(self, message: str) -> str:
        code_matches = RESPONSE_CODE_PATTERN.findall(message)
        if len(code_matches) == 0:
            raise ValueError("No code block found in the response")
        if len(code_matches) > 1:
            raise ValueError("Multiple code blocks found in the response")

        code = code_matches[0]

        return code

    def extract_invs(self, message: str) -> list[str]:
        invariant_matches = RESPONSE_INVARIANT_PATTERN.findall(message)
        return invariant_matches

    def extract_fields(
        self, message: str
    ) -> list[tuple[typing.Union[str, APIDomainAllPlaceholder], Field]]:
        field_matches = RESPONSE_FIELD_PATTERN.findall(message)
        results = []
        for f in field_matches:
            if "::" not in f:
                raise ValueError(
                    f"Invalid field format: {f}, should contain `::` to sepreate api and field"
                )
            api_domain, field = f.split("::", 1)
            if api_domain == "__all__":
                api_domain = API_DOMAIN_ALL_PLACEHOLDER
            if "." not in field:
                raise ValueError(
                    f"Invalid field format: {f}, should contain `.` to sepreate category and name"
                )
            category, name = field.split(".", 1)
            if category not in ["argument", "response", "header", "env"]:
                raise ValueError(
                    f"Invalid category: {category} in field {f}, should be one of 'argument', 'response', 'header', 'env'"
                )
            field = Field.construct_from(category, name)
            results.append((api_domain, field))
        return results

    def extract_json(self, message: str):
        json_matches = RESPONSE_JSON_PATTERN.findall(message)
        if len(json_matches) == 0:
            raise ValueError("No json block found in the response")
        if len(json_matches) > 1:
            raise ValueError("Multiple json blocks found in the response")
        json_str = json_matches[0]
        json_obj = json.loads(json_str)
        return json_obj

    def dump_log(
        self,
        messages: list[dict[str, str]],
        output: str,
        is_from_cache: bool,
        is_error: bool = False,
    ):
        all_msgs = str(messages)
        md5 = hashlib.md5(all_msgs.encode()).hexdigest()

        now_time = datetime.datetime.now()
        now = now_time.strftime("%Y-%m-%d-%H-%M-%S-%f")
        idx = 0
        cache_str = "-C" if is_from_cache else "-N"
        if is_error:
            cache_str = "-E"
        while True:
            folder = now_time.strftime("%Y-%m-%d/%H" + cache_str)
            os.makedirs(os.path.join(file_names.gpt_dump_folder, folder), exist_ok=True)
            full_name = os.path.join(
                file_names.gpt_dump_folder, folder, f"{now}-{md5}-{idx}.md"
            )
            if not os.path.exists(full_name):
                break
            idx += 1

        with open(full_name, "w") as f:
            for msg in messages:
                role = msg["role"]
                content = msg["content"]
                f.write(
                    f"--------------------------------- {role} ---------------------------------\n"
                )
                f.write(content)
                f.write("\n")
            f.write("\n")
            f.write(
                f"--------------------------------- Output ---------------------------------\n"
            )
            f.write(output)
            f.write("\n")
