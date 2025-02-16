import json
import os
import pickle

import zstandard

from ..file_types.log_file import LogFile


def determine_external_apis(logs: LogFile) -> list[str]:
    results = set()

    all_uas = set()

    for log in logs.log_items:
        log_content = log.content
        header = log_content.get("headers", {})
        # print(header)
        if "user-agent" in header:
            ua = header["user-agent"]
        elif "User-Agent" in header:
            ua = header["User-Agent"]
        else:
            ua = None

        if ua is None:
            continue

        if ua.startswith("Mozilla/5.0"):
            results.add(log.api)

    return list(results)

