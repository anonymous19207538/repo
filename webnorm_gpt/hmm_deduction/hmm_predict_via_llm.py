import json
import os
import pickle

import tqdm
import zstandard

from .. import logger
from ..file_types.proj_desc_file import ProjDescFile
from ..gpt_invoker import GPTInvoker
from .hmm_predict_via_llm_prompt import (
    HMM_FILTER_RETRY,
    HMM_FILTER_SYSTEM,
    HMM_FILTER_USER,
)


def api_to_desc(api: str, proj_desc: ProjDescFile, prob: float | None):
    api_desc = proj_desc.api_map[api]
    def_req = api_desc.def_req
    def_resp = api_desc.def_resp

    def_req = def_req.strip()
    def_req = def_req.replace("\n\n", "\n")
    def_req = def_req.replace("\n\n", "\n")
    def_req_lines = def_req.split("\n")
    def_req = "\n".join([f"    {line}" for line in def_req_lines])
    def_req = def_req[4:]

    def_resp = def_resp.strip()
    def_resp = def_resp.replace("\n\n", "\n")
    def_resp = def_resp.replace("\n\n", "\n")
    def_resp_lines = def_resp.split("\n")
    def_resp = "\n".join([f"    {line}" for line in def_resp_lines])
    def_resp = def_resp[4:]

    if prob is not None:
        prob_str = f" (prob: {prob:.2f})"
    else:
        prob_str = ""

    api_desc_full = (
        f"- API: {api}{prob_str}\n  req_params: {def_req}\n  response_type: {def_resp}"
    )
    return api_desc_full


def hmm_predict_via_llm(
    result_dict: dict[str, list[tuple[float, int, str]]],
    gpt_invoker: GPTInvoker,
    proj_desc: ProjDescFile,
    external_apis: set[str] | None,
    truncate_num: int = 20,
    truncate_prob: float = 0.1,
):
    all_result = {}
    result_dict_list = list(result_dict.items())
    t = tqdm.tqdm(result_dict_list)
    for api, targets in t:
        to_filter = []
        for idx, (prob, _, tgt_api) in enumerate(targets):
            if external_apis is not None and tgt_api not in external_apis:
                continue
            if prob < truncate_prob:
                targets = targets[:idx]
                break
            if len(to_filter) >= truncate_num:
                break
            to_filter.append((prob, None, tgt_api))

        if len(to_filter) == 0:
            continue

        targets = to_filter

        # if api != "cancel.service.CancelServiceImpl.cancelOrder":
        #     continue

        focal = api_to_desc(api, proj_desc, None)

        other_apis = []
        all_candidates = set()
        for prob, _, target in targets:
            other_apis.append(api_to_desc(target, proj_desc, prob))
            all_candidates.add(target)
        other_apis = "\n".join(other_apis)

        prompt = [
            {"role": "system", "content": HMM_FILTER_SYSTEM},
            {
                "role": "user",
                "content": HMM_FILTER_USER.format(
                    focal_api=focal, candidate_related_apis=other_apis
                ),
            },
        ]

        for _ in range(5):
            output = gpt_invoker.generate(prompt)
            try:
                output_json = gpt_invoker.extract_json(output)
                if "related_apis" not in output_json:
                    raise ValueError("No `related_apis` field found in the response")
                related_apis = output_json["related_apis"]
                for related_api in related_apis:
                    if related_api not in all_candidates:
                        raise ValueError(
                            f"Related API {related_api} not in the candidate list. Do you have a typo?"
                        )
                break
            except Exception as e:
                error_str = str(e)
                prompt.append(
                    {
                        "role": "assistant",
                        "content": output,
                    }
                )
                prompt.append(
                    {
                        "role": "user",
                        "content": HMM_FILTER_RETRY.format(error=error_str),
                    }
                )
        else:
            all_result[api] = None
            logger.error("Failed to extract related APIs for %s", api)

        all_result[api] = related_apis

    return all_result
