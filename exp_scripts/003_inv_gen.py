import json
import multiprocessing
import os
import pickle
import sys
import traceback
from collections import defaultdict

import tqdm
import zstandard

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))


from webnorm_gpt import logger
from webnorm_gpt.file_types.log_file import LogFile, LogItem
from webnorm_gpt.file_types.proj_desc_file import ProjDescFile
from webnorm_gpt.gen_inv.inv_gens import InvGeneratorCommonSenseNewJsonFormat
from webnorm_gpt.gpt_invoker import GPTInvoker
from webnorm_gpt.schema_induction.join_all import join_all
from webnorm_gpt.schema_induction.relation_induction import infer_relations_in_table

N_SAMPLES_IN_GEN = 10


def load_dataflow_map():
    cur_path = os.path.abspath(os.path.dirname(__file__))
    with open(
        os.path.join(
            cur_path,
            "../exp-results/train-ticket/hmm_deduction_result_pred_filtered.json",
        ),
        "rt",
    ) as f:
        hmm_deduction_result_pred_filtered = json.load(f)

    results = defaultdict(list)

    for k, v in hmm_deduction_result_pred_filtered.items():
        for t in v:
            if t != k:
                results[k].append(t)

    return results


share_between_processes = None
NUM_PROCESSES = 8
output_folder = None


def main():
    global share_between_processes
    global output_folder

    cur_path = os.path.abspath(os.path.dirname(__file__))

    output_folder = os.path.join(cur_path, "../gen-inv-dump")
    os.makedirs(output_folder, exist_ok=True)

    with zstandard.open(os.path.join(cur_path, "db_and_schemas.pickle.zst"), "rb") as f:
        db, db_schema, log_schema, binlogs = pickle.load(f)

    proj_desc_file = ProjDescFile()
    proj_desc_file.load_from_file_path(
        os.path.join(
            cur_path,
            "../generated/train-ticket-projdesc.json.zst",
        )
    )

    dataflow_map = load_dataflow_map()

    with open(
        os.path.join(
            cur_path,
            "../exp-results/train-ticket/gpt-4o/sql-foreign-keys.json",
        ),
        "rt",
    ) as f:
        foreign_key_results = json.load(f)

    training_logs, training_tables = join_all(
        db, foreign_key_results, dataflow_map, binlogs
    )

    print("Join finished")

    gpt_invoker = GPTInvoker(
        turns=20,
        dump_gpt_log=True,
    )

    queue = multiprocessing.Queue(len(proj_desc_file.apis) + NUM_PROCESSES)
    qback = multiprocessing.Queue(len(proj_desc_file.apis) + NUM_PROCESSES)

    with open(os.path.join(cur_path, "focal_apis.json"), "rt") as f:
        focal_apis = json.load(f)
    focal_apis = set(focal_apis)

    share_between_processes = (
        queue,
        training_logs,
        proj_desc_file,
        gpt_invoker,
        qback,
        db_schema,
        log_schema,
        focal_apis,
    )
    pool = multiprocessing.Pool(NUM_PROCESSES)

    for i in range(NUM_PROCESSES):
        pool.apply_async(run_child, args=(i,))

    for i, _ in enumerate(proj_desc_file.apis):
        queue.put(i)
    for _ in range(NUM_PROCESSES):
        queue.put(None)

    print("Put finished")

    with open(os.path.join(cur_path, "ivn_gen_output.jsonl"), "w") as f:
        t = tqdm.tqdm(range(len(proj_desc_file.apis)))
        for result_idx in t:
            idx, invariants, prompts = qback.get()

            os.makedirs(os.path.join(cur_path, "../inv-gen-prompts"), exist_ok=True)
            with open(
                os.path.join(cur_path, "../inv-gen-prompts", f"{idx:04d}.log"), "w"
            ) as fout:
                if invariants is not None:
                    for inv in invariants:
                        fout.write(
                            "-----------------------------------------------------------\n"
                        )
                        fout.write("INV\n")
                        fout.write(inv["predicate"]["py_code"])
                        fout.write("\n")
                for p in prompts:
                    fout.write(
                        "-----------------------------------------------------------\n"
                    )
                    fout.write(p["role"])
                    fout.write("\n")
                    fout.write(p["content"])
                    fout.write("\n")

            if invariants is None:
                continue

            if len(invariants) > 0:
                pass
            else:
                logger.warning(
                    "API %s has no invariants.", proj_desc_file.apis[idx].name
                )
                # print(f"Warn: {proj_desc_file.apis[idx].name} has no invariants.")

            for inv in invariants:
                f.write(json.dumps(inv))
                f.write("\n")


def run_child(idx):
    (
        queue,
        training_logs,
        proj_desc_file,
        gpt_invoker,
        qback,
        db_schema,
        log_schema,
        focal_apis,
    ) = share_between_processes

    while True:
        api_idx = queue.get()
        if api_idx is None:
            break

        api = proj_desc_file.apis[api_idx]

        if api.name not in training_logs:
            qback.put((api_idx, None, []))
            continue

        if len(training_logs[api.name].log_items) < 5:
            qback.put((api_idx, None, []))
            continue

        if api.name not in focal_apis:
            qback.put((api_idx, None, []))
            continue

        # use logs to predict
        # inv_gen = InvGeneratorCommonSenseNewJsonFormat(
        #     api.name,
        #     N_SAMPLES_IN_GEN,
        #     gpt_invoker,
        #     0,
        #     proj_desc_file,
        # )

        # use schema to predict
        with open(os.path.join(output_folder, f"{api.name}.md"), "w") as f:
            inv_gen = InvGeneratorCommonSenseNewJsonFormat(
                api.name,
                3,
                gpt_invoker,
                0,
                proj_desc_file,
                True,
                f,
                log_schema=log_schema,
                db_schema=db_schema,
            )

            try:
                invariants, prompts = inv_gen.generate(training_logs[api.name])
            except Exception as e:
                # raise
                # traceback.print_exc()
                exc_info = traceback.format_exc()
                logger.error(
                    "Error in generating invariants for %s\n%s", api.name, exc_info
                )
                qback.put((api_idx, None, []))
                continue

            invs = [inv.save_to_json() for inv in invariants]

            qback.put((api_idx, invs, prompts))


if __name__ == "__main__":
    main()
