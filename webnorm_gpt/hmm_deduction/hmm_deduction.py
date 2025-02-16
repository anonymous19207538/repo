import datetime
import os
import pickle
import subprocess
import time
from collections import namedtuple

import numpy as np
import zstandard
from hmmlearn import hmm

from .. import logger
from ..file_types.log_file import LogFile

TrainedHMM = namedtuple("TrainedHMM", ["transmat", "emissionprob", "startprob"])


def hmm_learn_with_hmmlearn_library(
    n: int, k: int, event_sequence: np.ndarray, splits: np.ndarray
) -> TrainedHMM:
    model = hmm.CategoricalHMM(n_components=k)
    model.fit(event_sequence, splits)
    return TrainedHMM(
        model.transmat_,
        model.emissionprob_,
        model.startprob_,
    )


def hmm_learn_with_fasthmm(
    n: int, k: int, event_sequence: np.ndarray, splits: np.ndarray
) -> TrainedHMM:
    cur_path = os.path.abspath(os.path.dirname(__file__))
    temp_dir = os.path.join(cur_path, "../../hmm-temp")
    current_pid = os.getpid()
    hmm_input_file_name = os.path.join(temp_dir, f"hmm_input_{current_pid}.txt")
    hmm_output_file_name = os.path.join(temp_dir, f"hmm_output_{current_pid}.txt")
    executable_path = os.path.join(
        cur_path, "../../fast-hmm-learn/target/release/fast-hmm-learn"
    )

    os.makedirs(temp_dir, exist_ok=True)

    logger.info("Writing to %s", hmm_input_file_name)

    with zstandard.open(hmm_input_file_name, "wt") as f:
        print(n, file=f)
        print(k, file=f)

        for val in event_sequence:
            print(val.item(), file=f)

    logger.info("Running %s", executable_path)
    subprocess.run(
        [executable_path, "-i", hmm_input_file_name, "-o", hmm_output_file_name],
        check=True,
    )

    logger.info("Reading from %s", hmm_output_file_name)
    values = []
    with zstandard.open(hmm_output_file_name, "rt") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            line = float(line)
            values.append(line)

    if len(values) != 2 + n * n + n * k + n:
        logger.error(
            "Failed to process hmm file. Expected %d values, got %d",
            2 + n * n + n * k + n,
            len(values),
        )
        raise ValueError("Failed to process hmm file")

    read_n = int(values[0])
    read_k = int(values[1])
    if read_n != n or read_k != k:
        logger.error(
            "Failed to process hmm file. Expected n=%d k=%d, got n=%d k=%d",
            n,
            k,
            read_n,
            read_k,
        )
        raise ValueError("Failed to process hmm file")

    transmat = np.array(values[2 : 2 + n * n], dtype=np.float64).reshape(n, n)
    emissionprob = np.array(
        values[2 + n * n : 2 + n * n + n * k], dtype=np.float64
    ).reshape(n, k)
    startprob = np.array(values[2 + n * n + n * k :], dtype=np.float64)

    return TrainedHMM(transmat, emissionprob, startprob)


def hmm_learn(
    n: int, k: int, event_sequence: np.ndarray, splits: np.ndarray
) -> TrainedHMM:
    t_start = time.time()
    # res = hmm_learn_with_fasthmm(n, k, event_sequence, splits)
    logger.info(
        "Starting HMM learning at %s",
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
    res = hmm_learn_with_hmmlearn_library(n, k, event_sequence, splits)
    logger.info(
        "Finished HMM learning at %s",
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
    t_end = time.time()

    logger.info("HMM learning took %f seconds", t_end - t_start)

    return res


def hmm_deduce(
    logs: LogFile,
    login_api: str | None,
    max_samples: int = 100000,
    status_api_rate: float = 2,
):
    embedding_dict = {}
    event_sequence = []

    log_items = logs.log_items
    if len(log_items) > max_samples:
        log_items = log_items[:max_samples]
        logger.info(
            "Got too %d many samples, truncating to %d",
            len(logs.log_items),
            max_samples,
        )
    else:
        logger.info("Got %d samples", len(log_items))

    for log_idx, log in enumerate(log_items):
        api = log.api

        if api not in embedding_dict:
            embedding_dict[api] = len(embedding_dict)
        event_sequence.append(embedding_dict[api])
    event_sequence = np.array(event_sequence).reshape(-1, 1)

    logger.info("Begin splitting")
    last_split_idx = 0
    splits = []
    for idx, log in enumerate(log_items):
        if log.api == login_api:
            last_length = idx - last_split_idx
            splits.append(last_length)
            last_split_idx = idx
    splits.append(len(log_items) - last_split_idx)
    splits = np.array(splits)

    n_apis = len(embedding_dict)

    learned = hmm_learn(n_apis, int(n_apis * status_api_rate), event_sequence, splits)

    status_dict = {
        "transmat_": learned.transmat,
        "emissionprob_": learned.emissionprob,
        "startprob_": learned.startprob,
        "embedding_dict": embedding_dict,
        "event_sequence": event_sequence,
        "splits": splits,
    }

    return status_dict


def compute_backward(emissionprob, transmat, decay, rounds):
    N, M = emissionprob.shape

    transmat_backward = transmat.T
    transmat_backward = transmat_backward / transmat_backward.sum(axis=1, keepdims=True)

    sum_array = np.zeros([M, M])

    status_prob = emissionprob.T
    status_prob = status_prob / status_prob.sum(axis=1, keepdims=True)

    decay_factor = 1
    for _ in range(rounds):
        status_prob = np.dot(status_prob, transmat_backward)
        sum_array += np.dot(status_prob, emissionprob) * decay_factor
        decay_factor *= decay

    return sum_array


def hmm_predict(trained_model: dict, decay, rounds):
    transmat = trained_model["transmat_"]
    emissionprob = trained_model["emissionprob_"]
    embedding_dict = trained_model["embedding_dict"]
    embedding_dict_inv = {v: k for k, v in embedding_dict.items()}

    backward_res = compute_backward(emissionprob, transmat, decay, rounds)

    result = {}
    for i in range(len(embedding_dict)):
        cur_status = embedding_dict_inv[i]
        target_status = backward_res[i]
        # target_status = target_status / (target_status.sum() + 1e-6)
        target_status_tuples = [
            (val.item(), j, embedding_dict_inv[j])
            for j, val in enumerate(target_status)
        ]
        target_status_tuples.sort(reverse=True)
        result[cur_status] = target_status_tuples

    return result
