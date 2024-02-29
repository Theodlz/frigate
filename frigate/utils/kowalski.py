import multiprocessing
import os

import numpy as np
import pandas as pd

from penquins import Kowalski
from tqdm import tqdm

ZTF_ALERTS_CATALOG = "ZTF_alerts"


def connect_to_kowalski() -> Kowalski:
    try:
        k = Kowalski(
            protocol="https",
            host="kowalski.caltech.edu",
            port=443,
            token=os.getenv("KOWALSKI_TOKEN"),
            verbose=False,
            timeout=6000,
        )
        return k
    except Exception as e:
        raise ValueError(f"Failed to connect to Kowalski: {e}")


def validate_kowalski_connection() -> bool:
    k = connect_to_kowalski()
    return k.ping()


def _run_query(query):
    # connect to Kowalski
    try:
        k: Kowalski = connect_to_kowalski()
        return k.query(query=query).get("default")
    except Exception as e:
        print(f"Failed to connect to Kowalski: {e}")
        exit(1)


def candidates_count_from_kowalski(t_i, t_f, programids, objectIds=None) -> (int, str):
    # run a count query to get the number of candidates we are to expect
    k = connect_to_kowalski()
    query = {
        "query_type": "count_documents",
        "query": {
            "catalog": ZTF_ALERTS_CATALOG,
            "filter": {
                "candidate.jd": {"$gte": t_i, "$lt": t_f},
                "candidate.programid": {"$in": programids},
            },
        },
    }
    if objectIds is not None:
        query["query"]["filter"]["objectId"] = {"$in": objectIds}

    response = k.query(query=query).get("default")
    if response.get("status") != "success":
        return None, str(response.get("message"))[:1000]
    count = response.get("data", None)
    if count is None:
        return None, "Failed to get count of candidates"
    return count, None


def get_candidates_from_kowalski(
    t_i: float,
    t_f: float,
    programids: list,
    objectIds=None,
    n_threads=multiprocessing.cpu_count(),
):
    total, err = candidates_count_from_kowalski(t_i, t_f, programids, objectIds)
    if err:
        return None, err

    print(
        f"Expecting {total} candidates between {t_i} and {t_f} for programids {programids} (n_threads: {n_threads})"
    )

    numPerPage = 10000
    candidates = pd.DataFrame()
    batches = int(np.ceil(total / numPerPage))
    if objectIds is not None:
        batches = int(np.ceil(len(objectIds) / numPerPage))

    queries = []
    for i in range(batches):
        query = {
            "query_type": "find",
            "query": {
                "catalog": ZTF_ALERTS_CATALOG,
                "filter": {
                    "candidate.jd": {"$gte": t_i, "$lt": t_f},
                    "candidate.programid": {"$in": programids},
                },
                "projection": {
                    "_id": 0,
                    "candid": 1,
                    "objectId": 1,
                    "candidate": 1,
                    "classifications": 1,
                },
            },
            "kwargs": {"limit": numPerPage, "skip": i * numPerPage},
        }
        if objectIds is not None:
            query["query"]["filter"]["objectId"] = {"$in": objectIds}
        queries.append(query)

    with multiprocessing.Pool(processes=n_threads) as pool:
        with tqdm(total=total) as pbar:
            for response in pool.imap_unordered(_run_query, queries):
                if not isinstance(response, dict):
                    return None, f"Failed to get candidates from Kowalski: {response}"
                if response.get("status") != "success":
                    return None, str(response.get("message"))[:1000]
                data = response.get("data", [])
                # wa want to flatten the candidate object
                data = pd.json_normalize(data)
                candidates = pd.concat([candidates, data], ignore_index=True)
                pbar.update(len(data))
    # sort by jd from oldest to newest (lowest to highest)
    candidates = candidates.sort_values(by="candidate.jd", ascending=True)

    print(f"Got a total of {len(candidates)} candidates between {t_i} and {t_f}")
    return candidates, None
