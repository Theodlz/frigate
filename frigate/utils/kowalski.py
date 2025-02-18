import multiprocessing
import os
import uuid

from contextlib import closing


import numpy as np
import pandas as pd

from penquins import Kowalski
from tqdm import tqdm

from frigate.utils.datasets import save_dataframe, load_dataframe, remove_file

ZTF_ALERTS_CATALOG = "ZTF_alerts"

STRING_FIELDS = [
    "rbversion",
    "drbversion",
    "braai_version",
    "acai_b_version",
    "acai_h_version",
    "acai_n_version",
    "acai_o_version",
    "acai_v_version",
    "bts_version",
]


def shorten_string_fields(data: pd.DataFrame) -> pd.DataFrame:
    for field in STRING_FIELDS:
        if field in data.columns:
            data[field] = data[field].str.replace("_", "")
    return data


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
    low_memory=False,
    low_memory_format="parquet",
    low_memory_dir=None,
    format="parquet",
    verbose=True,
):
    if low_memory is True and low_memory_format not in ["parquet", "csv", "feather"]:
        return None, f"Invalid low_memory_format: {low_memory_format}"
    if low_memory is True and low_memory_dir is None:
        return None, "low_memory_dir is required when low_memory is True"

    total, err = candidates_count_from_kowalski(t_i, t_f, programids, objectIds)
    if err:
        return None, err

    if verbose:
        print(
            f"Expecting {total} candidates between {t_i} and {t_f} for programids {programids} (n_threads: {n_threads}, low_memory: {low_memory})"
        )

    filename = f"{t_i}_{t_f}_{'_'.join(map(str, programids))}.{format}"
    # look in the low memory dir (which is identical to the dir), if the file exists
    # if it does, load it and verify that it has the expected number of candidates
    try:
        existing_data = load_dataframe(filename, None, low_memory_dir)
        if existing_data is not None and len(existing_data) == total:
            if verbose:
                print(
                    f"Found existing data for {filename} with {total} candidates, skipping query"
                )
            return existing_data, None
    except Exception as e:
        if verbose:
            print(f"Failed to load existing data for {filename}: {e}, continuing")

    numPerPage = 10000
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
                    # we include everything except the following fields
                    "_id": 0,
                    "schemavsn": 0,
                    "publisher": 0,
                    "candidate.pdiffimfilename": 0,
                    "candidate.programpi": 0,
                    "candidate.candid": 0,
                    "cutoutScience": 0,
                    "cutoutTemplate": 0,
                    "cutoutDifference": 0,
                    "coordinates": 0,
                },
            },
            "kwargs": {"limit": numPerPage, "skip": i * numPerPage},
        }
        if objectIds is not None:
            query["query"]["filter"]["objectId"] = {"$in": objectIds}
        queries.append(query)

    candidates = []  # list of dataframes to concatenate later
    low_memory_pointers = []  # to use with low_memory=True

    # contextlib.closing should help close opened files or other things
    # it's just added security, it might not be necessary but could be in the future
    with closing(multiprocessing.Pool(processes=n_threads)) as pool:
        with tqdm(total=total, disable=not verbose) as pbar:
            for response in pool.imap_unordered(_run_query, queries):
                if not isinstance(response, dict):
                    return None, f"Failed to get candidates from Kowalski: {response}"
                if response.get("status") != "success":
                    return None, str(response.get("message"))[:1000]
                data = response.get("data", [])
                # wa want to flatten the candidate object
                data = pd.json_normalize(data)
                # we want to remove unnecessary chars from string fields to save space
                data = shorten_string_fields(data)

                if low_memory:
                    # if running in low memory mode, we directly store the partial dataframe
                    # and concatenate them later
                    # so we generate a random filename
                    filename = f"tmp_{uuid.uuid4()}.{low_memory_format}"
                    save_dataframe(
                        df=data,
                        filename=filename,
                        output_format=low_memory_format,
                        output_directory=low_memory_dir,
                        output_compression=None,
                        output_compression_level=None,
                    )
                    low_memory_pointers.append(filename)
                else:
                    # append to list of dataframes
                    candidates.append(data)
                pbar.update(len(data))
                del data, response

    # concatenate all dataframes
    if low_memory:
        candidates = []
        for filename in low_memory_pointers:
            data = load_dataframe(
                filename, format=low_memory_format, directory=low_memory_dir
            )
            candidates.append(data)
            remove_file(filename, directory=low_memory_dir)

    candidates = pd.concat(candidates, ignore_index=True)

    # sort by jd from oldest to newest (lowest to highest)
    candidates = candidates.sort_values(by="candidate.jd", ascending=True)

    if verbose:
        print(f"Got a total of {len(candidates)} candidates between {t_i} and {t_f}")
    return candidates, None
