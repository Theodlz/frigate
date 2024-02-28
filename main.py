import argparse
import os
import time

import arrow
import corner
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
from astropy.time import Time
from penquins import Kowalski
from tqdm import tqdm
import multiprocessing

def str_to_bool(value):
    if isinstance(value, bool):
        return value
    if value.lower() in {'false', 'f', '0', 'no', 'n'}:
        return False
    elif value.lower() in {'true', 't', '1', 'yes', 'y'}:
        return True
    raise ValueError(f'{value} is not a valid boolean value')

catalog = "ZTF_alerts"

def get_candidates_from_skyportal(t_i, t_f, groupIDs, filterIDs, token, saved=False):
    host = "https://fritz.science/api/candidates"
    headers = {"Authorization": f"token {token}"}
    # compute the isoformat of the start and end dates
    start_date = Time(t_i, format='jd').iso
    end_date = Time(t_f, format='jd').iso
    page = 1
    numPerPage = 100
    queryID = None
    total = None
    candidates = []
    if saved:
        print(f"Getting saved candidates from SkyPortal from {start_date} to {end_date} for groupIDs {groupIDs} and filterIDs {filterIDs}...")
    else:
        print(f"Getting candidates from SkyPortal from {start_date} to {end_date} for groupIDs {groupIDs} and filterIDs {filterIDs}...")
    if not groupIDs and not filterIDs:
        return None, "No groupIDs or filterIDs provided"
    while total is None or len(candidates) < total:
        params = {
            "startDate": start_date,
            "endDate": end_date,
            "pageNumber": page,
            "numPerPage": numPerPage
        }
        if saved:
            params["savedStatus"] = "savedToAllSelected"
        if groupIDs:
            params["groupIDs"] = groupIDs
        if filterIDs:
            params["filterIDs"] = filterIDs
        if queryID:
            params["queryID"] = queryID
        if total:
            params["totalMatches"] = total
        response = requests.get(host, headers=headers, params=params)
        if response.status_code != 200:
            return None, f"Failed to get candidates from SkyPortal: {response.text}"
        data = response.json().get("data", {})
        candidates += [candidate["id"]  for candidate in data.get("candidates", [])]
        total = data.get("totalMatches", 0)
        print(f"Got {len(candidates)} candidates at page {page} out of {total/numPerPage:.0f} (queryID: {queryID})")
        queryID = data.get("queryID", None)
        page += 1
    return candidates, None


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

def candidates_count_from_kowalski(t_i, t_f, programids, objectIds=None) -> (int, str):
    # run a count query to get the number of candidates we are to expect
    k = connect_to_kowalski()
    query = {
        "query_type": "count_documents",
        "query": {
            "catalog": catalog,
            "filter": {
                'candidate.jd': {
                    '$gte': t_i,
                    '$lt': t_f
                },
                'candidate.programid': {
                    '$in': programids
                }
            }
        }
    }
    if objectIds is not None:
        query["query"]["filter"]["objectId"] = {
            '$in': objectIds
        }

    response = k.query(query=query).get("default")
    if response.get("status") != "success":
        return None, str(response.get("message"))[:1000]
    count = response.get("data", None)
    if count is None:
        return None, "Failed to get count of candidates"
    return count, None


def _run_query(query):
    # connect to Kowalski
    try:
        k: Kowalski = connect_to_kowalski()
        return k.query(query=query).get("default")
    except Exception as e:
        print(f"Failed to connect to Kowalski: {e}")
        exit(1)

def get_candidates_from_kowalski(t_i: float, t_f: float, programids: list, objectIds=None, n_threads=multiprocessing.cpu_count()):
    total, err = candidates_count_from_kowalski(t_i, t_f, programids, objectIds)
    if err:
        return None, err

    print(f"Expecting {total} candidates between {t_i} and {t_f} for programids {programids} (n_threads: {n_threads})")

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
                "catalog": catalog,
                "filter": {
                    'candidate.jd': {
                        '$gte': t_i,
                        '$lt': t_f
                    },
                    'candidate.programid': {
                        '$in': programids
                    }
                },
                "projection": {
                    '_id': 0,
                    'candid': 1,
                    'objectId': 1,
                    'candidate': 1,
                    'classifications': 1
                }
            },
            "kwargs": {
                "limit": numPerPage,
                "skip": i * numPerPage
            }
        }
        if objectIds is not None:
            query["query"]["filter"]["objectId"] = {
                '$in': objectIds
            }
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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query Kowalski for stats")
    parser.add_argument('--programids', type=str, default='1', help='Program IDs to query')
    parser.add_argument('--start', type=str, default=np.floor(Time.now().jd - 1)+0.5, help='Start time for the query, default to 1 day ago')
    parser.add_argument('--nb_days', type=float, default=1.0, help='Number of days to query')
    parser.add_argument('--end', type=str, default=None, help='End time for the query')
    parser.add_argument('--k_token', type=str, default=None, help='Kowalski token')
    parser.add_argument('--n_threads', type=str, default=None, help='Number of threads to use when parallelizing queries')
    args = parser.parse_args()

    if not args.k_token:
        print("No Kowalski token provided")
        exit(1)

    # add the token in the environment
    os.environ['KOWALSKI_TOKEN'] = args.k_token

    # connect to Kowalski
    try:
        k = Kowalski(
            protocol="https",
            host="kowalski.caltech.edu",
            port=443,
            token=args.k_token,
            verbose=True,
            timeout=6000,
        )
        valid = k.ping()
        if valid is not True:
            raise ValueError("Could not reach the server")
        del k # we just wanted to test the connection
    except Exception as e:
        print(f"Failed to connect to Kowalski: {e}")
        exit(1)

    n_threads = args.n_threads
    if n_threads is None:
        n_threads = multiprocessing.cpu_count()
    else:
        n_threads = int(n_threads)
        n_threads = min(n_threads, multiprocessing.cpu_count())

    try:
        programids = list(map(int, args.programids.split(',')))
    except ValueError:
        print(f"Invalid programids: {args.programids}")
        exit(1)

    # TODO: make these arguments
    try:
        # check if start is a string or a float as string
        try:
            t_i = float(args.start)
        except ValueError:
            t_i = Time(args.start).jd
    except ValueError:
        print(f"Invalid start time: {args.start}")
        exit(1)
    if args.end:
        try:
            try:
                t_f = float(args.end)
            except ValueError:
                t_f = Time(args.end).jd
        except ValueError:
            print(f"Invalid end time: {args.end}")
            exit(1)
    else:
        t_f = t_i+args.nb_days

    t_i = float(t_i)
    t_f = float(t_f)

    # get candidates from Kowalski
    candidates, err = get_candidates_from_kowalski(t_i, t_f, programids, n_threads=n_threads)

    # get all the candidates candid from fritz
    # get the data from kowalski
    #
    # check which objects are saved on Fritz
    if err:
        print(err)
        exit(1)
    # save the dataframe to disk, with name <start>_<end>_<programids>.csv
    filename = f"{t_i}_{t_f}_{'_'.join(map(str, programids))}.csv"
    candidates.to_csv(filename, index=False)
    print(f"Saved candidates to {filename}")
