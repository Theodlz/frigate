import requests
from astropy.time import Time
import os


def get_skyportal_token():
    try:
        token = os.getenv("SKYPORTAL_TOKEN")
        if token is None:
            raise ValueError("No SkyPortal token found")
        return token
    except Exception as e:
        raise ValueError(f"Failed to get SkyPortal token: {e}")


def get_candids_per_filter_from_skyportal(
    t_i, t_f, groupIDs, filterIDs, saved=False, token=None
):
    token = get_skyportal_token() if token is None else token
    host = "https://fritz.science/api/candidates_filter"
    headers = {"Authorization": f"token {token}"}
    # compute the isoformat of the start and end dates
    start_date = Time(t_i, format="jd").iso
    end_date = Time(t_f, format="jd").iso
    page = 1
    numPerPage = 500  # 500 is the max for this endpoint
    total = None
    counter = 0
    candids_per_filter = {}
    if saved:
        print(
            f"Getting saved candidates from SkyPortal from {start_date} to {end_date} for groupIDs {groupIDs} and filterIDs {filterIDs}..."
        )
    else:
        print(
            f"Getting candidates from SkyPortal from {start_date} to {end_date} for groupIDs {groupIDs} and filterIDs {filterIDs}..."
        )
    if not groupIDs and not filterIDs:
        return None, "No groupIDs or filterIDs provided"
    while total is None or counter < total:
        params = {
            "startDate": start_date,
            "endDate": end_date,
            "pageNumber": page,
            "numPerPage": numPerPage,
        }
        if saved:
            params["savedStatus"] = "savedToAllSelected"
        if groupIDs and groupIDs not in ["all", "*"]:
            params["groupIDs"] = groupIDs
        if filterIDs:
            params["filterIDs"] = filterIDs
        if total:
            params["totalMatches"] = total
        response = requests.get(host, headers=headers, params=params)
        if response.status_code != 200:
            return None, f"Failed to get candidates from SkyPortal: {response.text}"
        data = response.json().get("data", {})
        for candidate in data.get("candidates", []):
            # each candidate has a filter_id and a passing_alert_id which is the candid
            filter_id = int(candidate.get("filter_id"))
            passing_alert_id = candidate.get("passing_alert_id")
            if filter_id not in candids_per_filter:
                candids_per_filter[filter_id] = []
            candids_per_filter[filter_id].append(passing_alert_id)
            counter += 1

        total = data.get("totalMatches", total)
        print(
            f"Got {counter} candidates at page {page} out of {total/numPerPage:.0f} pages..."
        )
        page += 1

    # sort the keys of the dictionary by the number of candidates descending
    candids_per_filter = dict(
        sorted(candids_per_filter.items(), key=lambda item: len(item[1]), reverse=True)
    )
    print(
        f"Filters with candidates: {[key for key, value in candids_per_filter.items() if len(value) > 0]}"
    )
    print("Number of candidates per filter:")
    for key, value in candids_per_filter.items():
        print(f"Filter {key}: {len(value)}")
    return candids_per_filter, None
