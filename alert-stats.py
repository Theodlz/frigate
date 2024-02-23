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



def get_values_batch(k, fields, rounding, t_i, t_f, programids, objectIds=None):
    field_leaves = [field.split('.')[-1] for field in fields]
    if objectIds is None:
        query = {
            "query_type": "aggregate",
            "query": {
                "catalog": catalog,
                "pipeline": [
                    {
                        '$match': {
                            'candidate.jd': {
                                '$gte': t_i,
                                '$lt': t_f
                            },
                            'candidate.programid': {
                                '$in': programids
                            }
                        }
                    }, {
                        '$project': {
                            '_id': 0,
                        }
                    }, {
                        '$group': {
                            '_id': None,
                            # 'values': {
                            #     '$push': f'${field_leaf}'
                            # }
                        }
                    }, {
                        '$project': {
                            '_id': 0,
                            # 'values': 1
                        }
                    }
                ]
            }
        }
        for field, field_leaf in zip(fields, field_leaves):
            query["query"]["pipeline"][1]["$project"][field_leaf] = {
                '$round': [
                    f'${field}', rounding
                ]
            } if rounding else f'${field}'
            query["query"]["pipeline"][2]["$group"][f"values_{field_leaf}"] = {
                '$push': f'${field_leaf}'
            }
            query["query"]["pipeline"][3]["$project"][f"values_{field_leaf}"] = 1

        response = k.query(query=query).get("default")
        if response is None:
            return None, "No response"
        if response.get("status") != "success":
            return None, str(response.get("message"))[:1000]
        if len(response.get("data", [])) == 0:
            return None, "No data found"
        data = response.get("data", [])[0]
        data_per_field = {fields[i]: data.get(f"values_{field_leaves[i]}", []) for i in range(len(field_leaves))}

    else:
        # here we batch the queries because we can't send too many objectIds at once, so we cap it to 1000 at a time
        data_per_field = {}
        for i in range(0, len(objectIds), 1000):
            query = {
                "query_type": "aggregate",
                "query": {
                    "catalog": catalog,
                    "pipeline": [
                        {
                            '$match': {
                                'objectId': {
                                    '$in': objectIds[i:i+1000]
                                },
                                'candidate.jd': {
                                    '$gte': t_i,
                                    '$lt': t_f
                                },
                                'candidate.programid': {
                                    '$in': programids
                                }
                            }
                        }, {
                            '$project': {
                                '_id': 0,
                            }
                        }, {
                            '$group': {
                                '_id': None,
                                # 'values': {
                                #     '$push': f'${field_leaf}'
                                # }
                            }
                        }, {
                            '$project': {
                                '_id': 0,
                                # 'values': 1
                            }
                        }
                    ]
                }
            }
            for field, field_leaf in zip(fields, field_leaves):
                query["query"]["pipeline"][1]["$project"][field_leaf] = {
                    '$round': [
                        f'${field}', rounding
                    ]
                } if rounding else f'${field}'
                query["query"]["pipeline"][2]["$group"][f"values_{field_leaf}"] = {
                    '$push': f'${field_leaf}'
                }
                query["query"]["pipeline"][3]["$project"][f"values_{field_leaf}"] = 1

            response = k.query(query=query).get("default")
            if response is None:
                return None, "No response"
            if response.get("status") != "success":
                return None, str(response.get("message"))[:1000]
            if len(response.get("data", [])) == 0:
                return None, "No data found"
            data = response.get("data", [])[0]
            for field, field_leaf in zip(fields, field_leaves):
                data_per_field[field] = data_per_field.get(field, []) + data.get(f"values_{field_leaf}", [])
    return data_per_field, None

def get_stats(k, field, rounding, t_i, t_f, programids, passed_filters=None, saved=None):
    start = time.time()
    values_all, error = get_values_batch(k, field, rounding, t_i, t_f, programids)
    if error or values_all is None:
        return None, error

    if len(values_all) == 0:
        return None, "No data found"

    if passed_filters:
        print(f"Getting values for {len(passed_filters)} alerts that passed filters...")
        values_passed_filters, error = get_values_batch(k, field, rounding, t_i, t_f, programids, passed_filters)
        if error or values_passed_filters is None:
            return None, error
    if saved:
        print(f"Getting values for {len(saved)} saved alerts...")
        values_saved, error = get_values_batch(k, field, rounding, t_i, t_f, programids, saved)
        if error or values_saved is None:
            return None, error
    end = time.time()
    print(f"Queries took {end - start:.2f} seconds")
    stats_per_field = {}
    # if one of the field_leaf is neargaia, we want to filter out values where the gaia distance is too large in the negative direction
    # basically when that distance is missing, the value is set to -999, so we want to filter those out
    # find the indexes where the gaia distance is not -999 and remove the corresponding values from all the other fields
    # if "neargaia" in values_all:
    #     gaia_missing_indexes = np.where(values_all["neargaia"] == -999)[0]
    #     values_all = {k: v[~gaia_missing_indexes] for k, v in values_all.items()}
    # if passed_filters and "neargaia" in values_passed_filters:
    #     gaia_missing_indexes = np.where(values_passed_filters["neargaia"] == -999)[0]
    #     values_passed_filters = {k: v[~gaia_missing_indexes] for k, v in values_passed_filters.items()}
    # if saved and "neargaia" in values_saved:
    #     gaia_missing_indexes = np.where(values_saved["neargaia"] == -999)[0]
    #     values_saved = {k: v[~gaia_missing_indexes] for k, v in values_saved.items()}

    for field_leaf, values in values_all.items():
        stats_per_field[field_leaf] = {
            "all": {
                "min": np.min(values),
                "max": np.max(values),
                "avg": np.mean(values),
                "median": np.median(values),
                "std": np.std(values),
                "total": len(values),
                "values": values
            }
        }
        if passed_filters:
            stats_per_field[field_leaf]["passed_filters"] = {
                "min": np.min(values_passed_filters[field_leaf]),
                "max": np.max(values_passed_filters[field_leaf]),
                "avg": np.mean(values_passed_filters[field_leaf]),
                "median": np.median(values_passed_filters[field_leaf]),
                "std": np.std(values_passed_filters[field_leaf]),
                "total": len(values_passed_filters[field_leaf]),
                "values": values_passed_filters[field_leaf]
            }
        if saved:
            stats_per_field[field_leaf]["saved"] = {
                "min": np.min(values_saved[field_leaf]),
                "max": np.max(values_saved[field_leaf]),
                "avg": np.mean(values_saved[field_leaf]),
                "median": np.median(values_saved[field_leaf]),
                "std": np.std(values_saved[field_leaf]),
                "total": len(values_saved[field_leaf]),
                "values": values_saved[field_leaf]
            }
    return stats_per_field, None

def plot_histogram(stats_per_field, nb_bins=100):
    fig, axes = plt.subplots(3, len(stats_per_field), figsize=(15, 8))
    row = 0
    column = 0
    for field, data in stats_per_field.items():
        max_all = max(data['all']['max'], data['passed_filters']['max'], data['saved']['max'])
        min_all = min(data['all']['min'], data['passed_filters']['min'], data['saved']['min'])
        for key, color in zip(['all', 'passed_filters', 'saved'], ['b', 'r', 'g']):
            if key in data:
                binwidth = (max(data[key]['values']) - min(data[key]['values'])) / min(nb_bins, len(data[key]['values']))
                bins = np.arange(min(data[key]['values']), max(data[key]['values']) + binwidth, binwidth)
                # Create the histogram
                axes[column][row].hist(data[key]['values'], bins=bins, alpha=0.8, edgecolor='none', color=color)
            # Add a title and labels
            axes[column][row].set_title(f'{key}: Distribution of {field} (total={data[key]["total"]})')
            axes[column][row].set_ylabel('Number of alerts')
            axes[column][row].set_xlabel(str(field))
            # we set the x axis to be the same for all plots
            axes[column][row].set_xlim(min_all, max_all)
            column += 1
        row += 1
        column = 0

    fig.tight_layout()
    plt.show()

def plot_corner(stats_per_field):
    df = pd.DataFrame()
    # add one columns for each field
    for field, data in stats_per_field.items():
        df[field] = data['all']['values']
    # normalize the values between 0 and 1
    for field in df.columns:
        df[field] = (df[field] - df[field].min()) / (df[field].max() - df[field].min())
    figure = corner.corner(df, color='b')
    if 'passed_filters' in stats_per_field[list(stats_per_field.keys())[0]]:
        df = pd.DataFrame()
        # add one columns for each field
        for field, data in stats_per_field.items():
            df[field] = data['passed_filters']['values']

        # normalize the values between 0 and 1, using the max and min of the 'all' values
        for field in df.columns:
            df[field] = (df[field] - stats_per_field[field]['all']['min']) / (stats_per_field[field]['all']['max'] - stats_per_field[field]['all']['min'])
        figure = corner.corner(df, fig=figure, color='r')
    if 'saved' in stats_per_field[list(stats_per_field.keys())[0]]:
        df = pd.DataFrame()
        # add one columns for each field
        for field, data in stats_per_field.items():
            df[field] = data['saved']['values']

        # normalize the values between 0 and 1, using the max and min of the 'all' values
        for field in df.columns:
            df[field] = (df[field] - stats_per_field[field]['all']['min']) / (stats_per_field[field]['all']['max'] - stats_per_field[field]['all']['min'])
        figure = corner.corner(df, fig=figure, color='g')
    plt.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query Kowalski for stats")
    parser.add_argument('--features', type=str, default='candidate.magpsf', help='Metadata field to query')
    parser.add_argument('--rounding', type=int, default=None, help='Rounding for the field, if any')
    parser.add_argument('--programids', type=str, default='1', help='Program IDs to query')
    parser.add_argument('--plot', type=str_to_bool, nargs='?', const=True, default=False, help='Plot the histogram')
    parser.add_argument('--nb_bins', type=float, default=200, help='Number of bins for the histogram')
    parser.add_argument('--start', type=str, default=Time.now().iso, help='Start time for the query')
    parser.add_argument('--nb_days', type=float, default=1.0, help='Number of days to query')
    parser.add_argument('--end', type=str, default=None, help='End time for the query')
    parser.add_argument('--sp_token', type=str, default=None, help='SkyPortal token')
    parser.add_argument('--sp_groupIDs', type=str, default=None, help='SkyPortal group IDs')
    parser.add_argument('--sp_filterIDs', type=str, default=None, help='SkyPortal group IDs')
    parser.add_argument('--k_token', type=str, default=None, help='Kowalski token')
    args = parser.parse_args()

    if not args.k_token:
        print("No Kowalski token provided")
        exit(1)
    if not args.sp_token:
        print("No SkyPortal token provided")
        exit(1)

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
    except Exception as e:
        print(f"Failed to connect to Kowalski: {e}")
        exit(1)

    fields = args.features
    rounding = args.rounding
    programids = args.programids
    try:
        fields = list(map(str, fields.split(',')))
    except ValueError:
        print(f"Invalid fields: {fields}")
        exit(1)
    try:
        programids = list(map(int, programids.split(',')))
    except ValueError:
        print(f"Invalid programids: {programids}")
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

    if args.sp_token and (args.sp_groupIDs or args.sp_filterIDs):
        if args.sp_groupIDs:
            try:
                args.sp_groupIDs = list(map(int, args.sp_groupIDs.split(',')))
            except ValueError:
                print(f"Invalid groupIDs: {args.sp_groupIDs}")
                exit(1)
        if args.sp_filterIDs:
            try:
                args.sp_filterIDs = list(map(int, args.sp_filterIDs.split(',')))
            except ValueError:
                print(f"Invalid filterIDs: {args.sp_filterIDs}")
                exit(1)

        # FETCH ALERTS THAT PASSED FILTERS FROM SKYPORTAL (SP)
        start_timer = arrow.now()
        filename = f"cache/candidates_{args.sp_groupIDs}_{args.sp_filterIDs}_{t_i}_{t_f}.txt"
        file_exists = os.path.exists(filename)
        if file_exists:
            with open(filename,'r') as f:
                    candidates = f.read().splitlines()
            print(f"Loaded candidates from cache: {filename}...")
        else:
            candidates, error= get_candidates_from_skyportal(t_i, t_f, args.sp_groupIDs, args.sp_filterIDs, args.sp_token, saved=False)
            if error:
                print(f"Failed to get candidates from SkyPortal: {error}")
                exit(1)
            print(f"Got {len(candidates)} candidates from SkyPortal.")
        end_timer = arrow.now()
        if not file_exists:
            if not os.path.exists("cache"):
                os.makedirs("cache")
            with open(filename, 'w') as f:
                for candidate in candidates:
                    f.write("%s\n" % candidate)
            print(f"SkyPortal query for candidates that passed filter(s) took {end_timer - start_timer}")
        end_timer = arrow.now()

        # FETCH ALERTS THAT WERE SAVED AS SOURCES FROM SP
        start_timer = arrow.now()
        filename = f"cache/candidates_saved_{args.sp_groupIDs}_{args.sp_filterIDs}_{t_i}_{t_f}.txt"
        file_exists = os.path.exists(filename)
        if file_exists:
            with open(filename,'r') as f:
                    saved = f.read().splitlines()
            print(f"Loaded saved alerts from cache: {filename}...")
        else:
            saved, error= get_candidates_from_skyportal(t_i, t_f, args.sp_groupIDs, args.sp_filterIDs, args.sp_token, saved=True)
            if error:
                print(f"Failed to get saved alerts from SkyPortal: {error}")
                exit(1)
            print(f"Got {len(saved)} saved alerts from SkyPortal.")
        end_timer = arrow.now()
        if not file_exists:
            if not os.path.exists("cache"):
                os.makedirs("cache")
            with open(filename, 'w') as f:
                for candidate in saved:
                    f.write("%s\n" % candidate)
            print(f"SkyPortal query for saved alerts took {end_timer - start_timer}")


        # next we run the same query, but with savedStatus=savedToAllSelected to get the subset that was saved to the groups

    print(f"Querying Kowalski for stats for {fields} from {t_i} to {t_f} for programids {programids} {'with rounding to ' + str(rounding) if rounding else ''}...")
    stats_per_field, error = get_stats(k, fields, rounding, t_i, t_f, programids, passed_filters=candidates, saved=saved)
    if error:
        print(f"Failed to get stats for {fields}: {error}")
        exit(1)

    for field, stats in stats_per_field.items():
        # print the keys of the stats
        print(f"\nStats for {field}:")
        print(f"  All alerts:")
        print(f"    min: {stats['all']['min']}")
        print(f"    max: {stats['all']['max']}")
        print(f"    average: {stats['all']['avg']}")
        print(f"    median: {stats['all']['median']}")
        print(f"    standard deviation: {stats['all']['std']}")
        if 'passed_filters' in stats:
            print(f"  Alerts that passed filters:")
            print(f"    min: {stats['passed_filters']['min']}")
            print(f"    max: {stats['passed_filters']['max']}")
            print(f"    average: {stats['passed_filters']['avg']}")
            print(f"    median: {stats['passed_filters']['median']}")
            print(f"    standard deviation: {stats['passed_filters']['std']}")
        if 'saved' in stats:
            print(f"  Saved alerts:")
            print(f"    min: {stats['saved']['min']}")
            print(f"    max: {stats['saved']['max']}")
            print(f"    average: {stats['saved']['avg']}")
            print(f"    median: {stats['saved']['median']}")
            print(f"    standard deviation: {stats['saved']['std']}")


    print(f"Total alerts: {stats_per_field[list(stats_per_field.keys())[0]]['all']['total']}")
    if 'passed_filters' in stats_per_field[list(stats_per_field.keys())[0]]:
        print(f"Total alerts that passed filters: {stats_per_field[list(stats_per_field.keys())[0]]['passed_filters']['total']}")
    if 'saved' in stats_per_field[list(stats_per_field.keys())[0]]:
        print(f"Total saved alerts: {stats_per_field[list(stats_per_field.keys())[0]]['saved']['total']}")

    if args.plot:
      plot_histogram(stats_per_field, nb_bins=args.nb_bins)

      plot_corner(stats_per_field)
