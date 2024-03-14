# we want to open a given dataset and compute some statistics on a set of columns
import os
import json

from frigate.utils.datasets import load_dataframe, compute_column_stats
from frigate.utils.parsers import stats_parser_args

if __name__ == "__main__":
    args = stats_parser_args()
    elements = os.path.split(args.dataset_path)
    dataset = elements[-1]
    path = elements[0]

    df = load_dataframe(filename=dataset, directory=path)
    # DEBUG: print the first 10 rows of the dataframe
    print(df.head(10))

    # print the total number of unique filters
    unique_filter_ids = []
    for index, row in df.iterrows():
        unique_filter_ids.extend(row["passed_filters"])
    unique_filter_ids = list(set(unique_filter_ids))
    print(f"\nTotal number of unique filters: {len(unique_filter_ids)}")

    # get the candidates that passed at least one filter, i.e the ones where len(passed_filters) > 0
    candidates_passed_filters = df[df["passed_filters"].apply(lambda x: len(x) > 0)]
    print(
        f"Number of candidates that passed at least one filter: {len(candidates_passed_filters)}"
    )

    # print the total number of candidates passing filters per filter, so basically the sum of all the passed_filters
    total = 0
    for index, row in df.iterrows():
        total += len(row["passed_filters"])

    print(f"Total number of candidates passing any filters: {total}")

    for column in args.columns:
        stats = compute_column_stats(df, column)
        print(f"\nStats for column {column}:")
        print(json.dumps(stats, indent=4))
