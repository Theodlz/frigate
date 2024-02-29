import argparse
import multiprocessing
import os

import numpy as np
from astropy.time import Time

from frigate.utils.datasets import validate_output_options


def str_to_bool(value):
    if isinstance(value, bool):
        return value
    if value.lower() in {"false", "f", "0", "no", "n"}:
        return False
    elif value.lower() in {"true", "t", "1", "yes", "y"}:
        return True
    raise ValueError(f"{value} is not a valid boolean value")


def main_parser():
    parser = argparse.ArgumentParser(description="Query Kowalski for stats")
    parser.add_argument(
        "--programids", type=str, default="1,2,3", help="Program IDs to query"
    )
    parser.add_argument(
        "--start",
        type=str,
        default=np.floor(Time.now().jd - 1) + 0.5,
        help="Start time for the query, default to 1 day ago",
    )
    parser.add_argument(
        "--nb_days", type=float, default=1.0, help="Number of days to query"
    )
    parser.add_argument("--end", type=str, default=None, help="End time for the query")
    parser.add_argument("--k_token", type=str, default=None, help="Kowalski token")
    parser.add_argument(
        "--n_threads",
        type=str,
        default=None,
        help="Number of threads to use when parallelizing queries",
    )
    parser.add_argument(
        "--output_format",
        type=str,
        default="parquet",
        help="Output format for the results",
    )
    parser.add_argument(
        "--output_compression",
        type=str,
        default=None,
        help="Output compression for the results",
    )
    parser.add_argument(
        "--output_compression_level",
        type=int,
        default=None,
        help="Output compression level for the results",
    )
    parser.add_argument(
        "--output_directory",
        type=str,
        default="./data",
        help="Output directory for the results",
    )
    parser.add_argument(
        "--low_memory",
        type=str_to_bool,
        default=False,
        help="Use low memory mode, to reduce RAM usage",
    )
    return parser


def main_parser_args():
    args = main_parser().parse_args()

    if not args.k_token:
        # we try to get the token from the environment if it is not provided here
        k_token_env = os.environ.get("KOWALSKI_TOKEN")
        if k_token_env:
            args.k_token = k_token_env
    else:
        # if provided, we add the token in the environment instead
        os.environ["KOWALSKI_TOKEN"] = args.k_token

    # validate the output options
    try:
        validate_output_options(
            args.output_format,
            args.output_compression,
            args.output_compression_level,
            args.output_directory,
        )
    except ValueError as e:
        raise ValueError(f"Invalid output options: {e}")

    # validate the number of threads
    n_threads = args.n_threads
    if n_threads is None:
        n_threads = multiprocessing.cpu_count()
    else:
        n_threads = int(n_threads)
        n_threads = min(n_threads, multiprocessing.cpu_count())
    args.n_threads = n_threads

    # validate the programids
    try:
        programids = list(map(int, args.programids.split(",")))
    except ValueError:
        raise ValueError(f"Invalid programids: {args.programids}")
    args.programids = programids

    # validate the start and end times
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
        t_f = t_i + args.nb_days

    args.start = float(t_i)
    args.end = float(t_f)

    return args
