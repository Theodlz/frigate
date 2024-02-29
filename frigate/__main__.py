import argparse
import multiprocessing
import os

import numpy as np
from astropy.time import Time

from frigate.utils.kowalski import get_candidates_from_kowalski
from frigate.utils.datasets import save_dataframe, validate_output_options


def str_to_bool(value):
    if isinstance(value, bool):
        return value
    if value.lower() in {"false", "f", "0", "no", "n"}:
        return False
    elif value.lower() in {"true", "t", "1", "yes", "y"}:
        return True
    raise ValueError(f"{value} is not a valid boolean value")


if __name__ == "__main__":
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
    args = parser.parse_args()

    # VALIDATE ARGUMENTS

    # validate the Kowalski token
    if not args.k_token:
        print("No Kowalski token provided")
        exit(1)
    # add the token in the environment
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
        print(e)
        exit(1)

    # validate the number of threads
    n_threads = args.n_threads
    if n_threads is None:
        n_threads = multiprocessing.cpu_count()
    else:
        n_threads = int(n_threads)
        n_threads = min(n_threads, multiprocessing.cpu_count())

    # validate the programids
    try:
        programids = list(map(int, args.programids.split(",")))
    except ValueError:
        print(f"Invalid programids: {args.programids}")
        exit(1)

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

    t_i = float(t_i)
    t_f = float(t_f)

    # GET CANDIDATES FROM KOWALSKI
    candidates, err = get_candidates_from_kowalski(
        t_i, t_f, programids, n_threads=n_threads
    )
    if err:
        print(err)
        exit(1)

    # SAVE CANDIDATES TO DISK

    # filename: <start>_<end>_<programids>.<output_format>
    filename = f"{t_i}_{t_f}_{'_'.join(map(str, programids))}"
    save_dataframe(
        df=candidates,
        filename=filename,
        output_format=args.output_format,
        output_compression=args.output_compression,
        output_compression_level=args.output_compression_level,
        output_directory=args.output_directory,
    )

    print(f"Saved candidates to {os.path.join(args.output_directory, filename)}")
