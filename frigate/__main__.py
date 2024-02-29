from frigate.utils.datasets import save_dataframe
from frigate.utils.kowalski import get_candidates_from_kowalski
from frigate.utils.parsers import main_parser_args


def str_to_bool(value):
    if isinstance(value, bool):
        return value
    if value.lower() in {"false", "f", "0", "no", "n"}:
        return False
    elif value.lower() in {"true", "t", "1", "yes", "y"}:
        return True
    raise ValueError(f"{value} is not a valid boolean value")


if __name__ == "__main__":
    # PARSE COMMAND LINE ARGUMENTS
    args = main_parser_args()

    # GET CANDIDATES FROM KOWALSKI
    candidates, err = get_candidates_from_kowalski(
        args.start, args.end, args.programids, n_threads=args.n_threads
    )
    if err:
        print(err)
        exit(1)

    # SAVE CANDIDATES TO DISK
    # filename: <start>_<end>_<programids>.<output_format> (ext added by save_dataframe function)
    filename = f"{args.start}_{args.end}_{'_'.join(map(str, args.programids))}"
    filepath = save_dataframe(
        df=candidates,
        filename=filename,
        output_format=args.output_format,
        output_compression=args.output_compression,
        output_compression_level=args.output_compression_level,
        output_directory=args.output_directory,
    )

    print(f"Saved candidates to {filepath}")
