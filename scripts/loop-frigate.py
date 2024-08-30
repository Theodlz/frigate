from frigate.__main__ import process_candidates
from frigate.utils.parsers import main_parser_args

args = main_parser_args()
start_values = [args.start]

for start in start_values:
    try:
        args.start = float(start)
        args.end = args.start + args.nb_days
        process_candidates(args)
    except Exception as e:
        print(f"Error occurred while running the command for start value {start}: {e}")
