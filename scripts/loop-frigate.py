import traceback
from tqdm import tqdm
from frigate.__main__ import process_candidates
from frigate.utils.parsers import main_parser_args

args = main_parser_args()
start_values = args.start
if isinstance(start_values, (int, str, float)):
    start_values = [start_values]

for start in tqdm(start_values, desc="Processing nights"):
    try:
        args.start = float(start)
        args.end = args.start + args.nb_days
        args.verbose = False
        process_candidates(args)
    except Exception as e:
        traceback.print_exc()
        print(f"Error occurred while running the command for start value {start}: {e}")
