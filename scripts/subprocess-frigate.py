import argparse
import sys
import os
import subprocess

def loop_parser():
    parser = argparse.ArgumentParser(description='Run frigate with specified parameters.')
    parser.add_argument('--start', nargs='+', required=True, help='List of start values')
    parser.add_argument("--nb_days", type=float, default=1.0, help="Number of days to query")
    parser.add_argument("--sp_token", type=str, default=None, help="Skyportal token")
    parser.add_argument("--k_token", type=str, default=None, help="Kowalski token")
    parser.add_argument("--output_directory", type=str, default="./data", help="Output directory for the results")
    return parser

def loop_parser_args():
    args = loop_parser().parse_args()
    if not args.k_token:
        # we try to get the token from the environment if it is not provided here
        k_token_env = os.environ.get("KOWALSKI_TOKEN")
        if k_token_env:
            args.k_token = k_token_env
    else:
        # if provided, we add the token in the environment instead
        os.environ["KOWALSKI_TOKEN"] = args.k_token

    if not args.sp_token:
        # we try to get the token from the environment if it is not provided here
        sp_token_env = os.environ.get("SKYPORTAL_TOKEN")
        if sp_token_env:
            args.sp_token = sp_token_env
    else:
        # if provided, we add the token in the environment instead
        os.environ["SKYPORTAL_TOKEN"] = args.sp_token
    return args

args = loop_parser_args()
start_values = args.start
nb_days = args.nb_days
sp_token = args.sp_token
k_token = args.k_token
output_directory = args.output_directory


for start in start_values:
    try:
        result = subprocess.run([
            sys.executable, '-m', 'frigate', 
            f'--start={start}', 
            f'--nb_days={nb_days}', 
            f'--sp_token={sp_token}', 
            f'--k_token={k_token}', 
            f'--output_directory={output_directory}'
        ])
        result.check_returncode()
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while running the command for start value {start}: {e}")


