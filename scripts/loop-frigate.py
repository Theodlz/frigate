import argparse
import sys
import subprocess

parser = argparse.ArgumentParser(description='Run frigate with specified parameters.')
parser.add_argument('--start', nargs='+', required=True, help='List of start values')
parser.add_argument("--nb_days", type=float, default=1.0, help="Number of days to query")
parser.add_argument("--sp_token", type=str, default=None, help="Skyportal token")
parser.add_argument("--k_token", type=str, default=None, help="Kowalski token")
parser.add_argument("--output_directory", type=str, default="./data", help="Output directory for the results")

args = parser.parse_args()

start_values = args.start
nb_days = args.nb_days
sp_token = args.sp_token
k_token = args.k_token
output_directory = args.output_directory

# Iterate over each start value
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


