import argparse
import sys

from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument(
    "--data_root", 
    type     = str,
    required = True,
    help     = "datasets directory")
parser.add_argument(
    "--log_id",
    type     = str,
    required = True,
    help     = "id of log to visualize"
)

def get_args():
    args = parser.parse_args(sys.argv[1:])
    return (Path(args.data_root), Path(args.log_id))