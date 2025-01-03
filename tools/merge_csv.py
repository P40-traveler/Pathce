#!/usr/bin/env python
import sys
import json
import argparse
import csv
import pathlib

parser = argparse.ArgumentParser(
    prog=sys.argv[0],
    description=
    "Merge multiple separate CSV edge lists to a single CSV edge list, in CEG format."
)
parser.add_argument("-d",
                    "--dataset",
                    help="Specify the dataset dir",
                    required=True)
parser.add_argument("-s",
                    "--schema",
                    help="Specify the gCard schema path",
                    required=True)
parser.add_argument("-o",
                    "--output",
                    help="Specify the output file",
                    required=True)
args = parser.parse_args()

with open(args.schema) as f:
    schema = json.load(f)

output = open(args.output, "w+")
output = csv.writer(output)

dataset = pathlib.Path(args.dataset)
for label, label_id in schema["edge_labels"].items():
    path = dataset.joinpath(f"{label}.csv")
    with open(path) as f:
        reader = csv.DictReader(f)
        edges = [[record["src"], label_id, record["dst"]] for record in reader]
    output.writerows(edges)
