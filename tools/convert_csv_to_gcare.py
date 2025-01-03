#!/usr/bin/env python
import sys
import json
import argparse
import csv
from pathlib import Path

parser = argparse.ArgumentParser(
    prog=sys.argv[0], description="Convert a CSV dataset to G-CARE format")
parser.add_argument("-s",
                    "--schema",
                    help="Specify the schema path",
                    required=True)
parser.add_argument("-d",
                    "--dataset",
                    help="Specify the dataset dir",
                    required=True)
parser.add_argument("-o",
                    "--output",
                    help="Specify the output file path",
                    required=True)
args = parser.parse_args()

dataset_dir = Path(args.dataset)
with open(args.schema) as f:
    schema = json.load(f)

with open(args.output, "w") as f:
    writer = csv.writer(f, delimiter=" ", lineterminator="\n")
    writer.writerow(["t", "#", 123])
    vnum = 0
    enum = 0
    for vlabel_name, vlabel_id in schema["vertex_labels"].items():
        with open(dataset_dir.joinpath(f"{vlabel_name}.csv")) as f:
            reader = csv.DictReader(f)
            for row in reader:
                writer.writerow(["v", int(row["id"]), int(vlabel_id)])
                vnum += 1
    for elabel_name, elabel_id in schema["edge_labels"].items():
        with open(dataset_dir.joinpath(f"{elabel_name}.csv")) as f:
            reader = csv.DictReader(f)
            for row in reader:
                writer.writerow(
                    ["e",
                     int(row["src"]),
                     int(row["dst"]),
                     int(elabel_id)])
                enum += 1
    print("vnum:", vnum)
    print("enum:", enum)
