#!/usr/bin/env python
import sys
import json
import argparse
import csv
import os

parser = argparse.ArgumentParser(
    prog=sys.argv[0],
    description=
    "Merge a directory of patterns (in JSON format) to a single CEG pattern file"
)
parser.add_argument("-p",
                    "--pattern",
                    help="Specify the pattern directory",
                    required=True)
parser.add_argument("-o",
                    "--output",
                    help="Specify the output path",
                    required=True)
args = parser.parse_args()

patterns = {}
for entry in os.scandir(args.pattern):
    if not entry.is_file() or not entry.name.endswith(".json"):
        continue
    with open(entry.path) as f:
        pattern = json.load(f)
    patterns[entry.name] = pattern

rows = []
for _, p in sorted(patterns.items(), key=lambda item: item[0]):
    p_edges = sorted(p["edges"], key=lambda e: (e["src"], e["dst"]))
    edges = []
    for e in p_edges:
        src = e["src"]
        dst = e["dst"]
        edges.append(f"{src}-{dst}")
    edges = ";".join(edges)
    labels = []
    for e in p_edges:
        label = e["label_id"]
        labels.append(f"{label}")
    labels = "->".join(labels)
    rows.append((edges, labels, 0))

output = open(args.output, "w+")
writer = csv.writer(output, lineterminator="\n")
writer.writerows(rows)
