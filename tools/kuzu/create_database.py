#!/usr/bin/env python
import sys
import json
import argparse
import kuzu
import pathlib

parser = argparse.ArgumentParser(
    prog=sys.argv[0],
    description="Create a Kuzu database based on the given dataset and schema")
parser.add_argument("-d",
                    "--dataset",
                    help="Specify the dataset directory",
                    required=True)
parser.add_argument("-s",
                    "--schema",
                    help="Specify the gCard schema path",
                    required=True)
parser.add_argument("-o",
                    "--output",
                    help="Specify the output directory",
                    required=True)
args = parser.parse_args()

with open(args.schema) as f:
    schema = json.load(f)

db = kuzu.Database(args.output)
conn = kuzu.Connection(db)

vertex_map = {}
edge_map = {}
for vertex_label, vertex_label_id in schema["vertex_labels"].items():
    ddl = f"create node table {vertex_label} (id uint64, primary key (id))"
    conn.execute(ddl)
    vertex_map[vertex_label_id] = vertex_label

for edge_label, edge_label_id in schema["edge_labels"].items():
    edge_map[edge_label_id] = edge_label

for edge in schema["edges"]:
    edge_label_id = edge["label"]
    src_id = edge["from"]
    dst_id = edge["to"]
    card = edge["card"]
    src_label = vertex_map[src_id]
    dst_label = vertex_map[dst_id]
    edge_label = edge_map[edge_label_id]
    if card == "ManyToMany":
        card = "MANY_MANY"
    elif card == "ManyToOne":
        card = "MANY_ONE"
    elif card == "OneToMany":
        card = "ONE_MANY"
    elif card == "OneToOne":
        card = "ONE_ONE"
    else:
        assert False
    ddl = f"create rel table {edge_label} (from {src_label} to {dst_label}, {card})"
    conn.execute(ddl)

dataset = pathlib.Path(args.dataset)
for vertex_label in schema["vertex_labels"]:
    path = dataset.joinpath(f"{vertex_label}.csv")
    cypher = f"copy {vertex_label} from \"{path}\" (header=true)"
    conn.execute(cypher)

for edge_label in schema["edge_labels"]:
    path = dataset.joinpath(f"{edge_label}.csv")
    cypher = f"copy {edge_label} from \"{path}\" (header=true)"
    conn.execute(cypher)
