#!/usr/bin/env python
import sys
import json
import argparse
import kuzu

parser = argparse.ArgumentParser(prog=sys.argv[0],
                                 description="Execute a given query with Kuzu")
parser.add_argument("-p",
                    "--pattern",
                    help="Specify the pattern path",
                    required=True)
parser.add_argument("-s",
                    "--schema",
                    help="Specify the gCard schema path",
                    required=True)
parser.add_argument("-d",
                    "--database",
                    help="Specify the database path",
                    required=True)
parser.add_argument("-v",
                    "--verbose",
                    help="Specify whether to show the cypher query",
                    action="store_true")
args = parser.parse_args()

with open(args.schema) as f:
    schema = json.load(f)

with open(args.pattern) as f:
    pattern = json.load(f)

db = kuzu.Database(args.database)
conn = kuzu.Connection(db)

vertices = {}
for vertex in pattern["vertices"]:
    tag_id = vertex["tag_id"]
    label_id = vertex["label_id"]
    vertices[tag_id] = label_id

edges = {}
for edge in pattern["edges"]:
    tag_id = edge["tag_id"]
    src_id = edge["src"]
    dst_id = edge["dst"]
    label_id = edge["label_id"]
    edges[tag_id] = (src_id, label_id, dst_id)

vertex_label_map = {}
for vertex_label, vertex_label_id in schema["vertex_labels"].items():
    vertex_label_map[vertex_label_id] = vertex_label

edge_label_map = {}
for edge_label, edge_label_id in schema["edge_labels"].items():
    edge_label_map[edge_label_id] = edge_label

clauses = []
for vertex_tag_id, vertex_label_id in vertices.items():
    vertex_label = vertex_label_map[vertex_label_id]
    clauses.append(f"(v{vertex_tag_id}: {vertex_label})")

for edge_tag_id, (src_tag_id, edge_label_id, dst_tag_id) in edges.items():
    edge_label = edge_label_map[edge_label_id]
    clauses.append(
        f"(v{src_tag_id})-[e{edge_tag_id}: {edge_label}]->(v{dst_tag_id})")

clauses = ", ".join(clauses)
cypher = f"match {clauses} return count(*)"

if args.verbose:
    print(cypher)

results = conn.execute(cypher)
while results.has_next():
    result = [f"{r}" for r in results.get_next()]
    result = ",".join(result)
    print(result)
