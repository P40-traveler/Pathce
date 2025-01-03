#!/usr/bin/env python
import sys
import json
import argparse

parser = argparse.ArgumentParser(
    prog=sys.argv[0], description="Generate SQL DDL from a given gCard schema")
parser.add_argument("-s",
                    "--schema",
                    help="Specify the schema path",
                    required=True)
args = parser.parse_args()

with open(args.schema) as f:
    schema = json.load(f)

sql = []
for vertex_label in schema["vertex_labels"]:
    sql.append(
        f"create table {vertex_label} as from read_csv('{vertex_label}.csv')")

for edge_label in schema["edge_labels"]:
    sql.append(
        f"create table {edge_label} as from read_csv('{edge_label}.csv')")

sql.append("")

sql = ";\n".join(sql)
print(sql)
