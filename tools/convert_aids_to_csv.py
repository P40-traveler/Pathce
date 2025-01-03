#!/usr/bin/env python
import sys
import json
import argparse
import csv
from tqdm import tqdm
from pathlib import Path

parser = argparse.ArgumentParser(
    prog=sys.argv[0],
    description="Convert AIDS dataset in G-CARE format to CSV format")
parser.add_argument("-i",
                    "--input",
                    help="Specify the input txt file",
                    required=True)
parser.add_argument("-s",
                    "--schema",
                    help="Specify the output schema path",
                    required=True)
parser.add_argument("-d",
                    "--dataset",
                    help="Specify the output dataset dir",
                    required=True)
parser.add_argument("-t",
                    "--type",
                    help="Specify the conversion type",
                    default="regular",
                    choices=["regular", "merge", "extend"])
args = parser.parse_args()

vertices = {}
vertices_per_label = {}
edges = {}
with open(args.input) as f:
    reader = csv.reader(f, delimiter=" ", lineterminator="\n", strict=False)
    next(reader)
    for row in reader:
        if row[0] == "v":
            assert len(row) == 3, "invalid vertex row"
            id = int(row[1])
            label = int(row[2])
            assert id not in vertices, "duplicate vertex id"
            vertices[id] = label
            if label not in vertices_per_label:
                vertices_per_label[label] = []
            vertices_per_label[label].append(id)
        elif row[0] == "e":
            assert len(row) == 4, "invalid edge row"
            src = int(row[1])
            dst = int(row[2])
            label = int(row[3])
            if label not in edges:
                edges[label] = []
            edges[label].append((src, dst))
        else:
            assert False, "invalid object type"

dataset = Path(args.dataset)
dataset.mkdir(exist_ok=True)


def convert_regular():
    edges_partitioned = {}
    for el, es in edges.items():
        for s, t in tqdm(es):
            sl = vertices[s]
            tl = vertices[t]
            if (sl, el, tl) not in edges_partitioned:
                edges_partitioned[(sl, el, tl)] = []
            edges_partitioned[(sl, el, tl)].append((s, t))

    for (sl, el, tl), es in edges_partitioned.items():
        path = dataset / f"{sl}_{el}_{tl}.csv"
        with open(path, "w") as f:
            print(f"write {path}")
            writer = csv.writer(f, delimiter=",", lineterminator="\n")
            writer.writerow(["src", "dst"])
            writer.writerows(es)

    # remove orphan vertex labels
    orphan_vertex_labels = []
    for vl in vertices_per_label.keys():
        orphan = True
        for sl, el, tl in edges_partitioned.keys():
            if sl == vl or tl == vl:
                orphan = False
                break
        if orphan:
            orphan_vertex_labels.append(vl)
    for vl in orphan_vertex_labels:
        vertices_per_label.pop(vl)

    for vl, ids in vertices_per_label.items():
        path = dataset / f"{vl}.csv"
        with open(path, "w") as f:
            print(f"write {path}")
            writer = csv.writer(f, delimiter=",", lineterminator="\n")
            writer.writerow(["id"])
            writer.writerows([[id] for id in ids])

    schema = {}
    schema["vertex_labels"] = {
        f"{vl}": vl
        for vl in sorted(vertices_per_label.keys())
    }
    schema["edge_labels"] = {
        f"{sl}_{el}_{tl}": i
        for i, (sl, el, tl) in enumerate(sorted(edges_partitioned.keys()))
    }
    schema["vertices"] = [{
        "label": vl,
        "discrete": False
    } for vl in sorted(vertices_per_label.keys())]
    schema["edges"] = [{
        "card": "ManyToMany",
        "from": sl,
        "label": i,
        "to": tl
    } for i, (sl, _, tl) in enumerate(sorted(edges_partitioned.keys()))]

    with open(args.schema, "w") as f:
        print(f"write {args.schema}")
        json.dump(schema, f)


def convert_merge():
    for el, es in edges.items():
        path = dataset / f"{el}.csv"
        with open(path, "w") as f:
            print(f"write {path}")
            writer = csv.writer(f, delimiter=",", lineterminator="\n")
            writer.writerow(["src", "dst"])
            writer.writerows(es)

    path = dataset / "vertex.csv"
    with open(path, "w") as f:
        print(f"write {path}")
        writer = csv.writer(f, delimiter=",", lineterminator="\n")
        writer.writerow(["id"])
        for id in vertices.keys():
            writer.writerow([id])

    schema = {}
    schema["vertex_labels"] = {"vertex": 0}
    schema["edge_labels"] = {f"{el}": el for el in sorted(edges.keys())}
    schema["vertices"] = [{"label": 0, "discrete": False}]
    schema["edges"] = [{
        "card": "ManyToMany",
        "from": 0,
        "label": el,
        "to": 0
    } for el in sorted(edges.keys())]

    with open(args.schema, "w") as f:
        print(f"write {args.schema}")
        json.dump(schema, f)

if args.type == "regular":
    convert_regular()
else:
    convert_merge()