import json
import argparse
import os


def convert(query):
    x = []
    where = []
    triples = []
    for e in query["edges"]:
        src = e["src"]
        dst = e["dst"]
        label_id = e["label_id"]
        predicate = f"http://ex.org/0{label_id}"
        x.append(predicate)
        where.append(f"?o{src} <{predicate}> ?o{dst} .")
        triples.append([f"?o{src}", f"<{predicate}>", f"?o{dst}"])
    query = f"SELECT * FROM WHERE {{ {" ".join(where)} }}"
    output = {
        "x": x,
        "y": int(query["count"]) if "count" in query else 0,
        "query": query,
        "triples": triples,
    }
    return output


parser = argparse.ArgumentParser(
    description="Convert PathCE query to GNCE query")
parser.add_argument("-i", "--input")
parser.add_argument("-o", "--output")

{
    "x": ["http://ex.org/024", "http://ex.org/00"],
    "y":
    6380,
    "query":
    "SELECT * WHERE {  ?o0 <http://ex.org/024> ?o1 .  ?o1 <http://ex.org/00> ?o2 .  }",
    "triples": [["?o0", "<http://ex.org/024>", "?o1"],
                ["?o1", "<http://ex.org/00>", "?o2"]]
}

args = parser.parse_args()
outputs = []
if os.path.isdir(args.input):
    for entry in os.scandir(args.input):
        if not entry.name.endswith(".json"):
            continue
        with open(entry.path) as f:
            query = json.load(f)
        query = convert(query)
        outputs.append(query)
else:
    with open(args.input) as f:
        query = json.load(f)
        query = convert(query)
        outputs.append(query)

with open(args.output, "w") as f:
    json.dump(outputs, f, indent=4)