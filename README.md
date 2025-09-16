# PathCE-Artifacts

This repository contains artifacts for _Path-centric Cardinality Estimation for Subgraph Matching_.

## Project Structure

```
.
├── README.md               # the README file
├── catalogs                # contains generated catalog files
├── ceg                     # contains the code for CEG
├── color                   # contains the code for Color & ColorMax
├── datasets                # contains the experiment datasets
├── pathce                  # contains the code for PathCE
├── gcare                   # contains the code for G-CARE (SumRDF, WJ, etc.)
├── glogs                   # contains the code for GLogS
├── graphs                  # contains serialized graph files
├── patterns                # contains the query patterns
├── requirements.txt        # pip requirements file
├── rust-toolchain.toml     # Rust toolchain specification
├── schemas                 # contains the schemas for datasets
├── scripts                 # contains scripts to run baselines
├── tools                   # contains utilities like pattern converter, pattern visualizer, etc.
```

## Environment

1. Ubuntu 22.04 or newer.
2. Python 3.10 or newer.
3. Make sure you have the following packages installed:

- openjdk-8-jdk
- protobuf-compiler
- build-essential
- clang
- cmake
- python3-pip
- uuid

4. Make sure you have Rust installed (see https://www.rust-lang.org/tools/install for more details)
5. Make sure you have set up and activate a python virtual environment:

```bash
$ python3 -m venv .venv
$ source .venv/bin/activate
$ pip install -r requirements.txt
```

6. Make sure you have Julia installed (see https://julialang.org/downloads/ for more details)

## Get Started

Here we provide instructions for running the estimators on LDBC-0.003.

### PathCE

1. Build PathCE:

```bash
$ cd pathce
$ cargo build -r
$ cd ..
```

2. Serialize the LDBC-0.003 dataset:

```bash
$ scripts/pathce/build_ldbc_graph.sh 0.003
```

3. Build the PSG with maximal path length of 3 and 200 vertex partitions, in 32 threads:

```bash
$ scripts/pathce/build_ldbc_catalog.sh 0.003 4 3 4 200
```

4. Estimate LSQB q1 using the built PSG:

```bash
$ scripts/pathce/estimate_heuristic.sh catalogs/ldbc/pathce/ldbc_sf0.003_3_4_200/ 3 4 patterns/lsqb/q1.json
```

You can see the output like:

```
7412,0.009125875
```

The first value is the estimate and the second one is the estimation latency.

### GLogS

1. Build GLogS:

```bash
$ cd glogs/ir
$ cargo build -r
$ cd ../..
```

2. Serialize the LDBC-0.003 dataset:

```bash
$ scripts/glogs/build_ldbc_graph.sh 0.003
```

3. Build the GLogS summary in 32 threads:

```bash
$ scripts/glogs/build_ldbc_catalog.sh 0.003 32
```

4. Estimate LSQB q1 using the built summary:

```bash
$ scripts/glogs/estimate.sh catalogs/ldbc/glogs/ldbc_sf0.003.bincode patterns/lsqb/q1.json
```

### G-CARE

1. Build G-CARE

```bash
$ cd gcare
$ mkdir build && cd build
$ cmake .. -DCMAKE_BUILD_TYPE=Release
$ make -j
$ cd ../..
```

2. Build SumRDF/WJ summary:

```bash
$ scripts/gcare/build_ldbc_summary METHOD 0.003
```

`METHOD` can be `sumrdf` (SumRDF) or `wj` (WJ).

3. Estimate LSQB q1 using the built summary:

```bash
$ scripts/gcare/estimate.sh METHOD datasets/ldbc/sf0.003 patterns/lsqb/q1.json
```

### CEG

The original version of CEG builds the summary tailored to the given query, which is not in accordance with our assumption that the summary should be built once. To better integrate CEG into our framework, we adapt it to use GLogS' summary.

1. Make sure you have GLogS' summary built.
2. Estimate LSQB q1 using the built summary:

```bash
$ scripts/ceg/estimate.sh catalogs/ldbc/glogs/ldbc_sf0.003.bincode schemas/ldbc/ldbc_glogs_schema.json patterns/lsqb/q1.json
```

### GNCE

We put the code for evaluating GNCE into a seperate repository, which will soon be open source.

### Color & ColorMax

1. Instantiate Julia packages:

```bash
$ julia --project=color
julia> using Pkg;
julia> Pkg.instantiate();
```

2. Build Color or ColorMax summary:

```bash
$ scripts/color/build_ldbc_summary.sh 0.003 # Color
$ scripts/color_max/build_ldbc_summary.sh 0.003 # ColorMax
```

3. Estimate LSQB q1 using the built summary (Ensure you have activated the Python virtual environment since we need to run some Python scripts to transform the query):

```bash
$ scripts/color/estimate.sh catalogs/ldbc/color/ldbc_sf0.003_mix_6_50000.obj patterns/lsqb/q1.json # Color
$ scripts/color_max/estimate.sh catalogs/ldbc/color_max/ldbc_sf0.003_mix_6_50000.obj patterns/lsqb/q1.json # ColorMax
```
