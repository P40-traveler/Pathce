# Copilot Instructions for PathCE-Artifacts

This guide helps AI coding agents work productively in the PathCE-Artifacts codebase, which supports research on path-centric cardinality estimation for subgraph matching. Follow these conventions and workflows for effective contributions.

## Architecture Overview

- **Major Components:**
  - `pathce/`: Rust implementation of PathCE estimator
  - `glogs/`: Rust implementation of GLogS estimator
  - `gcare/`: C++ implementation for G-CARE (SumRDF, WJ)
  - `ceg/`: Java implementation for CEG estimator
  - `color/`, `color_max/`: Julia implementations for Color/ColorMax estimators
  - `tools/`: Python utilities for pattern conversion, visualization, and data preparation
  - `scripts/`: Bash scripts orchestrate builds, dataset serialization, summary construction, and estimation runs
  - `datasets/`, `catalogs/`, `patterns/`, `schemas/`, `graphs/`: Data, results, and configuration files

## Developer Workflows

- **Build Rust Components:**
  - `cd pathce && cargo build -r`
  - `cd glogs/ir && cargo build -r`
- **Build C++ Components:**
  - `cd gcare && mkdir build && cd build && cmake .. -DCMAKE_BUILD_TYPE=Release && make -j`
- **Build Java Components:**
  - Use provided JARs in `ceg/lib/` and scripts in `ceg/`
- **Python Environment:**
  - `python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`
- **Julia Environment:**
  - `julia --project=color` then `Pkg.instantiate()`
- **Run Estimation Pipelines:**
  - Use scripts in `scripts/` (e.g., `scripts/pathce/estimate_heuristic.sh`, `scripts/glogs/estimate.sh`, etc.)
  - Inputs: dataset, summary/catalog, pattern, schema as required
- **Data Preparation:**
  - Use scripts in `tools/` for format conversion and preprocessing

## Project-Specific Conventions

- **Summary Construction:**
  - Summaries/catalogs are built once per dataset and reused for multiple queries
  - CEG estimator is adapted to use GLogS summaries for consistency
- **Naming Patterns:**
  - Catalogs: `catalogs/ldbc/pathce/ldbc_sf{scale}_{maxlen}_{partitions}`
  - Patterns: `patterns/lsqb/q{num}.json`
  - Schemas: `schemas/ldbc/ldbc_glogs_schema.json`
- **Script Usage:**
  - Always activate the correct environment (Python, Julia) before running scripts
  - Most scripts expect positional arguments: dataset scale, summary/catalog path, pattern path, etc.

## Integration Points & Dependencies

- **External Tools:**
  - Rust, C++, Java, Python, Julia required
  - System dependencies: openjdk-8-jdk, protobuf-compiler, clang, cmake, uuid
- **Inter-component Communication:**
  - Data flows via serialized files (catalogs, summaries, patterns)
  - Scripts coordinate execution across languages

## Key Files & Directories

- `README.md`: High-level instructions and workflow examples
- `scripts/`: Entry points for all major workflows
- `tools/`: Data conversion and utility scripts
- `pathce/`, `glogs/`, `gcare/`, `ceg/`, `color/`, `color_max/`: Estimator implementations
- `datasets/`, `catalogs/`, `patterns/`, `schemas/`, `graphs/`: Data and configuration

## Example Workflow

1. Build estimator (e.g., PathCE)
2. Serialize dataset
3. Build summary/catalog
4. Run estimation script with pattern and summary

---

If any section is unclear or missing details, please provide feedback to improve these instructions.
