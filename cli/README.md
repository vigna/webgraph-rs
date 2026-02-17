# CLI for WebGraph

[![downloads](https://img.shields.io/crates/d/webgraph-cli)](https://crates.io/crates/webgraph-cli)
[![dependents](https://img.shields.io/librariesio/dependents/cargo/webgraph-cli)](https://crates.io/crates/webgraph-cli/reverse_dependencies)
![GitHub CI](https://github.com/vigna/webgraph-rs/actions/workflows/rust.yml/badge.svg)
![license](https://img.shields.io/crates/l/webgraph-cli)
[![Latest version](https://img.shields.io/crates/v/webgraph-cli.svg)](https://crates.io/crates/webgraph-cli)
[![Documentation](https://docs.rs/webgraph-cli/badge.svg)](https://docs.rs/webgraph-cli)

Command-line interface for the Rust implementation of the [WebGraph framework]
for graph compression.

This crate provides the `webgraph` CLI tool with various subcommands for working
with compressed graphs. The tool supports:

- **Building** accessory data structures (Elias-Fano offsets, DCF)
- **Converting** graphs between formats (arcs, ASCII, endianness)
- **Transforming** graphs (transpose, simplify)
- **Analyzing** graphs (code statistics)
- **Running** algorithms (Layered Label Propagation)
- **Benchmarking** graph operations

Each module corresponds to a group of commands, and each command is
implemented as a submodule.

## Subcommands

- `analyze`: Compute statistics on graphs
- `bench`: Benchmark graph operations
- `build`: Build accessory graph data structures
- `check`: Check coherence of graph files
- `from`: Ingest data into graphs
- `perm`: Create and manipulate permutations
- `run`: Run algorithms on graphs
- `to`: Convert graphs between representations
- `transform`: Apply transformations to graphs

## Separate Binaries

This crate also provides specialized standalone binaries:

- `webgraph-dist`: Tools for computing graph properties based on distances
  (HyperBall, ExactSumSweep)
- `webgraph-sccs`: Compute strongly connected components

## Environment Variables

- `RUST_MIN_STACK`: Minimum thread stack size (in bytes); we suggest
  `RUST_MIN_STACK=8388608` (8MiB)
- `TMPDIR`: Where to store temporary files (potentially very large ones)
- `RUST_LOG`: Configuration for [`env_logger`]


## Acknowledgments

This software has been partially supported by project SERICS (PE00000014) under
the NRRP MUR program funded by the EU - NGEU, and by project ANR COREGRAPHIE,
grant ANR-20-CE23-0002 of the French Agence Nationale de la Recherche. Views and
opinions expressed are however those of the authors only and do not necessarily
reflect those of the European Union or the Italian MUR. Neither the European
Union nor the Italian MUR can be held responsible for them.

[WebGraph framework]: <https://webgraph.di.unimi.it/>
[`env_logger`]: <https://docs.rs/env_logger/latest/env_logger/>
