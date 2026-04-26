# CLI for WebGraph

[![crates.io badge]][crates.io]
[![docs.rs badge]][docs.rs]
[![rustc badge]][min rustc version]
[![CI badge]][CI]
![license badge]
[![downloads badge]][crates.io]

Command-line interface for the Rust implementation of the [WebGraph framework]
for graph compression.

This crate provides the `webgraph` CLI tool with various subcommands for working
with compressed graphs. The tool can be installed with `cargo install webgraph-cli`,
or by invoking `cargo build` and retrieving it from your target directory.

The tool supports:

- **Building** accessory data structures (Elias–Fano offsets, DCF)
- **Converting** graphs between formats (arcs, ASCII, endianness)
- **Transforming** graphs (transpose, simplify)
- **Analyzing** graphs (code statistics)
- **Running** algorithms (Layered Label Propagation)
- **Benchmarking** graph operations

Each module corresponds to a group of commands, and each command is
implemented as a submodule.

The command `webgraph build complete` will generate completion code for
several commonly used shells, making it easier to use the CLI tool. For
example if you are using `bash`, you can run:

```bash
source <(webgraph build complete bash)
```

to have completions in the current shell session.

## Subcommands

- `analyze`: computes statistics on graphs;
- `bench`: benchmarks graph operations;
- `build`: builds accessory graph data structures;
- `check`: checks coherence of graph files;
- `from`: ingests data into graphs;
- `perm`: creates and manipulates permutations;
- `run`: runs algorithms on graph;
- `to`: converts graphs between representations;
- `transform`: applies transformations to graphs.

## Separate Binaries

This crate also provides specialized standalone binaries:

- `webgraph-dist`: tools for computing graph properties based on distances,
  including measures of centrality (HyperBall, ExactSumSweep);
- `webgraph-sccs`: computes strongly connected components;
- `webgraph-rank`: computes centrality measures on graphs.

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

[WebGraph framework]: https://webgraph.di.unimi.it/
[`env_logger`]: https://docs.rs/env_logger/latest/env_logger/
[crates.io badge]: https://img.shields.io/crates/v/webgraph-cli.svg
[crates.io]: https://crates.io/crates/webgraph-cli
[docs.rs badge]: https://docs.rs/webgraph-cli/badge.svg
[docs.rs]: https://docs.rs/webgraph-cli
[rustc badge]: https://img.shields.io/badge/rustc-1.85+-red.svg
[min rustc version]: https://rust-lang.github.io/rfcs/2495-min-rust-version.html
[CI badge]: https://github.com/vigna/webgraph-rs/actions/workflows/rust.yml/badge.svg
[CI]: https://github.com/vigna/webgraph-rs/actions
[license badge]: https://img.shields.io/crates/l/webgraph-cli
[downloads badge]: https://img.shields.io/crates/d/webgraph-cli
