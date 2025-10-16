# Algorithms for WebGraph

[![downloads](https://img.shields.io/crates/d/webgraph-algo)](https://crates.io/crates/webgraph-algo)
[![dependents](https://img.shields.io/librariesio/dependents/cargo/webgraph-algo)](https://crates.io/crates/webgraph-algo/reverse_dependencies)
![GitHub CI](https://github.com/vigna/webgraph-rs/actions/workflows/rust.yml/badge.svg)
![license](https://img.shields.io/crates/l/webgraph-algo)
[![Latest version](https://img.shields.io/crates/v/webgraph-algo.svg)](https://crates.io/crates/webgraph-algo)
[![Documentation](https://docs.rs/webgraph-algo/badge.svg)](https://docs.rs/webgraph-algo)

Algorithms for the Rust implementation of the [WebGraph framework] for graph
compression.

This crate provides efficient algorithms for analyzing compressed graphs:

## Algorithms

### Graph Structure
- **Strongly Connected Components** (SCCs): Tarjan's algorithm and variants for
  computing SCCs in directed graphs
- **Topological Sorting**: Order vertices of a directed acyclic graph
- **Acyclicity Testing**: Check if a graph is acyclic

### Distance Computation
- **HyperBall**: Probabilistic algorithm for computing distances, closeness
  centrality, and other measures using HyperLogLog counters
- **ExactSumSweep**: Exact computation of eccentricities, radius, and diameter

### Community Detection
- **Layered Label Propagation** (LLP): Fast community detection algorithm for
  large graphs

## CLI Integration

Many algorithms can also be accessed through the `webgraph-cli` command-line
tool.

## Acknowledgments

This software has been partially supported by project SERICS (PE00000014) under
the NRRP MUR program funded by the EU - NGEU, and by project ANR COREGRAPHIE,
grant ANR-20-CE23-0002 of the French Agence Nationale de la Recherche. Views and
opinions expressed are however those of the authors only and do not necessarily
reflect those of the European Union or the Italian MUR. Neither the European
Union nor the Italian MUR can be held responsible for them.

[WebGraph framework]: <https://webgraph.di.unimi.it/>
