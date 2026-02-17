# Algorithms for WebGraph

[![downloads](https://img.shields.io/crates/d/webgraph-algo)](https://crates.io/crates/webgraph-algo)
[![dependents](https://img.shields.io/librariesio/dependents/cargo/webgraph-algo)](https://crates.io/crates/webgraph-algo/reverse_dependencies)
![GitHub CI](https://github.com/vigna/webgraph-rs/actions/workflows/rust.yml/badge.svg)
![license](https://img.shields.io/crates/l/webgraph-algo)
[![Latest version](https://img.shields.io/crates/v/webgraph-algo.svg)](https://crates.io/crates/webgraph-algo)
[![Documentation](https://docs.rs/webgraph-algo/badge.svg)](https://docs.rs/webgraph-algo)

Algorithms for the Rust implementation of the [WebGraph framework] for graph
compression.

## Algorithms

### Graph Structure

- **Strongly Connected Components** ([SCCs]): [Tarjan's algorithm] and
  [Kosaraju's algorithm] for computing SCCs in directed graphs;
  [sequential][symm_seq] and [parallel][symm_par] computation
  of connected components for symmetric graphs
- **[Topological Sorting]**: Orders vertices of a directed acyclic graph
- **[Acyclicity Testing]**: Checks if a graph is acyclic

### Distance Computation

- **[HyperBall]**: Probabilistic algorithm for computing distances, closeness
  centrality, and other measures using HyperLogLog counters
- **[ExactSumSweep]**: Exact computation of eccentricities, radius, and
  diameter

### Community Detection

- **[Layered Label Propagation]** (LLP): Fast community detection algorithm for
  large graphs

## CLI Integration

Many algorithms can also be accessed through the [command-line interface].

## Acknowledgments

This software has been partially supported by project SERICS (PE00000014) under
the NRRP MUR program funded by the EU - NGEU, and by project ANR COREGRAPHIE,
grant ANR-20-CE23-0002 of the French Agence Nationale de la Recherche. Views and
opinions expressed are however those of the authors only and do not necessarily
reflect those of the European Union or the Italian MUR. Neither the European
Union nor the Italian MUR can be held responsible for them.

[SCCs]: https://docs.rs/webgraph-algo/latest/webgraph_algo/sccs/index.html
[Tarjan's algorithm]: https://docs.rs/webgraph-algo/latest/webgraph_algo/sccs/fn.tarjan.html
[Kosaraju's algorithm]: https://docs.rs/webgraph-algo/latest/webgraph_algo/sccs/fn.kosaraju.html
[symm_seq]: https://docs.rs/webgraph-algo/latest/webgraph_algo/sccs/fn.symm_seq.html
[symm_par]: https://docs.rs/webgraph-algo/latest/webgraph_algo/sccs/fn.symm_par.html
[Topological Sorting]: https://docs.rs/webgraph-algo/latest/webgraph_algo/fn.top_sort.html
[Acyclicity Testing]: https://docs.rs/webgraph-algo/latest/webgraph_algo/fn.is_acyclic.html
[HyperBall]: https://docs.rs/webgraph-algo/latest/webgraph_algo/distances/hyperball/struct.HyperBallBuilder.html
[ExactSumSweep]: https://docs.rs/webgraph-algo/latest/webgraph_algo/distances/exact_sum_sweep/index.html
[Layered Label Propagation]: https://docs.rs/webgraph-algo/latest/webgraph_algo/llp/index.html
[command-line interface]: https://docs.rs/webgraph-cli/latest/index.html
[WebGraph framework]: https://webgraph.di.unimi.it/
