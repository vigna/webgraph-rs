# WebGraph

[![downloads](https://img.shields.io/crates/d/webgraph)](https://crates.io/crates/webgraph)
[![dependents](https://img.shields.io/librariesio/dependents/cargo/webgraph)](https://crates.io/crates/webgraph/reverse_dependencies)
![GitHub CI](https://github.com/vigna/webgraph-rs/actions/workflows/rust.yml/badge.svg)
![license](https://img.shields.io/crates/l/webgraph)
[![](https://tokei.rs/b1/github/vigna/webgraph-rs)](https://github.com/vigna/webgraph-rs)
[![Latest version](https://img.shields.io/crates/v/webgraph.svg)](https://crates.io/crates/webgraph)
[![Documentation](https://docs.rs/webgraph/badge.svg)](https://docs.rs/webgraph)

A Rust implementation of the [WebGraph framework] for graph compression.

WebGraph is a framework for graph compression aimed at studying web graphs, but
currently being applied to several other type of graphs. It
provides simple ways to manage very large graphs, exploiting modern compression
techniques. More precisely, it is currently made of:

- A set of simple codes, called ζ _codes_, which are particularly suitable for
 storing web graphs (or, in general, integers with a power-law distribution in a
 certain exponent range).

- Algorithms for compressing web graphs that exploit gap compression and
 differential compression (à la
 [LINK](http://www.hpl.hp.com/techreports/Compaq-DEC/SRC-RR-175.html)),
 intervalisation, and ζ codes to provide a high compression ratio (see [our
 datasets](http://law.di.unimi.it/datasets.php)). The algorithms are controlled
 by several parameters, which provide different tradeoffs between access speed
 and compression ratio.

- Algorithms for accessing a compressed graph without actually decompressing
 it, using lazy techniques that delay the decompression until it is actually
 necessary.

- Algorithms for analysing very large graphs, such as {@link
 it.unimi.dsi.webgraph.algo.HyperBall}, which has been used to show that
 Facebook has just [four degrees of
 separation](http://vigna.di.unimi.it/papers.php#BBRFDS).

- A [Java implementation](http://webgraph.di.unimi.it/) of the algorithms above,
  now in maintenance mode.

- This crate, providing a complete, documented implementation of the algorithms
  above in Rust. It is free software distributed under either the  [GNU Lesser
 General Public License
 2.1+](https://www.gnu.org/licenses/old-licenses/lgpl-2.1.html) or the [Apache
 Software License 2.0](https://www.apache.org/licenses/LICENSE-2.0).

- [Data sets](http://law.di.unimi.it/datasets.php) for large graph (e.g.,
  billions of links).

## Citation

You are welcome to use and improve WebGraph for your research work! If you find
our software useful for research, please cite the following papers in your own:

- [“WebGraph: The Next Generation (Is in
  Rust)”](http://vigna.di.unimi.it/papers.php#FVZWNG), by Tommaso Fontana,
  Sebastiano Vigna, and Stefano Zacchiroli, in WWW '24: Companion Proceedings
  of the ACM on Web Conference 2024, pages 686-689.  [DOI
  10.1145/3589335.3651581](https://dl.acm.org/doi/10.1145/3589335.3651581)

- [“The WebGraph Framework I: Compression
  Techniques”](http://vigna.di.unimi.it/papers.php#BoVWFI), by Paolo Boldi and
  Sebastiano Vigna, in _Proc. of the 13th international conference on World
  Wide Web, WWW 2004, pages 595-602, ACM. [DOI
  10.1145/988672.988752](https://dl.acm.org/doi/10.1145/988672.988752)
  
## Quick Setup

Assuming you have built all binaries, you will first need a graph in BV format,
for example downloading it from the [LAW website]. For a graph with basename
BASENAME, you will need the `BASENAME.graph` file (the bitstream containing a
compressed representation of the graph), the `BASENAME.properties` file
(metadata) and the `BASENAME.offsets` file (a bitstream containing pointers into
the graph bitstream).

As a first step, if you need random access to the successors of a node, you need
to build an [Elias–Fano] representation of the offsets (this part can be skipped
if you just need sequential access). There is a CLI command `webgraph` with many
subcommands, among which `build`, and `webgraph build ef BASENAME` will build
the representation for you, serializing it with [ε-serde] in a file
named `BASENAME.ef`.

Then, to load the graph you need to call

```[ignore]
let graph = BVGraph::with_basename("BASENAME").load()?;
```

The [`with_basename`] method returns a [`LoadConfig`] instance that can be
further customized, selecting endianness, type of memory access, and so on. By
default you will get big endianness, memory mapping for both the graph and the
offsets, and dynamic code dispatch.

Once you load the graph, you can [retrieve the successors of a node] or
[iterate on the whole graph]. In particular, using the handy [`for_`] macro,
you can write an iteration on the graph as

```[ignore]
for_!((src, succ) in graph {
    for dst in succ {
        [do something with the arc src -> dst]
    }
});
```

## More Options

- By starting from the [`BVGraphSeq`] class you can obtain an instance that does
not need the `BASENAME.ef` file, but provides only [iteration].

- Graphs can be labeled by [zipping] them together with a [labeling]. In fact,
  graphs are just labelings with `usize` labels.

## Operating on Graphs

There are many operations available on graphs, such as [`transpose`] and
[`simplify`]. You can [permute] a graph.

## Acknowledgments

This software has been partially supported by project SERICS (PE00000014) under
the NRRP MUR program funded by the EU - NGEU, and by project ANR COREGRAPHIE,
grant ANR-20-CE23-0002 of the French Agence Nationale de la Recherche.

[`transpose`]: <https://docs.rs/webgraph/latest/webgraph/transform/transpose/index.html>
[`simplify`]: <https://docs.rs/webgraph/latest/webgraph/transform/simplify/index.html>
[`with_basename`]: <https://docs.rs/webgraph/latest/webgraph/struct.BVGraph.html#method.with_basename>
[`BVGraphSeq`]: <https://docs.rs/webgraph/latest/webgraph/struct.BVGraphSeq.html>
[`LoadConfig`]: <https://docs.rs/webgraph/latest/webgraph/struct.LoadConfig.html>
[iterate on the whole graph]: <https://docs.rs/webgraph/latest/webgraph/trait/SequentialLabeling.html#method.iter>
[zipping]: <https://docs.rs/webgraph/latest/webgraph/struct/Zip.html>
[labeling]: <https://docs.rs/webgraph/latest/webgraph/trait/SequentialLabeling.html>
[iteration]: <https://docs.rs/webgraph/latest/webgraph/trait/SequentialLabeling.html#method.iter>
[retrieve the successors of a node]: <https://docs.rs/webgraph/latest/webgraph/trait/RandomAccessGraph.html#method.successors>
[LAW website]: <http://law.di.unimi.it/>
[Elias–Fano]: <sux::dict::EliasFano>
[WebGraph framework]: <https://webgraph.di.unimi.it/>
[permute]: <https://docs.rs/webgraph/latest/webgraph/transform/permute/index.html>
[ε-serde]: <nttps://crates.io/crates/epserde/>
[`for_`]: <https://docs.rs/lender/latest/lender/macro.for_.html>
