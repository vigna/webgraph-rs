# WebGraph

[![downloads](https://img.shields.io/crates/d/webgraph)](https://crates.io/crates/webgraph)
[![dependents](https://img.shields.io/librariesio/dependents/cargo/webgraph)](https://crates.io/crates/webgraph/reverse_dependencies)
![GitHub CI](https://github.com/vigna/webgraph-rs/actions/workflows/rust.yml/badge.svg)
![license](https://img.shields.io/crates/l/webgraph)
[![Latest version](https://img.shields.io/crates/v/webgraph.svg)](https://crates.io/crates/webgraph)
[![Documentation](https://docs.rs/webgraph/badge.svg)](https://docs.rs/webgraph)
[![Coverage Status](https://coveralls.io/repos/github/vigna/webgraph-rs/badge.svg?branch=main)](https://coveralls.io/github/vigna/webgraph-rs?branch=main)

A Rust implementation of the [WebGraph framework] for graph compression.

WebGraph is a framework for graph compression aimed at studying web graphs, but
currently being applied to several other types of graphs. It
provides simple ways to manage very large graphs, exploiting modern compression
techniques. More precisely, it is currently made of:

- A set of simple codes, called ζ _codes_, which are particularly suitable for
  storing web graphs (or, in general, integers with a power-law distribution in a
  certain exponent range).

- Algorithms for compressing web graphs that exploit gap compression and
  differential compression (à la
  [LINK](https://ieeexplore.ieee.org/document/999950)),
  intervalization, and ζ codes to provide a high compression ratio (see [our
  datasets](http://law.di.unimi.it/datasets.php)). The algorithms are controlled
  by several parameters, which provide different tradeoffs between access speed
  and compression ratio.

- Algorithms for accessing a compressed graph without actually decompressing
  it, using lazy techniques that delay the decompression until it is actually
  necessary.

- Algorithms for analyzing very large graphs, such as
  [HyperBall](https://dl.acm.org/doi/10.5555/2606262.2606545), which has been
  used to show that Facebook has just [four degrees of
  separation](http://vigna.di.unimi.it/papers.php#BBRFDS).

- A [Java implementation](http://webgraph.di.unimi.it/) of the algorithms above,
  now in maintenance mode.

- This crate, providing a complete, documented implementation of the algorithms
  above in Rust. It is free software distributed under either the  [GNU Lesser
  General Public License
  2.1+](https://www.gnu.org/licenses/old-licenses/lgpl-2.1.html) or the [Apache
  Software License 2.0](https://www.apache.org/licenses/LICENSE-2.0).

- [Data sets](http://law.di.unimi.it/datasets.php) for large graphs (e.g.,
  billions of links).

## Citation

You are welcome to use and improve WebGraph for your research work! If you find
our software useful for research, please cite the following papers in your own:

- “[WebGraph: The Next Generation (Is in
  Rust)](http://vigna.di.unimi.it/papers.php#FVZWNG)”, by Tommaso Fontana,
  Sebastiano Vigna, and Stefano Zacchiroli, in _WWW '24: Companion Proceedings
  of the ACM on Web Conference 2024_, pages 686–689. [DOI
  10.1145/3589335.3651581](https://dl.acm.org/doi/10.1145/3589335.3651581).

- “[The WebGraph Framework I: Compression
  Techniques](http://vigna.di.unimi.it/papers.php#BoVWFI)”, by Paolo Boldi and
  Sebastiano Vigna, in _Proc. of the 13th international conference on World
  Wide Web_, WWW 2004, pages 595–602, ACM. [DOI
  10.1145/988672.988752](https://dl.acm.org/doi/10.1145/988672.988752).
  
## Quick Setup

Assuming you have built all binaries, you will first need a graph in BV format,
for example downloading it from the [LAW website]. For a graph with basename
`BASENAME`, you will need the `BASENAME.graph` file (the bitstream containing a
compressed representation of the graph), the `BASENAME.properties` file
(metadata), and the `BASENAME.offsets` file (a bitstream containing pointers into
the graph bitstream).

As a first step, if you need random access to the successors of a node, you need
to build an [Elias–Fano] representation of the offsets (this part can be skipped
if you just need sequential access). There is a CLI command `webgraph` with many
subcommands, among which `build`, and `webgraph build ef BASENAME` will build
the representation for you, serializing it with [ε-serde] in a file
named `BASENAME.ef`.

Then, to load the graph you need to call

```ignore
let graph = BVGraph::with_basename("BASENAME").load()?;
```

The [`with_basename`] method returns a [`LoadConfig`] instance that can be
further customized, selecting endianness, type of memory access, and so on. By
default you will get big endianness, memory mapping for both the graph and the
offsets, and dynamic code dispatch.

Note that on Windows memory mapping requires that the length of the graph file
is a multiple of the internal bit buffer. You can use the CLI command `run pad
u32` to ensure that your graph file is properly padded.

Once you load the graph, you can [retrieve the successors of a node] or
[iterate on the whole graph]. In particular, using the handy [`for_`] macro,
you can write an iteration on the graph as

```ignore
for_![(src, succ) in graph {
    for dst in succ {
        [do something with the arc src -> dst]
    }
}];
```

## Mutable Graphs

A number of structures make it possible to create dynamically growing graphs:
[`BTreeGraph`], [`VecGraph`] and their labeled counterparts
[`LabeledBTreeGraph`] and [`LabeledVecGraph`]. These structures can also
be serialized with [serde](https://crates.io/crates/serde) using the feature
gate `serde`; [`VecGraph`]/[`LabeledVecGraph`] can also be serialized with
[ε-serde](https://crates.io/crates/epserde).

## Command–Line Interface

We provide a command-line interface to perform various operations on graphs. The
CLI is the main method of the library, so it can be executed with `cargo run`.

## More Options

- By starting from the [`BVGraphSeq`] class you can obtain an instance that does
  not need the `BASENAME.ef` file, but provides only [iteration].

- Graphs can be labeled by [zipping] them together with a [labeling]. In fact,
  graphs are just labelings with `usize` labels.

## Operating on Graphs

There are many operations available on graphs, such as [transpose],
[simplify], and [permute].

## Compressing Graphs Given as List of Arcs

A simple way to compress a graph is to provide it as a list of arcs. The
`webgraph` CLI provides a command `from` with a subcommand `arcs` that reads a
list of TAB-separated list of arcs from standard input and writes a compressed
[`BvGraph`]. For example,

```bash
echo -e "0\t1\n1\t2\n2\t3" >3-cycle.tsv
cargo run --release from arcs --exact 3-cycle <3-cycle.tsv
```

will create a file compressed graph with basename `3-cycle`. The `--exact` flag
is used to specify that the labels provided are exactly the node numbers,
numbered starting from zero: otherwise, a mapping from assigned node number to
labels will be created in RAM and store in `3-cycle.nodes` file.
The labels are stored in a `HashMap`, so, for very large graphs, the mapping
might not fit in RAM. For example,

```bash
echo -e "a\tb\nb\tc\nc\ta" > graph.tsv
# convert to bvgraph
cat graph.tsv | cargo run --release from arcs graph
```

The graph can be converted back in the arcs format using the `to arcs` command.
Passing the `.nodes` files to `--labels` will write the labels instead of the
node numbers.

```bash
# convert back to tsv
cargo run --release to arcs --labels=graph.nodes graph > back.tsv
```

Moreover, the `--separator` argument can be used in both `from arcs` and `to arcs`
to change the character that separates source and target to parse other formats
such as `csv`. For example,

```bash
echo -e "a,b\nb,c\nc,a" > graph.csv
# convert to bvgraph
$ cat graph.csv | cargo run --release from arcs --separator=',' graph
# convert back to csv
$ cargo run --release to arcs --separator=',' --labels=graph.nodes graph > back.csv
```

## Acknowledgments

This software has been partially supported by project SERICS (PE00000014) under
the NRRP MUR program funded by the EU - NGEU, and by project ANR COREGRAPHIE,
grant ANR-20-CE23-0002 of the French Agence Nationale de la Recherche. Views and
opinions expressed are however those of the authors only and do not necessarily
reflect those of the European Union or the Italian MUR. Neither the European
Union nor the Italian MUR can be held responsible for them.

[transpose]: <https://docs.rs/webgraph/latest/webgraph/transform/fn.transpose.html>
[simplify]: <https://docs.rs/webgraph/latest/webgraph/transform/fn.simplify.html>
[permute]: <https://docs.rs/webgraph/latest/webgraph/transform/fn.permute.html>
[`with_basename`]: <https://docs.rs/webgraph/latest/webgraph/graphs/bvgraph/random_access/struct.BvGraph.html#method.with_basename>
[`BVGraphSeq`]: <https://docs.rs/webgraph/latest/webgraph/graphs/bvgraph/sequential/struct.BvGraphSeq.html>
[`BVGraph`]: <https://docs.rs/webgraph/latest/webgraph/graphs/bvgraph/sequential/struct.BvGraph.html>
[`LoadConfig`]: <https://docs.rs/webgraph/latest/webgraph/graphs/bvgraph/load/struct.LoadConfig.html>
[iterate on the whole graph]: <https://docs.rs/webgraph/latest/webgraph/traits/labels/trait.SequentialLabeling.html#method.iter>
[zipping]: <https://docs.rs/webgraph/latest/webgraph/labels/zip/struct.Zip.html>
[labeling]: <https://docs.rs/webgraph/latest/webgraph/traits/labels/trait.SequentialLabeling.html>
[iteration]: <https://docs.rs/webgraph/latest/webgraph/traits/labels/trait.SequentialLabeling.html#method.iter>
[retrieve the successors of a node]: <https://docs.rs/webgraph/latest/webgraph/traits/graph/trait.RandomAccessGraph.html#method.successors>
[LAW website]: <http://law.di.unimi.it/>
[Elias–Fano]: <sux::dict::EliasFano>
[WebGraph framework]: <https://webgraph.di.unimi.it/>
[ε-serde]: <https://crates.io/crates/epserde/>
[`for_`]: <https://docs.rs/lender/latest/lender/macro.for_.html>
[`VecGraph`]: <https://docs.rs/webgraph/latest/webgraph/graphs/vec_graph/struct.VecGraph.html>
[`LabeledVecGraph`]: <https://docs.rs/webgraph/latest/webgraph/graphs/vec_graph/struct.LabeledVecGraph.html>
[`BTreeGraph`]: <https://docs.rs/webgraph/latest/webgraph/graphs/btree_graph/struct.BTreeGraph.html>
[`LabeledBTreeGraph`]: <https://docs.rs/webgraph/latest/webgraph/graphs/btree_graph/struct.LabeledBTreeGraph.html>
