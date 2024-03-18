# WebGraph

[![downloads](https://img.shields.io/crates/d/webgraph)](https://crates.io/crates/webgraph)
[![dependents](https://img.shields.io/librariesio/dependents/cargo/webgraph)](https://crates.io/crates/webgraph/reverse_dependencies)
![GitHub CI](https://github.com/vigna/webgraph-rs/actions/workflows/rust.yml/badge.svg)
![license](https://img.shields.io/crates/l/webgraph)
[![](https://tokei.rs/b1/github/vigna/webgraph-rs)](https://github.com/vigna/webgraph-rs).
[![Latest version](https://img.shields.io/crates/v/webgraph.svg)](https://crates.io/crates/webgraph)
[![Documentation](https://docs.rs/webgraph/badge.svg)](https://docs.rs/webgraph)

A Rust implementation of the [WebGraph framework](https://webgraph.di.unimi.it/)
for graph compression.

## Quick Setup

Assuming you have built all binaries, you will first need a graph in BV format,
for example downloading it from the [LAW website](http://law.di.unimi.it/). You
will need the `.graph` file (the bitstream containing a compressed representation
of the graph), the `.properties` file (metadata) and the `.offsets` file (a
bitstream containing pointers into the graph bitstream). As a first step, if
you need random access to the successors of a node, you need
to build an [Elias--Fano](sux::dict::EliasFano) representation of the
offsets with the command `build_ef` (this part can be skipped if you just need
sequential access), which will generate an `.ef` file. Then, to load a graph
with basename `BASENAME` you need to call

```[ignore]
let graph = BVGraph::with_basename("BASENAME").load()?;
```

The [`with_basename`] method returns a [`LoadConfig`] instance that can be further
customized, selecting endianness, type of memory access, etc. By default you
will get big endianness, memory mapping for both the graph and the offsets, and
dynamic code dispatch.

Once you loaded the [graph](), you can [retrieve the successors of a node]()
or [iterate on the whole graph]().

## More Options

- By starting from the [`BVGraphSeq`] class you can obtain an instance that
does not need the `.ef` file, but provides only [iteration]().

- Graphs can be labeled by [zipping]() then together with a [labeling](). In fact,
  graphs are just labelings with `usize` labels.

## Operating on Graphs

There are many operations available on graphs, such as [`transpose`] or [`simplify`].

## Acknowledgments

This software has been partially supported by project SERICS (PE00000014) under the NRRP MUR program funded by the EU - NGEU,
and by project ANR COREGRAPHIE, grant ANR-20-CE23-0002 of the French Agence Nationale de la Recherche.
