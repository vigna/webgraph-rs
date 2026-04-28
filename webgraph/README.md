# WebGraph

[![crates.io badge]][crates.io]
[![docs.rs badge]][docs.rs]
[![rustc badge]][min rustc version]
[![CI badge]][CI]
![license badge]
[![downloads badge]][crates.io]
[![coveralls badge]][coveralls]

A Rust implementation of the [WebGraph framework] for graph compression.

WebGraph is a framework for graph compression aimed at studying web graphs, but
currently being applied to several other types of graphs. It
provides simple ways to manage very large graphs, exploiting modern compression
techniques. More precisely, it is currently made of:

- Algorithms for compressing web graphs that exploit gap compression and
  differential compression (à la [LINK]), intervalization, and ζ codes to
  provide a high compression ratio (see [our datasets]). The algorithms are
  controlled by several parameters, which provide different tradeoffs between
  access speed and compression ratio.

- Algorithms for accessing a compressed graph without actually decompressing
  it, using lazy techniques that delay the decompression until it is actually
  necessary.

- Algorithms for analyzing very large graphs, such as [HyperBall], which has
  been used to show that Facebook has just [four degrees of separation].

- A [Java implementation] of the algorithms above, now in maintenance mode.

- This crate, providing a complete, documented implementation of the algorithms
  above in Rust. It is free software distributed under either the
  [GNU Lesser General Public License 2.1+] or the
  [Apache Software License 2.0].

- [Data sets] for large graphs (e.g., billions of links).

## Users of WebGraph

<a href="https://www.softwareheritage.org/">
<img src="https://raw.githubusercontent.com/vigna/webgraph-rs/main/svg/SWH.svg" width="200"></a>
        
<a href="https://www.commoncrawl.org/"><img src="https://raw.githubusercontent.com/vigna/webgraph-rs/main/svg/CC.svg" width="200"></a>

## Citation

You are welcome to use and improve WebGraph for your research work! If you find
our software useful for research, please cite the following papers in your own:

- "[WebGraph: The Next Generation (Is in Rust)]", by Tommaso Fontana,
  Sebastiano Vigna, and Stefano Zacchiroli, in _WWW '24: Companion Proceedings
  of the ACM on Web Conference 2024_, pages 686–689.
  [DOI 10.1145/3589335.3651581].

- "[The WebGraph Framework I: Compression Techniques]", by Paolo Boldi
  and Sebastiano Vigna, in _Proc. of the 13th international conference on
  World Wide Web_, WWW 2004, pages 595–602, ACM.
  [DOI 10.1145/988672.988752].

## Loading a compressed graph

A graph in BV format consists of a `BASENAME.graph` file (the compressed
bitstream), a `BASENAME.properties` file (metadata), and a `BASENAME.offsets`
file (pointers into the bitstream). You can download graphs from the [LAW web
site] or the [Common Crawl web site].

For random access to the successors of a node, you also need a `BASENAME.ef`
file containing an [Elias–Fano] representation of the offsets. You can build it
with the [command-line interface] (`webgraph build ef BASENAME`) or
programmatically with [`store_ef`].

To load a graph with random access:

```ignore
let graph = BvGraph::with_basename("BASENAME").load()?;
```

[`BvGraph::with_basename`] returns a [`LoadConf`] that can be further
customized, selecting endianness, type of memory access (memory mapping, full
loading, etc.), and so on. By default you will get big endianness, memory
mapping, and dynamic code dispatch.

If you only need [sequential access][iteration] (e.g., scanning all arcs), use
[`BvGraphSeq`], which does not require the `.ef` file:

```ignore
let graph = BvGraphSeq::with_basename("BASENAME").load()?;
```

You can [retrieve the successors of a node] or [iterate on the whole
graph]. In particular, using the handy [`for_`] macro:

```ignore
for_![(src, succ) in graph {
    for dst in succ {
        [do something with the arc src -> dst]
    }
}];
```

Note that on Windows memory mapping requires that the length of the graph file
is a multiple of the internal bit buffer. You can use the CLI command `run pad
u32` to ensure that your graph file is properly padded.

## In-memory graph representations

Several structures are available for building graphs in memory:

- [`VecGraph`] and [`LabeledVecGraph`]: mutable graphs backed by vectors; arcs
  must be added in increasing successor order. Serializable with [ε-serde].

- [`BTreeGraph`] and [`LabeledBTreeGraph`]: mutable graphs backed by B-tree
  maps; arcs can be added in any order.

- [`CsrGraph`]: a classical immutable Compressed Sparse Row representation,
  useful for algorithms that need fast random access without compression
  overhead. Serializable with [ε-serde].

All these types (except `ArcListGraph`) can also be serialized with [serde]
using the feature gate `serde`.

## Importing your data

The [command-line interface] provides a `from arcs` subcommand that reads
tab-separated arcs from standard input and compresses them directly into BV
format.

From code, you have several options depending on how your data is organized:

- If you can generate arcs **in sorted order** (by source, then by target), wrap
  your iterator in an [`ArcListGraph`] and pass it directly to
  [`BvCompConf::comp_graph`]; no intermediate storage is needed.

- If your arcs are **unsorted**, [`ParSortedGraph::from_pairs`] accepts an
  iterator on `(usize, usize)` pairs, sorts them, and returns a graph ready for
  parallel compression.

- If you can produce arcs in parallel as a **Rayon parallel iterator**,
  [`ParSortedGraph::par_from_pairs`] does the same with parallel sorting.

In all cases the result implements [`IntoParLenders`], so it can be passed to
[`BvCompConf::par_comp`] for parallel compression. For full control over
deduplication, memory budget, and progress logging, use the
[`ParSortedGraphConf`] builder obtained via [`ParSortedGraph::config()`].

All of the above also works for labeled graphs: just use `((usize, usize), L)`
pairs instead of `(usize, usize)` and the corresponding
[`ParSortedLabeledGraph`] methods.

## Compressing graphs

The entry point for compression is [`BvComp::with_basename`], which returns a
[`BvCompConf`] builder. Compression parameters (window size, codes for
different components, etc.) are controlled by [`CompFlags`]. compression
paramters are described in detail in the [`bvgraph`] module documentation.

To compress sequentially a graph that implements [`SequentialGraph`]:

```ignore
BvComp::with_basename("BASENAME")
    .comp_graph::<BE>(graph)?;
```

For better compression ratios at the cost of a longer compression time, use
[`BvCompConf::bvgraphz`], which enables Zuckerli-like
dynamic-programming reference selection.

### Parallel compression

Any graph implementing [`IntoParLenders`] can be compressed in parallel:

```ignore
BvComp::with_basename("BASENAME")
    .par_comp::<BE, _>(&graph)?;
```

All common graph types implement [`IntoParLenders`] automatically via
[`SplitLabeling`]. Transparent wrappers can adjust the splitting:

- [`ParGraph`]: overrides the number of parallel parts.
- [`ParDcfGraph`]: uses a precomputed [degree cumulative function][DCF] to
  balance work by arcs rather than by nodes.

### Labeled compression

Graphs with labels can be compressed with
[`BvCompConf::comp_labeled_graph`] (sequential) or
[`BvCompConf::par_comp_labeled`] (parallel). Label storage is controlled by a
[`StoreLabelsConf`] implementation; [`BitStreamStoreLabelsConf`] provides
bitstream-based storage with optional Zstandard compression.

## Graph transforms

The [`transform`] module provides common graph operations, each available in
sequential and parallel ([`SplitLabeling`]-based) variants:

- [**Transpose**][transpose]: reverse all arcs
  ([`transpose_split`] for parallel, [`transpose_labeled`] for labeled graphs).
- [**Symmetrize**][symmetrize]: add missing reverse arcs, optionally removing
  self-loops ([`symmetrize_split`] for parallel).
- [**Permute**][permute]: renumber nodes according to a permutation
  ([`permute_split`] for parallel).
- [**Map**][map]: renumber nodes through an arbitrary function, with
  deduplication ([`map_split`] for parallel).

## Graph and data wrappers

Lightweight wrappers combine or modify data without copying:

- [`PermutedGraph`]: applies a node permutation lazily.
- [`UnionGraph`]: merges arcs from two graphs.
- [`NoSelfLoopsGraph`]: filters out self-loops.
- [`JavaPermutation`]: reads Java WebGraph permutation files.
- [`ArcListGraph`]: a lazy sequential graph backed by an iterator of arcs,
  useful for feeding data directly into the compressor without materializing the
  graph in memory.

## Labels

Graphs are a special case of _labelings_: a [`SequentialLabeling`] assigns a
sequence of labels to each node, and a graph is simply a labeling whose labels
are `usize` successors. You can attach additional labels to a graph by
[zipping][`Zip`] it with a labeling.

Bitstream-based labelings are provided by [`BitStreamLabeling`] (random access)
and [`BitStreamLabelingSeq`] (sequential). Custom label (de)serializers
implement the [`BitDeserializer`]/[`BitSerializer`] traits. The [`Left`] and
[`Right`] projections extract one component from a zipped labeling.

## Graph traversals

The [`visits`] module provides breadth-first and depth-first traversals, both
sequential and parallel:

- **BFS**: [`breadth_first::Seq`], [`breadth_first::ParFair`] (fair division of
  work) and [`breadth_first::ParLowMem`] (memory-efficient).
- **DFS**: [`depth_first::SeqIter`].

Visits use a callback-based API with event types ([`EventPred`]/[`EventNoPred`])
that carry predecessor/parent information when requested.

## Graph iterators

[`BfsOrder`] and [`DfsOrder`] wrap the visit machinery into standard iterators
that yield nodes in BFS or DFS order. [`BfsOrderFromRoots`] starts the visit
from a given set of roots.

## Random graphs

[`ErdosRenyi`] generates Erdős–Rényi random graphs with a given number of nodes
and edge probability, useful for testing and benchmarking.

## Utilities

- [`par_map_fold`]: parallel map-fold over iterators, an alternative to Rayon's
  [`ParallelBridge`] that avoids some of its bottlenecks.
- [`graph::eq`], [`graph::eq_labeled`], [`labels::eq_sorted`]: equality checks
  for graphs and labelings, useful in tests.
- [`labels::check_impl`]: verifies that the sequential and random-access
  implementations of a labeling are consistent.

## Command-line interface

We provide a [command-line interface] to perform various operations on unlabeled
graphs (compression, transformation, analysis, etc.).

## Acknowledgments

This software has been partially supported by project SERICS (PE00000014) under
the NRRP MUR program funded by the EU - NGEU, and by project ANR COREGRAPHIE,
grant ANR-20-CE23-0002 of the French Agence Nationale de la Recherche. Views and
opinions expressed are however those of the authors only and do not necessarily
reflect those of the European Union or the Italian MUR. Neither the European
Union nor the Italian MUR can be held responsible for them.

[transpose]: https://docs.rs/webgraph/latest/webgraph/transform/fn.transpose.html
[`transpose_split`]: https://docs.rs/webgraph/latest/webgraph/transform/fn.transpose_split.html
[`transpose_labeled`]: https://docs.rs/webgraph/latest/webgraph/transform/fn.transpose_labeled.html
[symmetrize]: https://docs.rs/webgraph/latest/webgraph/transform/fn.symmetrize.html
[`symmetrize_split`]: https://docs.rs/webgraph/latest/webgraph/transform/fn.symmetrize_split.html
[permute]: https://docs.rs/webgraph/latest/webgraph/transform/fn.permute.html
[`permute_split`]: https://docs.rs/webgraph/latest/webgraph/transform/fn.permute_split.html
[map]: https://docs.rs/webgraph/latest/webgraph/transform/fn.map.html
[`map_split`]: https://docs.rs/webgraph/latest/webgraph/transform/fn.map_split.html
[`transform`]: https://docs.rs/webgraph/latest/webgraph/transform/index.html
[`BvGraph::with_basename`]: https://docs.rs/webgraph/latest/webgraph/graphs/bvgraph/random_access/struct.BvGraph.html#method.with_basename
[`BvGraphSeq`]: https://docs.rs/webgraph/latest/webgraph/graphs/bvgraph/sequential/struct.BvGraphSeq.html
[`LoadConf`]: https://docs.rs/webgraph/latest/webgraph/graphs/bvgraph/load/struct.LoadConf.html
[`BvComp::with_basename`]: https://docs.rs/webgraph/latest/webgraph/graphs/bvgraph/comp/struct.BvComp.html#method.with_basename
[`BvCompConf`]: https://docs.rs/webgraph/latest/webgraph/graphs/bvgraph/comp/struct.BvCompConf.html
[`BvCompConf::bvgraphz`]: https://docs.rs/webgraph/latest/webgraph/graphs/bvgraph/comp/struct.BvCompConf.html#method.bvgraphz
[`BvCompConf::comp_labeled_graph`]: https://docs.rs/webgraph/latest/webgraph/graphs/bvgraph/comp/struct.BvCompConf.html#method.comp_labeled_graph
[`BvCompConf::par_comp_labeled`]: https://docs.rs/webgraph/latest/webgraph/graphs/bvgraph/comp/struct.BvCompConf.html#method.par_comp_labeled
[`CompFlags`]: https://docs.rs/webgraph/latest/webgraph/graphs/bvgraph/comp/struct.CompFlags.html
[`store_ef`]: https://docs.rs/webgraph/latest/webgraph/graphs/bvgraph/fn.store_ef.html
[`IntoParLenders`]: https://docs.rs/webgraph/latest/webgraph/traits/trait.IntoParLenders.html
[`SplitLabeling`]: https://docs.rs/webgraph/latest/webgraph/traits/labels/trait.SplitLabeling.html
[`ParGraph`]: https://docs.rs/webgraph/latest/webgraph/graphs/par_graphs/struct.ParGraph.html
[`ParDcfGraph`]: https://docs.rs/webgraph/latest/webgraph/graphs/par_graphs/struct.ParDcfGraph.html
[DCF]: https://docs.rs/webgraph/latest/webgraph/traits/labels/trait.SequentialLabeling.html#method.build_dcf
[`ParSortedGraph`]: https://docs.rs/webgraph/latest/webgraph/graphs/par_sorted_graph/struct.ParSortedGraph.html
[`ParSortedGraphConf`]: https://docs.rs/webgraph/latest/webgraph/graphs/par_sorted_graph/struct.ParSortedGraphConf.html
[`MemoryUsage`]: https://docs.rs/webgraph/latest/webgraph/utils/enum.MemoryUsage.html
[`StoreLabelsConf`]: https://docs.rs/webgraph/latest/webgraph/traits/store/trait.StoreLabelsConf.html
[`BitStreamStoreLabelsConf`]: https://docs.rs/webgraph/latest/webgraph/labels/bitstream/struct.BitStreamStoreLabelsConf.html
[`SequentialGraph`]: https://docs.rs/webgraph/latest/webgraph/traits/graph/trait.SequentialGraph.html
[`SequentialLabeling`]: https://docs.rs/webgraph/latest/webgraph/traits/labels/trait.SequentialLabeling.html
[iterate on the whole graph]: https://docs.rs/webgraph/latest/webgraph/traits/labels/trait.SequentialLabeling.html#method.iter
[iteration]: https://docs.rs/webgraph/latest/webgraph/traits/labels/trait.SequentialLabeling.html#method.iter
[retrieve the successors of a node]: https://docs.rs/webgraph/latest/webgraph/traits/graph/trait.RandomAccessGraph.html#method.successors
[`Zip`]: https://docs.rs/webgraph/latest/webgraph/labels/zip/struct.Zip.html
[`BitStreamLabeling`]: https://docs.rs/webgraph/latest/webgraph/labels/bitstream/labeling/struct.BitStreamLabeling.html
[`BitStreamLabelingSeq`]: https://docs.rs/webgraph/latest/webgraph/labels/bitstream/labeling/struct.BitStreamLabelingSeq.html
[`BitDeserializer`]: https://docs.rs/webgraph/latest/webgraph/traits/serde/trait.BitDeserializer.html
[`BitSerializer`]: https://docs.rs/webgraph/latest/webgraph/traits/serde/trait.BitSerializer.html
[`PermutedGraph`]: https://docs.rs/webgraph/latest/webgraph/graphs/permuted_graph/struct.PermutedGraph.html
[`UnionGraph`]: https://docs.rs/webgraph/latest/webgraph/graphs/union_graph/struct.UnionGraph.html
[`NoSelfLoopsGraph`]: https://docs.rs/webgraph/latest/webgraph/graphs/no_selfloops_graph/struct.NoSelfLoopsGraph.html
[`visits`]: https://docs.rs/webgraph/latest/webgraph/visits/index.html
[`breadth_first::Seq`]: https://docs.rs/webgraph/latest/webgraph/visits/breadth_first/seq/struct.Seq.html
[`breadth_first::ParFair`]: https://docs.rs/webgraph/latest/webgraph/visits/breadth_first/par_fair/struct.ParFair.html
[`breadth_first::ParLowMem`]: https://docs.rs/webgraph/latest/webgraph/visits/breadth_first/par_low_mem/struct.ParLowMem.html
[`depth_first::SeqIter`]: https://docs.rs/webgraph/latest/webgraph/visits/depth_first/seq/struct.SeqIter.html
[`EventPred`]: https://docs.rs/webgraph/latest/webgraph/visits/breadth_first/enum.EventPred.html
[`EventNoPred`]: https://docs.rs/webgraph/latest/webgraph/visits/breadth_first/enum.EventNoPred.html
[`BfsOrder`]: https://docs.rs/webgraph/latest/webgraph/visits/breadth_first/seq/struct.BfsOrder.html
[`BfsOrderFromRoots`]: https://docs.rs/webgraph/latest/webgraph/visits/breadth_first/seq/struct.BfsOrderFromRoots.html
[`DfsOrder`]: https://docs.rs/webgraph/latest/webgraph/visits/depth_first/seq/struct.DfsOrder.html
[`ErdosRenyi`]: https://docs.rs/webgraph/latest/webgraph/graphs/random/er/struct.ErdosRenyi.html
[`VecGraph`]: https://docs.rs/webgraph/latest/webgraph/graphs/vec_graph/struct.VecGraph.html
[`LabeledVecGraph`]: https://docs.rs/webgraph/latest/webgraph/graphs/vec_graph/struct.LabeledVecGraph.html
[`BTreeGraph`]: https://docs.rs/webgraph/latest/webgraph/graphs/btree_graph/struct.BTreeGraph.html
[`LabeledBTreeGraph`]: https://docs.rs/webgraph/latest/webgraph/graphs/btree_graph/struct.LabeledBTreeGraph.html
[`CsrGraph`]: https://docs.rs/webgraph/latest/webgraph/graphs/csr_graph/struct.CsrGraph.html
[`ArcListGraph`]: https://docs.rs/webgraph/latest/webgraph/graphs/arc_list_graph/struct.ArcListGraph.html
[`BvCompConf::comp_graph`]: https://docs.rs/webgraph/latest/webgraph/graphs/bvgraph/comp/struct.BvCompConf.html#method.comp_graph
[`BvCompConf::par_comp`]: https://docs.rs/webgraph/latest/webgraph/graphs/bvgraph/comp/struct.BvCompConf.html#method.par_comp
[`ParSortedGraph::from_pairs`]: https://docs.rs/webgraph/latest/webgraph/graphs/par_sorted_graph/struct.ParSortedGraph.html#method.from_pairs
[`ParSortedGraph::par_from_pairs`]: https://docs.rs/webgraph/latest/webgraph/graphs/par_sorted_graph/struct.ParSortedGraph.html#method.par_from_pairs
[`ParSortedGraph::config()`]: https://docs.rs/webgraph/latest/webgraph/graphs/par_sorted_graph/struct.ParSortedGraph.html#method.config
[`ParSortedLabeledGraph`]: https://docs.rs/webgraph/latest/webgraph/graphs/par_sorted_graph/struct.ParSortedLabeledGraph.html
[`Left`]: https://docs.rs/webgraph/latest/webgraph/labels/proj/struct.Left.html
[`Right`]: https://docs.rs/webgraph/latest/webgraph/labels/proj/struct.Right.html
[`JavaPermutation`]: https://docs.rs/webgraph/latest/webgraph/utils/java_perm/struct.JavaPermutation.html
[`par_map_fold`]: https://docs.rs/webgraph/latest/webgraph/traits/par_map_fold/trait.ParMapFold.html
[`graph::eq`]: https://docs.rs/webgraph/latest/webgraph/traits/graph/fn.eq.html
[`graph::eq_labeled`]: https://docs.rs/webgraph/latest/webgraph/traits/graph/fn.eq_labeled.html
[`labels::eq_sorted`]: https://docs.rs/webgraph/latest/webgraph/traits/labels/fn.eq_sorted.html
[`labels::check_impl`]: https://docs.rs/webgraph/latest/webgraph/traits/labels/fn.check_impl.html
[`for_`]: https://docs.rs/lender/latest/lender/macro.for_.html
[LAW web site]: http://law.di.unimi.it/
[Common Crawl web site]: https://commoncrawl.org/
[Elias–Fano]: https://docs.rs/sux/latest/sux/dict/elias_fano/struct.EliasFano.html
[WebGraph framework]: https://webgraph.di.unimi.it/
[ε-serde]: https://crates.io/crates/epserde/
[serde]: https://crates.io/crates/serde
[command-line interface]: https://docs.rs/webgraph-cli/latest/index.html
[LINK]: https://ieeexplore.ieee.org/document/999950
[our datasets]: http://law.di.unimi.it/datasets.php
[HyperBall]: https://dl.acm.org/doi/10.5555/2606262.2606545
[four degrees of separation]: http://vigna.di.unimi.it/papers.php#BBRFDS
[Java implementation]: http://webgraph.di.unimi.it/
[GNU Lesser General Public License 2.1+]: https://www.gnu.org/licenses/old-licenses/lgpl-2.1.html
[Apache Software License 2.0]: https://www.apache.org/licenses/LICENSE-2.0
[Data sets]: http://law.di.unimi.it/datasets.php
[WebGraph: The Next Generation (Is in Rust)]: http://vigna.di.unimi.it/papers.php#FVZWNG
[DOI 10.1145/3589335.3651581]: https://dl.acm.org/doi/10.1145/3589335.3651581
[The WebGraph Framework I: Compression Techniques]: http://vigna.di.unimi.it/papers.php#BoVWFI
[DOI 10.1145/988672.988752]: https://dl.acm.org/doi/10.1145/988672.988752
[crates.io badge]: https://img.shields.io/crates/v/webgraph.svg
[crates.io]: https://crates.io/crates/webgraph
[docs.rs badge]: https://docs.rs/webgraph/badge.svg
[docs.rs]: https://docs.rs/webgraph
[rustc badge]: https://img.shields.io/badge/rustc-1.85+-red.svg
[min rustc version]: https://rust-lang.github.io/rfcs/2495-min-rust-version.html
[CI badge]: https://github.com/vigna/webgraph-rs/actions/workflows/rust.yml/badge.svg
[CI]: https://github.com/vigna/webgraph-rs/actions
[license badge]: https://img.shields.io/crates/l/webgraph
[downloads badge]: https://img.shields.io/crates/d/webgraph
[coveralls badge]: https://coveralls.io/repos/github/vigna/webgraph-rs/badge.svg?branch=main
[coveralls]: https://coveralls.io/github/vigna/webgraph-rs?branch=main
[`bvgraph`]: https://docs.rs/webgraph/latest/webgraph/graphs/bvgraph/index.html
[`ParallelBridge`]: https://docs.rs/rayon/latest/rayon/iter/trait.ParallelBridge.html
