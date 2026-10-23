# MergeLabels: merging the labels of repeated arcs while sorting

## Intent

A labeled graph built by sorting labeled pairs may contain the same arc
`(src, dst)` several times with different labels. Two motivating cases:

- swh-graph: an arc from a directory to a content carries one label per file
  name, and the same content may appear under several names, so the sorted
  pairs contain repeated arcs, one per name, that must become a single arc
  whose label holds all the names;
- the user's applications, which generate a very large number of repeated
  arcs that must be combined, e.g., counted.

The BV format stores successor lists as sets, so repeats must be resolved
before compression.

### History

A previous version of this spec put the merging in the compressor
(`BvCompConf::merge_labels`), and it was implemented on `main` in commits
`79c89299..6b79b49f` (pushed to `origin/main`, unreleased: anyone
tracking git `main` may already use `BvCompConf::merge_labels`).

The user then observed that merging at compression time comes too late:
every batch the sort writes to disk still contains every repeated arc, and
the k-way merge reads them all back. When repeats are the norm, as in
counting, the disk space and I/O of the sort are proportional to the raw
arcs, not to the distinct ones.

The user's decision is to merge while sorting, by lifting the problem: each
batch is written with partially merged labels, and the k-way merge then
merges the partial results coming from different batches. Besides the
injection of a basic label into a partial result, this needs a merge of two
partial results. The user's concern is efficiency: injecting a label as a
singleton vector would cost an allocation per arc.

The answer, agreed with the user, is to never inject: an in-place step
adding a basic label to a reused state replaces the injection, and only the
merge of two states is new. A final step, also agreed with the user, turns
the fully merged state into the label stored in the graph, possibly of a
different type. This is the shape of the aggregation APIs of dataflow
systems and databases (see "Prior art").

The compression-side merging is removed: the sorted output has no repeated
arcs, so the compressor has nothing to merge. The explicit compressor errors
on repeated and unsorted successors, introduced by the same commits, stay.

## Goals

- The trait `MergeLabel<L>` (in `webgraph/src/traits/labels.rs`) is
  replaced by two traits, `MergeLabels` and `AddLabel<L>`, with an add
  step, a merge step, and a final step (see "The traits").
- The sort merges in both places where today `DEDUP` keeps the first
  occurrence:
  - within a batch, before writing it, so that each batch on disk holds at
    most one triple per distinct `(src, dst)`;
  - across batches, in the k-way merge, so that the sorted output has no
    repeated arcs.
- Neither the state written to disk nor the final label need be `Copy`:
  only the input label, which is radix-sorted in memory, must be. A `Vec`
  (or `SmallVec`, or an enum) of names must be usable. Today
  `ParSortedLabeledGraph` cannot carry non-`Copy` labels at all.
- A single merge-policy type parameter replaces the `const DEDUP: bool`
  parameter throughout the sorting code. "No merging" (repeats kept) is the
  default, `.dedup()` remains, and `.merge_labels(merger)` is new; they set
  the same parameter, so they cannot conflict (the last call wins).
- The library provides a serializer and deserializer for `Vec`-like labels
  (user's decision): swh-graph's existing code cannot serve the sort (the
  batch files are native-endian, and its deserializer is big-endian only),
  and the tests need one anyway.
- The compressor API returns to its 0.7 shape, keeping the explicit errors.

## Non-goals

- In-memory compaction of batches. An in-memory batch still holds every raw
  arc, so the number of batch files (the fan-in of the k-way merge) is
  driven by raw arcs; reducing it would need compaction of states in
  memory, which works only when the input and state types coincide.
- A deserialize-into API. `BitDeserializer::deserialize` returns an owned
  value, so decoding a `Vec` state allocates once per triple read. Users
  control this through their choice of state type (e.g.,
  `SmallVec<[u64; 1]>` or an enum `One(L) | Many(Vec<L>)`, since most arcs
  are not repeated). Possible follow-up.
- Merging in transposition, symmetrization, or labeled union. They keep
  their current behavior (transposing a graph cannot create repeats).
- Merging in the compressor (removed, see above).
- Semver compatibility: breaking changes are acceptable (few users at this
  time). The goal is the cleanest API.

## Anti-goals

- An allocation per arc in the batch stage when the state is a collection.
- Any way to configure the batch-level step and the k-way step separately,
  and hence inconsistently: a batch-level merge without the k-way merge
  would silently leave repeated arcs in the output.
- Any cost on the no-merging and `.dedup()` paths with respect to today.
  Both are hot paths of the unlabeled sort: `transpose_seq`/`transpose_par`
  (and the CLI transpose) do not deduplicate; `webgraph from arcs`,
  `symmetrize_seq`/`symmetrize_par`, the `map_*` transforms, and the CLI
  symmetrize with a permutation do. The CLI symmetrize without a
  permutation uses `symmetrize_sorted_par`, which does not deduplicate in
  the sort (`merge_forward` removes duplicates later).
- Silently collapsing repeated successors in the compressor.

## The traits

Decided with the user: the split into two traits, all the names below, the
`Default` bound, a provided `init` with the comment about reuse, the final
step with its own output type, and the location in `labels.rs` (already
re-exported from `traits` and hence from the prelude).

```rust
pub trait MergeLabels {
    type State: Default;
    type Output;

    /// Resets `state` to the empty state.
    ///
    /// The default implementation assigns `Self::State::default()`.
    /// Override it to reuse resources: for example, a merger
    /// collecting labels into a `Vec` can call `state.clear()`,
    /// keeping the allocation across runs of equal arcs.
    fn init(&self, state: &mut Self::State) {
        *state = Self::State::default();
    }

    /// Merges into `state` another state `other`.
    fn merge(&self, state: &mut Self::State, other: Self::State);

    /// Turns a fully merged state into the label of the arc.
    fn finish(&self, state: Self::State) -> Self::Output;
}

pub trait AddLabel<L>: MergeLabels {
    /// Adds `label` to `state`.
    fn add(&self, state: &mut Self::State, label: L);
}
```

The split is forced by the type system, not only a matter of taste: the
k-way merge (`KMergeIters`) sees states but never the input label type `L`,
so an `Iterator` implementation bounded by a trait generic in `L` would
leave `L` unconstrained (E0207, confirmed by the builder check in a scratch
crate). The k-way merge needs only `MergeLabels`; the batch encoding needs
`AddLabel<L>`. Users still implement both on one type and pass one value.

Semantics, as the sort uses them:

- **Batch stage.** Each batch encoding uses one `State` buffer, created
  with `Default` and reused for all the runs of equal `(src, dst)` in the
  batch (my call: one buffer per encoding call; the builder may hoist it).
  For each run it calls `init` once, then `add` once for each label of the
  run, *including the first*, then serializes the state by reference
  (`BitSerializer::serialize` takes `&SerType`). Every arc forms a run,
  possibly of length one. `finish` is never called in this stage: batch
  files hold partial states.
- **Merge stage.** For each pair, the state decoded from the first batch
  containing it becomes the accumulator, `merge` is called with the state
  from each other batch containing it, and `finish` is called exactly once,
  producing the label of the output arc. Pairs present in a single batch
  are finished too. `init` is not called in this stage.

Laws, to be documented: `init` yields an identity for `merge`; `merge` is
associative; adding a label gives the same result as merging with the
state obtained by adding that label alone to an initialized state. The
order of labels within a run is unspecified (the radix sort is on
`(src, dst)` only and unstable), and so is the order in which batches are
merged; `add` and `merge` may be order-dependent as long as `finish`
removes the dependency. For example, a collector can push in `add`, append
in `merge`, and sort (and possibly deduplicate) once in `finish`.

`merge` takes `other` by value, so collections can move or append its
contents; `finish` takes the state by value, so it can reuse its
allocation. Stable Rust has no defaults for associated types, so every
merger declares `Output` and writes `finish`, even when it is the identity.

Methods take `&self`, so one merger is shared by all sorting threads and
partitions (`M: Send + Sync`, and `Clone` if the k-way merge iterators need
their own copy).

### Prior art

Surveyed with the user (from knowledge of the APIs; only the Java WebGraph
one was checked against the source):

| Framework | start | add one input | merge two states | final step |
|---|---|---|---|---|
| Flink `AggregateFunction` | `createAccumulator` | `add` | `merge` | `getResult` |
| Beam `CombineFn` | `createAccumulator` | `addInput` | `mergeAccumulators` | `extractOutput` |
| Spark SQL `Aggregator` | `zero` | `reduce` | `merge` | `finish` |
| ClickHouse `IAggregateFunction` | `create` | `add` | `merge` | `insertResultInto` |
| DuckDB aggregates | `initialize` | `update` | `combine` | `finalize` |
| PostgreSQL `CREATE AGGREGATE` | `INITCOND` | `SFUNC` | `COMBINEFUNC` | `FINALFUNC` |
| Java `Collector` | `supplier` | `accumulator` | `combiner` | `finisher` |
| Java WebGraph `LabelMergeStrategy` | — | — | `merge` | — |

ClickHouse and PostgreSQL are the closest precedents: they serialize
partial aggregation states between stages, as our batch files do, and call
them states.

### Ready-made mergers

- `KeepOne` (`State = Output = L`, requires `L: Default`): keeps one label,
  which one being unspecified; `merge` keeps either; `finish` is the
  identity. `.dedup()` is this merger (my call): this adds an `L: Default`
  bound to labeled deduplication, which holds for `()` and for all
  practical label types; if the builder finds this costs anything on the
  unlabeled path, a dedicated policy is fine.
- `CountRepeats` (`State = Output = u64`, ignores the labels): counts the
  occurrences of each arc; `add` increments, `merge` adds, `finish` is the
  identity. Useful by itself (multigraph multiplicities) and the merger used
  by the tests.

## The `Vec` serializer

A serializer and deserializer for `Vec<T>` (user's decision), generic over
an inner serializer and deserializer for `T` and over the endianness, in
`webgraph/src/traits/bit_serde.rs` next to `FixedWidth`: the length coded
with γ, then the elements through the inner one. It works both for the
sort's batch files (native-endian) and for the compressor's label store.
Names are left to the builder.

## Where merging happens

1. **Batch encoding**: `encode_sorted_batch` of `GroupedGapsCodec`
   (`webgraph/src/utils/batch_codec/grouped_gaps.rs`) and `GapsCodec`
   (`gaps.rs`), replacing the keep-first logic of `DEDUP`. The batch length
   header and the outdegrees count distinct pairs, as with `DEDUP` today
   (`num_distinct_pairs`).
2. **K-way merge**: `KMergeIters::next`
   (`webgraph/src/utils/kmerge_iters.rs`). After extracting the minimum,
   while the new top of the heap has the same pair, extract it and merge;
   then finish. The heap is the lookahead, so `last_pair` is no longer
   needed. `count` must still be correct (it folds, as with `DEDUP` today).

The merge policy has a single source of truth: the batch codec owns it, and
the `KMergeIters` built from the codec's batches get it from the codec (in
`Sorter::into_split_iters`, `webgraph/src/utils/par_sort_iters.rs`), so the
two stages cannot disagree. Today the codec's `DEDUP` and the sorter's
`DEDUP` are independent parameters (`LabeledCodec` hardcodes `false` while
the sorter may deduplicate); that independence goes away.

Merging is complete per partition: all arcs with the same source end up in
the same partition, so every repeated pair meets in one `KMergeIters`.

## Types

Three label types are involved: the input label `L`, the state, and the
output label. With no merging, all three coincide.

- `BatchCodec` (`webgraph/src/utils/batch_codec/mod.rs`) distinguishes the
  in-memory label of the triples to sort (`Copy + Send + Sync + 'static`,
  as today, needed by the radix sort and by the batch-size computation in
  `Sorter::new`) from the state written to and decoded from disk
  (`Clone + Send + Sync + 'static`, not `Copy`). The codec's encoding side
  needs `L`, but `L` must not appear in the decoded iterator types that
  reach the public API (it can come from a trait or method parameter).
- `KMergeIters` yields the output label.
- The `ParSortedLabeledGraph` implementations
  (`webgraph/src/graphs/par_sorted_graph.rs`, `SequentialLabeling`,
  `IntoParLenders`, `IntoLender`, `Lending`, `make_labeled_lenders`) relax
  `L: Clone + Copy` to `L: Clone`. `arc_list_graph::NodeLabels` already
  requires only `L: Clone`.
- The serializer passed to the labeled sort methods serializes and
  deserializes the state; the compressor's label store serializes the
  output label. `LabelSerDeser` (`webgraph/src/traits/bit_serde.rs`)
  currently requires `SerType: Copy`; the state needs only
  `Clone + Send + Sync + 'static`.

## The API

```rust
// pairs: impl IntoIterator<Item = ((usize, usize), u32)>, with repeats.
let graph = ParSortedLabeledGraph::config()
    .merge_labels(CountRepeats)
    .sort_pairs(num_nodes, FixedWidth::<u64>::new(), pairs)?;
// graph has no repeated arcs; its labels are u64 counts.
BvComp::with_basename(&basename)
    .par_comp_labeled::<BE, _, _>(&graph, label_conf)?;
```

- `ParSortedLabeledGraphConf` replaces `const DEDUP: bool` with a
  merge-policy type parameter defaulting to no merging. `.dedup()` and
  `.merge_labels(merger)` change it, the same way `progress_logger` changes
  `PL`. `merge_labels` requires only `M: MergeLabels`; the terminal methods
  (`sort_graph`, `par_sort_graph`, `sort_pairs`, `par_sort_pair_iters`,
  `par_sort_pairs`, `sort_try_pairs`, `par_sort_try_pairs`), where `L` is
  known, require `M: AddLabel<L>`. They take pairs labeled with `L` and a
  serializer for the state. With no merging, the bounds are as today.
- `ParSortedGraphConf` (unlabeled) replaces `DEDUP` the same way and keeps
  `.dedup()`; it gets no `merge_labels` (its labels are `()`; counting
  unlabeled arcs is done with `ParSortedLabeledGraphConf` and `()` labels).
- The convenience constructors (`ParSortedLabeledGraph::from_pairs` and the
  like) use the default configuration, as today.
- The public aliases `SortedPairIter<DEDUP>` and `SortedLabeledIter<SD,
  DEDUP>` take the policy instead; public signatures mentioning
  `SortedPairIter<true>` (e.g., in `transform/symmetrize.rs`) change
  accordingly.
- The lower-level sorters `ParSortIters` and `ParSortPairs` lose their
  `DEDUP` parameter for labeled sorting, since the codec carries the policy.
  Unlabeled callers must still be able to request deduplication with no
  more code than today (`new_dedup` or an equivalent).
- Callers to update: `webgraph/src/transform/{map,symmetrize,transpose}.rs`,
  `cli/src/from/arcs.rs`, `cli/src/transform/symmetrize.rs`, and the tests
  and doc examples using `MergeLabel`, `DEDUP`, `new_dedup`,
  `DefaultBatchCodec<true>`, or `KMergeIters<_, _, true>`
  (`webgraph/tests/test_sort_merge.rs`, `webgraph/tests/test_batch_codec.rs`,
  and others the compiler finds).

## Reverting the compression-side merging

The end state, reached with new commits on top of `main` (no history
rewriting):

- `BvComp` and `BvCompZ` have no merge-policy type parameter; `new` takes
  the arguments it took in 0.7 (no merger); `push` and `extend` take items
  `(usize, SL::Label)` as in 0.7.
- `push` keeps the explicit errors, with one comparison per successor:
  `Repeated successor {succ} in successor list of node {node}` (now
  unconditional) and `Successor list of node {node} is not sorted: {succ}
  follows {prev}`.
- `BvCompConf` has no merge-policy parameter and no `merge_labels`; the
  turbofish arities of the labeled methods return to those of 0.7 (e.g.,
  `par_comp_labeled::<BE, _, _>`), and the call sites changed by the
  compression-side commits are restored.
- `NoMerge`, `WithMerge`, and `MergePolicy` leave `bvcomp.rs` (they may be
  reused or moved for the sorting policies).
- The compression-side merge tests (`mod merge_labels` in
  `webgraph/tests/test_bvgraph_roundtrip.rs`, `test_push_merges_runs` in
  `bvcomp.rs` and `bvcompz.rs`) are replaced by the sorting-side tests
  below; the error tests stay. `test_store::VecStore` in
  `webgraph/src/graphs/bvgraph/comp/mod.rs` goes if nothing uses it.
- Compression output stays bit-identical to 0.7 on graphs without repeats.

## Tests

Following the repository's conventions (`CLAUDE.md` and memory: test graphs
contain a cycle, nodes with outdegree 0, 1, 2, and 3, and nodes with
indegree 0, 1, 2, and 3; use `graph::eq_labeled`, `labels::eq_sorted`, and
the other ready-made comparison functions; no weak assertions):

1. **Counting repeats through the sort (requested by the user).** A labeled
   pair list with repeated arcs (some arcs once, some twice, some three
   times), sorted with `.merge_labels(CountRepeats)` and a small batch size
   (`MemoryUsage::BatchSize`), so that some repeats fall in the same batch
   (exercising `add`) and some in different batches (exercising `merge`).
   Sequential (`sort_pairs`) and parallel (`par_sort_pair_iters` with at
   least two iterators, so that repeats are split across threads). The
   sorted graph equals the expected labeled graph of multiplicities; it is
   also compressed with `par_comp_labeled`, reloaded, and compared.
2. **Collecting into a non-`Copy` state, with a final step of a different
   type.** A collector with `State = Vec<u32>` (push in `add`, append in
   `merge`, `init` with `clear`) and `Output = Box<[u32]>` (sorted in
   `finish`), through `ParSortedLabeledGraph` with small batches and the
   library's `Vec` serializer, compared with the expected labeled graph.
   The differing types make it a compile-time guarantee that `finish` is
   not applied in the batch stage; the expected sorted output checks that
   it is applied to every arc, repeated or not.
3. **Batch level.** Encoding a batch with repeats through a merging codec
   writes one triple per distinct pair (`BatchStats::total_triples`), and
   decoding yields the states.
4. **K-way merge.** `KMergeIters` over at least three iterators with
   overlapping pairs yields each pair once with the merged, finished label.
5. **`Vec` serializer** round trip, including the empty vector.
6. **Deduplication** keeps its behavior: the existing tests pass, with
   types updated.
7. **Compressor errors**: a repeated successor and an unsorted successor
   both return an error (not a panic), sequential and parallel, `BvComp`
   and `BvCompZ`.
8. Doc tests for `MergeLabels`, `AddLabel`, the ready-made mergers, the
   `Vec` serializer, and `ParSortedLabeledGraphConf::merge_labels`.

## Performance

- No regression on the no-merging and `.dedup()` paths: measure, before
  and after, on a real graph (`enwiki-2023`, the graph used for the
  previous measurement) both `webgraph transform transpose` (no merging)
  and `webgraph from arcs` (deduplication). All crates involved must be
  built with `target-cpu=native` (global instructions).
- No allocation per arc in the batch stage when the state is a `Vec`
  (buffer reuse through `init`).

## Documentation

- `MergeLabels` and `AddLabel`: the two stages, where `finish` is and is
  not called, the laws, the unspecified order and how `finish` can remove
  the dependency on it, reuse in `init`, ownership in `merge` and `finish`;
  a counting example and a collecting example. The ready-made mergers.
- `ParSortedLabeledGraphConf::merge_labels` and `::dedup`, the
  `par_sorted_graph` module docs, `KMergeIters`, the codecs, and the
  sorters: replace references to `DEDUP`, to `MergeLabel`, and to
  `BvCompConf::merge_labels`.
- `BvComp::push` and `BvCompZ::push`: successors must be sorted and
  distinct; repeated arcs are merged while sorting.
- `webgraph/CHANGELOG.md`: rewrite the unreleased 0.8.0 section. Drop the
  compression-side entries, and describe the new traits, the `Vec`
  serializer, the sorting API, the replacement of `DEDUP`, the relaxed
  `Copy` bound, and the compressor errors.

## Constraints

- Follow the repository guidelines (`CLAUDE.md`,
  <https://github.com/vigna/rust-dev-guidelines>): third-person docs,
  `anyhow` in this code path, `cargo fmt --all` after each batch of changes,
  `cargo clippy`, tests with `cargo test --workspace`.
- No `git stash` (use a worktree if isolation is needed).
- Work on `main` (user's decision for the previous round).

## Left to the builder

- The exact names of anything not named in this document (e.g., the
  in-memory and state label types of `BatchCodec`, the `Vec` serializer).
- How the merge policy is represented. Users pass their merger directly to
  `merge_labels`, and the no-merging path must not require `Default` on
  labels. A blanket implementation over `M: MergeLabels` overlapping with an
  implementation for a public "no merging" type is rejected by coherence,
  so, as in the compressor, this likely needs a sealed helper trait over two
  crate-local types (`NoMerge`, `WithMerge<M>`) or a similar device.
- How the codec's encoding side receives `L` (trait or method parameter),
  subject to `L` staying out of the decoded iterator types.
- How the sorters expose deduplication for unlabeled sorting, subject to
  the single-source-of-truth anti-goal.
- Whether to relax `LabelSerDeser` or to add a second trait for the state.
- How the compressor's error checks are structured once merging is gone
  (the `MergedSucc` adapter can shrink to a checking adapter).
