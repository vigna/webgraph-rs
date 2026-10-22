# MergeLabel: merging the labels of repeated arcs at compression time

## Intent

A labeled graph built by sorting labeled pairs may contain the same arc
`(src, dst)` several times with different labels. The motivating case is
swh-graph: an arc from a directory to a content carries one label per file
name, and the same content may appear under several names, so the sorted
pairs contain repeated arcs, one per name, that must become a single arc
whose label holds all the names.

The BV format stores successor lists as sets, so repeated successors must be
resolved before storage. Today nothing resolves them:

- `ParSortedLabeledGraph` with `DEDUP = false` passes repeated pairs through
  unchanged (`arc_list_graph::Succ` yields every pair);
- with `DEDUP = true`, `KMergeIters` and `GroupedGapsCodec` keep one
  occurrence and silently drop the other labels;
- `BvComp::push` pushes every successor into `succ_vec`, repeats included,
  and then fails by accident: in debug builds with
  `attempt to subtract with overflow` at `bvcomp.rs:232` (the residual gap
  `residuals[i] - residuals[i - 1] - 1`), in release builds with a ζ-code
  panic (`argument of integer logarithm must be positive`). Both were
  reproduced by feeding repeated labeled pairs through
  `ParSortedLabeledGraph::from_pairs` into `par_comp_labeled`.

An external user also observed that, were repeats accepted, `succ_vec` would
waste memory on repeated successors. In `BvComp` it is kept in the
`backrefs` circular buffer for `compression_window + 1` nodes; `BvCompZ`
buffers the successor lists of a whole chunk (`chunk_size` nodes) in a
`RaggedArray`. Labels themselves are never accumulated: `push_label` streams
them to the label bitstream.

The decision of how to combine labels belongs to the compressor, because it
is the stage that must turn arcs into sets and the stage where the stored
label type is decided. This mirrors Java's `LabelMergeStrategy`, except that
the merged label may have a different type from the input labels.

## Goals

- A public trait `MergeLabel<L>` describing how a run of labels on the same
  arc is merged into a single stored label, possibly of a different type.
- An opt-in way to configure a merger on the compressor, applying to all
  labeled compression entry points, sequential and parallel, for both
  `BvComp` and `BvCompZ`.
- Repeated successors become one successor in the compressor's buffers
  (`succ_vec` / `RaggedArray`) and one merged label in the label store:
  memory proportional to distinct successors, plus one reusable merge buffer.
- Without a merger, repeated successors (and successors that are not sorted)
  are reported as an explicit error, as in Java, instead of panicking by
  accident.

## Non-goals

- Merging labels in the sorting code (`ParSortIters`, `ParSortPairs`,
  `KMergeIters`, batch codecs). The sort's `DEDUP` keep-one behavior stays as
  it is.
- Relaxing the `Copy` bound on labels in `ParSortedLabeledGraph` (it comes
  from the radix sort of the batch codecs, `SD::SerType: Copy`). With merging
  in the compressor, the sort keeps carrying small `Copy` labels and the
  merged type is built only at compression time.
- A `BitSerializer` for `Vec`-like merged labels. Users merging into
  collections provide their own serializer (swh-graph already has one).
  Possible follow-up.
- Labeled union or symmetrization using the merger (Java's other users of
  `LabelMergeStrategy`). Possible follow-up; the trait should not preclude
  it.
- Semver compatibility: breaking changes are acceptable (few users at this
  time). The goal is the cleanest API, not a 0.7.x-compatible one.

## Anti-goals

- Silently collapsing repeated successors when no merger is configured.
- Any cost in the hot loop of `push` for the default (no merger) or
  unlabeled paths beyond the one comparison per successor needed to detect
  repeats and unsorted successors.
- Requiring `Default` (or any new bound) on label types for users who do not
  configure a merger.
- An allocation per arc when merging into a collection.

## The trait

Decided with the user (name, the different merged type, `Default`, a
provided `init` with no label, and the comment about reuse). The trait and
the ready-made mergers go in the existing `webgraph/src/traits/labels.rs`
(user's decision), which is already re-exported from `traits` and hence
from the prelude.

```rust
pub trait MergeLabel<L> {
    type Merged: Default;

    /// Resets `merged` to the empty merge.
    ///
    /// The default implementation assigns `Self::Merged::default()`.
    /// Override it to reuse resources: for example, a merger
    /// collecting labels into a `Vec` can call `merged.clear()`,
    /// keeping the allocation across runs of equal successors.
    fn init(&self, merged: &mut Self::Merged) {
        *merged = Self::Merged::default();
    }

    /// Merges `label` into `merged`.
    fn merge(&self, merged: &mut Self::Merged, label: L);
}
```

Semantics, as the compressor uses it:

- each compressor instance (one per thread in parallel compression) creates
  one `Merged` buffer with `Default` and reuses it for the whole compression;
- for each run of equal successors it calls `init` once, then `merge` once
  for each label of the run, *including the first*, then passes `&merged` to
  `StoreLabels::push_label`;
- runs are never empty;
- the order of labels within a run is unspecified (the batch codecs
  radix-sort on `(src, dst)` only, `batch_codec/mod.rs:257`, and the sort is
  unstable), so mergers should not depend on it. This must be documented.
  A merger that collects labels and needs a deterministic result must keep
  its collection ordered inside `merge`, since there is no end-of-run hook
  (my call: none for now; a provided `fn finish(&self, merged: &mut
  Self::Merged) {}` can be added later without breaking anyone).

Methods take `&self`, so one merger is shared by reference by all
compression threads (`M: Sync` for parallel compression).

## The API

Breaking changes are acceptable. The user-facing shape:

```rust
// No repeated successors: as today.
BvComp::with_basename(&basename)
    .par_comp_labeled::<BE, _, _>(&graph, label_conf)?;

// Repeated successors: labels are merged before storage.
BvComp::with_basename(&basename)
    .merge_labels(CountRepeats)          // a MergeLabel<L>
    .par_comp_labeled::<BE, _, _>(&graph, label_conf)?;
    // label_conf's serializer serializes CountRepeats::Merged
```

- `BvCompConf` gains a type parameter for the merger, with a default
  meaning "no merger". `merge_labels(merger)` changes it, the same way
  `progress_logger` changes `PL`. The setting applies to
  `comp_labeled_graph`, `comp_labeled_lender`, and `par_comp_labeled`, for
  both `BvComp` and `BvCompZ`.
- With a merger `M`, the labeled methods accept lenders whose label type is
  `(usize, L)` with `M: MergeLabel<L, Merged = <SLC::StoreLabels as
  StoreLabels>::Label>`. Without a merger, the bounds are as today
  (`L = <SLC::StoreLabels as StoreLabels>::Label`), with no new bounds on
  `L`.
- The unlabeled methods (`comp_graph`, `comp_lender`, `par_comp`) ignore the
  merger and always use the no-merger path. They are implemented today on
  top of the labeled ones via `UnitLabelGraph`, `UnitLabelLender`, and
  `UnitLabelParLenders`; that must keep working whatever merger is
  configured.
- `BvComp` and `BvCompZ` (low-level API) take the merger at construction
  (`new`), and `push`/`extend` accept `(usize, L)` items as above. The
  merge buffer lives on the compressor: its type is fixed by the label
  store (`M::Merged` must equal `SL::Label`), not by the input label type.
  The call sites of `new` to update are in `impls.rs` (two each),
  `webgraph/src/fuzz/bvcomp_and_read.rs` (`BvComp`), and
  `webgraph/tests/test_bvcomp.rs` (`BvCompZ`).
- No CLI changes are needed: the CLI does not compress labeled graphs, and
  `BvCompConf<PL>` in `cli/src/lib.rs` keeps compiling if the new merger
  type parameter has a default. Existing unlabeled sorting paths in the CLI
  and in `transform` already call `.dedup()`, and no existing test relies
  on repeated successors being accepted, so the new error does not affect
  current users.
- Changing the turbofish arity of the public methods is acceptable if it
  makes the API cleaner, but doc examples and tests must be updated
  accordingly.

## Errors

The same comparison with the previous successor that detects runs (`succ ==
prev`) also detects unsorted input (`succ < prev`). `push` returns an error
(an `anyhow` error, as `push` already returns `anyhow::Result`) in two
cases:

- a repeated successor with no merger configured: the message follows Java
  (`BVGraph.java:2201`): `Repeated successor {succ} in successor list of
  node {node}`;
- a successor smaller than the previous one, with or without a merger:
  `Successor list of node {node} is not sorted: {succ} follows {prev}` (or
  similar).

In parallel compression, a worker's error already surfaces as
`Compression thread {id} failed` with the cause attached; no change needed.

## Ready-made mergers

My call: ship a minimal set next to the trait, documented with examples:

- `KeepOne` (named by the user; `Merged = L`, requires `L: Default`): keeps
  one label of the run, which one being unspecified, since so is the order
  within a run (the implementation can simply overwrite, keeping the last
  one it sees). It is the opt-in replacement for silent collapsing.
- `CountRepeats` (`Merged` an unsigned integer, ignores the labels): counts
  the occurrences of each arc. Useful by itself (multigraph multiplicities)
  and the merger used by the tests.

A `Vec`-collecting merger is left out because there is no `Vec` serializer
to store its output (see non-goals); the builder may add one if trivial, as
`init` overriding with `clear()` is the canonical example of the reuse
comment.

## Tests

Following the repository's conventions (see `CLAUDE.md` and memory: test
graphs contain a cycle, nodes with outdegree 0, 1, 2, and 3, and nodes with
indegree 0, 1, 2, and 3; use `graph::eq_labeled` and the other ready-made
comparison functions; no weak assertions):

1. **Counting repeats (requested by the user).** A labeled arc list with
   repeated arcs (some arcs once, some twice, some three times), merged with
   `CountRepeats`, compressed and reloaded; the result must equal the
   expected labeled graph whose labels are the multiplicities. Covered
   combinations:
   - sequential (`comp_labeled_lender` or `comp_labeled_graph`) and parallel
     (`par_comp_labeled`);
   - `BvComp` and `BvCompZ`;
   - for the parallel case, input from `ParSortedLabeledGraph::from_pairs`
     with `DEDUP = false`, the exact path of the original report.
2. **Repeated successor without a merger** returns an error (not a panic),
   sequential and parallel, for `BvComp` and `BvCompZ`.
3. **Unsorted successors** return an error, with and without a merger.
4. Existing tests and doc examples keep passing after the API change.

## Documentation

- Docs for `MergeLabel` (with a short counting example), for the ready-made
  mergers, and for `BvCompConf::merge_labels`.
- `BvComp::push` / `BvCompZ::push`: successors must be sorted; repeated
  successors are merged by the configured merger, or rejected.
- `ParSortedLabeledGraph`, `ParSortedLabeledGraphConf::dedup`, and the
  module docs of `par_sorted_graph`: with `DEDUP = false` repeated pairs are
  kept, and compressing them requires a merger.
- `par_comp_labeled` and the sequential labeled methods: mention merging.
- `webgraph/CHANGELOG.md`: a new unreleased section describing the new
  trait, the new builder method, the breaking changes to `BvComp::new`,
  `BvCompZ::new`, `BvCompConf`, and the new errors.

## Constraints

- Follow the repository guidelines (`CLAUDE.md`,
  <https://github.com/vigna/rust-dev-guidelines>): third-person docs,
  `anyhow` in this code path, `cargo fmt --all` after each batch of changes,
  `cargo clippy`, tests with `cargo test -p webgraph`.
- No `git stash` (use a worktree if isolation is needed).
- Do not regress compression speed on the default path; a single comparison
  per successor is the budget.

## Left to the builder

- The exact names of anything not named in this document.
- How "no merger" is represented and how the compressor dispatches between
  the no-merger path and the merging path. Constraint: users pass their
  `MergeLabel` value directly to `merge_labels`, and the no-merger path must
  not require `Default` on labels. Note that a blanket implementation over
  `M: MergeLabel<L>` overlapping with an implementation for a public "no
  merger" type is rejected by coherence (downstream crates may implement
  `MergeLabel<TheirType>` for any type), so the dispatch likely needs a
  sealed helper trait over two distinct crate-local types, or a similar
  device.
- How the run grouping is shared between `BvComp::push` and
  `BvCompZ::push` (my suggestion: one iterator adapter that wraps the
  incoming `(succ, label)` iterator, yields each successor once, and pushes
  the run's merged label to the store; `BvCompZ::push` already feeds a lazy
  iterator to `RaggedArray::push`).
- Whether to add a `Vec`-collecting merger.
