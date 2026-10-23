# MergeLabels While Sorting: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Merge the labels of repeated arcs while sorting (batch stage and
k-way merge), replacing `const DEDUP: bool` with a merge-policy type, and
return the compressor to its 0.7 API while keeping its explicit errors.

**Architecture:** Two user traits (`MergeLabels`, `AddLabel<L>`) in
`traits/labels.rs`; three sealed policy types (`NoMerge`, `Dedup`,
`WithMerge<M>`) in a new `utils/merge_policy.rs`. Batch codecs own the
policy (batch stage, through `AddPolicy<L>`) and hand it to `KMergeIters`
(k-way stage, through `MergePolicy<S>`). The sorted-graph configurations
carry the policy and set it with `.dedup()` and `.merge_labels(m)`.

**Tech Stack:** Rust (workspace `webgraph`, `webgraph-cli`), dsi-bitstream,
rdst, dary_heap, rayon.

**Spec:** `docs/superpowers/specs/2026-10-22-merge-label-design.md`

## Global Constraints

- Work on `main`, new commits only; never `git stash` (use a worktree).
- `cargo fmt --all` after every batch of changes; `cargo clippy --workspace
  --all-targets` clean; `cargo test --workspace` green at the end of every
  task.
- Docs in third person ("Merges …"); no tests for `Debug`/`Display`; use
  `graph::eq`, `graph::eq_labeled`, `labels::eq_sorted`; no weak assertions.
- Test graphs contain a cycle, nodes with outdegree 0, 1, 2, 3 and nodes
  with indegree 0, 1, 2, 3.
- Error messages, verbatim: `Repeated successor {succ} in successor list of
  node {node}` and `Successor list of node {node} is not sorted: {succ}
  follows {prev}`.
- No cost on the no-merging and `.dedup()` paths: the benchmark of Task 7
  is the gate.
- Commit messages end with `Co-Authored-By: Claude Opus 5.5
  <noreply@anthropic.com>`. Do not push.

## Baseline (already measured, before any change)

Binary built from `6b79b49f` in the worktree
`$SCRATCH/baseline` (`$SCRATCH` = this session's scratchpad), with
`target-cpu=native` verified in the fingerprint. Script:
`$SCRATCH/bench/bench.sh BINARY LABEL RUNS`, which appends to
`$SCRATCH/bench/results.txt`. On enwiki-2023 (6.6M nodes, 165M arcs),
5 runs each: `transform transpose` median **4.29 s** (4.22–4.44);
`from arcs` (dedup) median **20.71 s** (20.39–20.93).

## Shared Interfaces

These are the names every task uses.

```rust
// webgraph/src/traits/labels.rs (re-exported by traits and the prelude)
pub trait MergeLabels {
    type State: Default;
    type Output;
    fn init(&self, state: &mut Self::State) { *state = Self::State::default(); }
    fn merge(&self, state: &mut Self::State, other: Self::State);
    fn finish(&self, state: Self::State) -> Self::Output;
}
pub trait AddLabel<L>: MergeLabels {
    fn add(&self, state: &mut Self::State, label: L);
}
pub struct KeepOne<L>(PhantomData<fn() -> L>); // Clone, Copy, Debug, Default for all L; new()
pub struct CountRepeats;                         // Clone, Copy, Debug, Default

// webgraph/src/utils/merge_policy.rs (pub mod; pub use in utils)
pub struct NoMerge;          // Clone, Copy, Debug, Default
pub struct Dedup;            // Clone, Copy, Debug, Default
pub struct WithMerge<M>(pub M); // Clone, Copy, Debug, Default (derived)
// Sealed (private supertrait). Doc-hidden items: COLLAPSES, merge, finish,
// Buffer, buffer, state.
pub trait MergePolicy<S>: Sealed + Clone + Send + Sync {
    type Output;
    const COLLAPSES: bool;                       // false only for NoMerge
    fn merge(&self, state: &mut S, other: S);    // Dedup: drops other
    fn finish(&self, state: S) -> Self::Output;  // identity except WithMerge
}
pub trait AddPolicy<L>: Sealed + Clone + Send + Sync {
    type State;                                  // L, except WithMerge: M::State
    type Buffer;                                 // (), except WithMerge: M::State
    const COLLAPSES: bool;
    fn buffer(&self) -> Self::Buffer;
    /// Returns the state to write for `run`, a nonempty run of equal
    /// pairs (of length one if !COLLAPSES).
    fn state<'a>(&self, buffer: &'a mut Self::Buffer,
                 run: &'a [((usize, usize), L)]) -> &'a Self::State;
}
// impls: NoMerge and Dedup for every S / every L: Copy;
// WithMerge<M> for M: MergeLabels<State = S> + Clone + Send + Sync, and for
// M: AddLabel<L> + Clone + Send + Sync with L: Copy.

// webgraph/src/utils/kmerge_iters.rs
pub struct KMergeIters<I, T = (), P = NoMerge, A: Send + Sync = ()>;
// Item = ((usize, usize), P::Output) when P: MergePolicy<T>
// new(iters) and with_anchor(iters, anchor) require P: Default;
// with_policy_and_anchor(iters, policy: P, anchor: A) for any P.

// webgraph/src/utils/batch_codec/mod.rs
pub trait BatchCodec: Send + Sync {
    type Label: Copy + Send + Sync + 'static;   // in-memory, radix-sorted
    type State: Send + Sync + 'static;           // written and decoded
    type Policy: MergePolicy<Self::State>;
    type DecodedBatch: IntoIterator<Item = ((usize, usize), Self::State),
                                    IntoIter: Send + Sync + Clone>;
    type EncodedBatchStats: BatchStats;
    fn merge_policy(&self) -> Self::Policy;
    // encode_sorted_batch / encode_batch take [((usize, usize), Self::Label)]
    // decode_batch unchanged
}
pub type DefaultBatchCodec<P = NoMerge> =
    GroupedGapsCodec<NE, (), GAMMA, GAMMA, DELTA, P>;
// GroupedGapsCodec<E, SD, OUTDEGREE_CODE, SRC_CODE, DST_CODE,
//                  P = NoMerge, L = <SD as BitSerializer<E, BitWriter<E>>>::SerType>
// GapsCodec<E, S, D, SRC_CODE, DST_CODE, P = NoMerge, L = S::SerType>
// new(sd) / new(s, d): P = NoMerge, L = SerType (unchanged call sites);
// with_policy(sd, policy) / with_policy(s, d, policy): any P, L.
// Default when the (de)serializers and P are Default.

// Sorters (ParSortIters, ParSortPairs): <P = NoMerge>; new() -> Self,
// new_dedup() -> Self<Dedup>; unlabeled methods for P in {NoMerge, Dedup}
// return SplitIters<utils::SortedPairIter<P>>; labeled methods
// (sort_labeled, sort_labeled_seq, try_sort_labeled_seq, try_sort_labeled)
// exist only on the NoMerge sorter (the codec carries the policy) and return
// SplitIters<KMergeIters<CodecIter<C>, C::State, C::Policy, Arc<TempDir>>>.

// webgraph/src/graphs/par_sorted_graph.rs
pub type SortedPairIter<P = NoMerge> =
    KMergeIters<CodecIter<DefaultBatchCodec<P>>, (), P, Arc<TempDir>>;
pub type SortedLabeledIter<SD, P = NoMerge> = KMergeIters<
    GroupedGapsIter<NE, SD, GAMMA, GAMMA, DELTA>,
    <SD as BitSerializer<NE, BitWriter<NE>>>::SerType, P, Arc<TempDir>>;
pub struct ParSortedLabeledGraphConf<PL = Option<ProgressLogger>, P = NoMerge>;
pub struct ParSortedGraphConf<PL = Option<ProgressLogger>, P = NoMerge>;
```

## Review Focus

1. The rayon path (`ParSortPairs`, i.e., `par_sort_pairs` and
   `par_sort_try_pairs`) with a merger must give the same graph as the
   sequential path. Pinned by `test_count_repeats_par_sort_pairs` (Task 6).
2. Without a policy, every repeat survives with its own label; with
   `.dedup()`, one arc per pair survives with one of its labels. Pinned by
   `test_no_merge_keeps_repeats` and `test_dedup_keeps_one` (Task 6).
3. `count()` on a collapsing `KMergeIters` counts distinct pairs;
   `ExactSizeIterator` exists only for `NoMerge`. Pinned in
   `test_kmerge_iters_merge` (Task 5).
4. Partitions and batches that receive no arcs (nodes with outdegree 0 at
   the end of a partition) with a merger. Pinned by the test graph of Task 6
   sorted with `num_lenders(3)`.
5. A non-`Copy` state survives cloning of the sorted iterators (a
   `ParSortedLabeledGraph` iterated twice). Pinned in `test_collect_names`
   (Task 6), which compares twice.

---

### Task 1: Return the compressor to its 0.7 API, keeping the errors

**Files:**
- Restore from `a26746a0`: `webgraph/src/graphs/bvgraph/comp/{bvcomp.rs,bvcompz.rs,impls.rs,mod.rs}`,
  `webgraph/src/fuzz/bvcomp_and_read.rs`, `webgraph/src/labels/bitstream/labeling.rs`,
  `webgraph/src/traits/store.rs`, `webgraph/src/graphs/par_sorted_graph.rs`
  (its diff since `a26746a0` is documentation only), `webgraph/tests/test_bvcomp.rs`,
  `webgraph/tests/test_par_bvcomp.rs`
- Modify: `webgraph/tests/test_bvgraph_roundtrip.rs` (drop `mod merge_labels`, keep its two error tests)

**Interfaces:**
- Produces: `BvComp::push` / `BvCompZ::push` taking `IntoIterator<Item = (usize, SL::Label)>`
  and returning the two errors; `BvCompConf` with no policy parameter;
  `par_comp_labeled::<BE, _, _>`.

- [ ] **Step 1: Restore the files**

Run: `git checkout a26746a0 -- <the files above>`. `labels.rs` keeps
`MergeLabel` for now (Task 2 replaces it); it is unused after this step.

- [ ] **Step 2: Write the failing tests**

In `bvcomp.rs` `mod test` and in `bvcompz.rs` `mod test`, with a compressor
writing to `std::io::sink()` and a `()` label store (adapt the
`sink_bvcomp` helper of `6b79b49f` without the policy argument; no
`VecStore`):

```rust
#[test]
fn test_push_repeated() -> anyhow::Result<()> {
    let err = sink_bvcomp()?.push([(1, ()), (1, ())]).unwrap_err();
    assert_eq!(err.to_string(), "Repeated successor 1 in successor list of node 0");
    Ok(())
}
#[test]
fn test_push_unsorted() -> anyhow::Result<()> {
    let err = sink_bvcomp()?.push([(2, ()), (1, ())]).unwrap_err();
    assert_eq!(err.to_string(), "Successor list of node 0 is not sorted: 1 follows 2");
    Ok(())
}
```

In `test_bvgraph_roundtrip.rs`, a new `mod successor_errors` with
`test_repeated_successor` and `test_unsorted_successor`, taken from
`test_repeated_without_merger` and `test_unsorted_successors` of
`mod merge_labels` without the merger: a labeled graph with arcs `(0, 1)`
twice (resp. `(0, 2), (0, 1)`), compressed with `comp_labeled_graph` and
`par_comp_labeled`, for `BvComp` and `BvCompZ`; each returns `Err` whose
chain contains the message above. Delete the rest of `mod merge_labels`.

- [ ] **Step 3: Run them to verify they fail**

Run: `cargo test -p webgraph push_ successor_errors`
Expected: the four unit tests panic on `unwrap_err` of an `Ok`, and the
roundtrip tests fail (no error is returned).

- [ ] **Step 4: Add the checks to `push`**

In both `push` methods, inside the loop over `succ_iter`, keep a local
`min_succ` (initially 0): if `succ < min_succ`, return the "Repeated" error
when `succ == min_succ - 1`, the "not sorted" error otherwise (`prev =
min_succ - 1`); then `min_succ = succ + 1`. One comparison per successor.
Document in both `push` docs: successors must be sorted and distinct, else
an error is returned; repeated arcs can be merged while sorting
(`ParSortedLabeledGraphConf::merge_labels`, link added in Task 6).

- [ ] **Step 5: Run the tests**

Run: `cargo test -p webgraph push_ successor_errors`, then `cargo test --workspace`
Expected: all pass.

- [ ] **Step 6: Commit**

`git commit -m "Compressor back to the 0.7 API; repeated and unsorted successors stay errors"`

### Task 2: `MergeLabels` and `AddLabel`

**Files:**
- Modify: `webgraph/src/traits/labels.rs` (replace the `MergeLabel` section)

**Interfaces:**
- Produces: `MergeLabels`, `AddLabel<L>`, `KeepOne<L>`, `CountRepeats` (see
  Shared Interfaces). `KeepOne<L>`: `State = Output = L` (`L: Default`),
  `add` assigns, `merge` keeps `state`, `finish` is the identity.
  `CountRepeats`: `State = Output = u64`, `add` increments, `merge` adds,
  `finish` is the identity, for every `L`.

- [ ] **Step 1: Write the doc tests first**

On `MergeLabels`, two examples: a counter (as `CountRepeats`), and a
collector with `State = Vec<u32>`, `Output = Box<[u32]>` (`init` clears,
`add` pushes, `merge` appends, `finish` sorts and calls
`into_boxed_slice`), each driven by hand through the two stages: e.g., for
the collector, two states built with `init` + `add` (labels `[3, 1]` and
`[2]`), merged, finished: `assert_eq!(&*out, &[1, 2, 3])`. On `KeepOne`:
`add` 7 then `finish` gives 7. On `CountRepeats`: states 2 and 3 merged
then finished give 5.

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p webgraph --doc labels`
Expected: compile errors (`MergeLabels` not found).

- [ ] **Step 3: Replace `MergeLabel`, `KeepOne`, `CountRepeats`**

Docs, per the spec's "Semantics" and "Laws": the two stages; `finish` is
never called in the batch stage and exactly once per output arc after the
k-way merge; `init` gives an identity for `merge`, `merge` is associative,
`add` is consistent with `merge`; the order of labels and of states is
unspecified and `finish` can remove the dependency on it; reuse in `init`;
ownership in `merge` and `finish`; methods take `&self`, shared by all
threads. Mergers are set with `ParSortedLabeledGraphConf::merge_labels`
(plain-text mention until Task 6 adds it; no broken intra-doc links).

- [ ] **Step 4: Run the tests**

Run: `cargo test -p webgraph --doc labels` then `cargo test --workspace`
Expected: pass.

- [ ] **Step 5: Commit**

`git commit -m "MergeLabels and AddLabel replace MergeLabel"`

### Task 3: The `Vec` serializer

**Files:**
- Modify: `webgraph/src/traits/bit_serde.rs`
- Test: `webgraph/tests/test_serde.rs`

**Interfaces:**
- Produces: `pub struct VecOf<S>(pub S)` (Clone, Copy, Debug, Default):
  `BitSerializer<E, BW>` for `BW: BitWrite<E> + GammaWrite<E>`, `S:
  BitSerializer<E, BW>`, `SerType = Vec<S::SerType>`;
  `BitDeserializer<E, BR>` for `BR: BitRead<E> + GammaRead<E>`, `DeserType
  = Vec<S::DeserType>`. Format: length in γ, then the elements. `name()`:
  `format!("VecOf<{}>", inner name)`.

- [ ] **Step 1: Write the failing test**

`test_vec_of` in `test_serde.rs`: for `BE` and `LE`, serialize with
`VecOf(FixedWidth::<u32>::new())` the vectors `vec![]`, `vec![7]`,
`vec![3, 0, u32::MAX]` into one `BufBitWriter` over a `Vec<u64>`, recording
the returned bit counts; read them back in order and assert equality with
the inputs and that the bit counts equal `len_gamma(len) + 32 * len`.

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p webgraph --test test_serde vec_of`
Expected: compile error (`VecOf` not found).

- [ ] **Step 3: Implement `VecOf` next to `FixedWidth`, with a doc example.**

Deserialization uses `Vec::with_capacity(len)`.

- [ ] **Step 4: Run the test**

Run: `cargo test -p webgraph --test test_serde`
Expected: pass.

- [ ] **Step 5: Commit**

`git commit -m "VecOf: a serializer for vectors of labels"`

### Task 4: Merge policies

**Files:**
- Create: `webgraph/src/utils/merge_policy.rs` (SPDX header as the
  neighbors, 2026 Sebastiano Vigna)
- Modify: `webgraph/src/utils/mod.rs` (`pub mod merge_policy; pub use merge_policy::*;`)

**Interfaces:**
- Consumes: `MergeLabels`, `AddLabel<L>` (Task 2).
- Produces: `NoMerge`, `Dedup`, `WithMerge<M>`, `MergePolicy<S>`,
  `AddPolicy<L>` exactly as in Shared Interfaces.

- [ ] **Step 1: Write the failing unit tests (in the module)**

```rust
#[test]
fn test_add_policies() {
    let run = [((0, 1), 'a'), ((0, 1), 'b'), ((0, 1), 'c')];
    assert_eq!(*Dedup.state(&mut Dedup.buffer(), &run), 'a');
    assert_eq!(*NoMerge.state(&mut NoMerge.buffer(), &run[1..2]), 'b');
    let p = WithMerge(CountRepeats);
    let mut buffer = AddPolicy::<char>::buffer(&p);
    assert_eq!(*p.state(&mut buffer, &run), 3);
    assert_eq!(*p.state(&mut buffer, &run[..1]), 1); // buffer reused, init called
}
#[test]
fn test_merge_policies() {
    let p = WithMerge(CountRepeats);
    let mut s = 2;
    MergePolicy::<u64>::merge(&p, &mut s, 3);
    assert_eq!(p.finish(s), 5);
    let mut s = 'x';
    Dedup.merge(&mut s, 'y');
    assert_eq!(Dedup.finish(s), 'x');
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p webgraph --lib merge_policy`
Expected: compile errors (module missing).

- [ ] **Step 3: Implement the module.**

Module docs: the three policies and where each stage happens (link
`KMergeIters`, `BatchCodec`, `ParSortedLabeledGraphConf::dedup`); the
traits are sealed. `NoMerge::merge` is `unreachable!()` (never called
because `COLLAPSES` is false); all methods `#[inline(always)]`.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p webgraph --lib merge_policy`, then `cargo test --workspace`
Expected: pass.

- [ ] **Step 5: Commit**

`git commit -m "Merge policies for sorting: NoMerge, Dedup, WithMerge"`

### Task 5: Replace `DEDUP` with merge policies across the sorting stack

One atomic migration: `KMergeIters`, `BatchCodec`, both codecs, both
sorters, `Sorter`, the aliases, the configurations, the transforms, the
CLI, and the tests must change together to compile. Behavior of the
no-merging and `.dedup()` paths is unchanged.

**Files:**
- Modify: `webgraph/src/utils/kmerge_iters.rs`, `webgraph/src/utils/batch_codec/{mod,grouped_gaps,gaps}.rs`,
  `webgraph/src/utils/{par_sort_iters,par_sort_pairs,mod}.rs`,
  `webgraph/src/graphs/par_sorted_graph.rs`,
  `webgraph/src/transform/{map,symmetrize,transpose,perm}.rs`, and whatever
  else the compiler reports (`cli/src/from/arcs.rs`,
  `cli/src/transform/symmetrize.rs` should need no change)
- Test: `webgraph/tests/test_sort_merge.rs`, `webgraph/tests/test_batch_codec.rs`

**Interfaces:**
- Consumes: Task 4 policies, Task 2 `CountRepeats`.
- Produces: everything under "Shared Interfaces" for `KMergeIters`,
  `BatchCodec`, codecs, sorters, aliases, and configurations. Terminal
  methods of `ParSortedLabeledGraphConf<PL, P>` gain a label parameter `L`
  (pairs are labeled with `L`) with bounds `SD: LabelSerDeser, L: Copy +
  Send + Sync + 'static, P: AddPolicy<L, State = SD::SerType> +
  MergePolicy<SD::SerType>`, and return
  `ParSortedLabeledGraph<SortedLabeledIter<SD, P>>`. `ParSortedGraphConf<PL,
  P>` terminal methods return `ParSortedGraph<SortedPairIter<P>>`.
  `.dedup()` returns the `Dedup` configuration on both.

- [ ] **Step 1: Write the failing tests**

`test_sort_merge.rs`: replace `KMergeIters<_, _, false>`/`<_, _, true>`
with `NoMerge`/`Dedup`, `new_dedup().sort_labeled(DefaultBatchCodec<true>)`
with `new().sort_labeled(DefaultBatchCodec::<Dedup>::default())`, and add:

```rust
#[test]
fn test_kmerge_iters_merge() {
    let iters = || vec![
        vec![((0, 1), 1_u64), ((0, 2), 1), ((2, 0), 2)],
        vec![((0, 1), 2), ((1, 1), 1)],
        vec![((0, 1), 3), ((2, 0), 1), ((3, 3), 1)],
    ].into_iter().map(Vec::into_iter);
    let merged = KMergeIters::<_, u64, WithMerge<CountRepeats>>::new(iters());
    assert_eq!(merged.collect::<Vec<_>>(),
        [((0, 1), 6), ((0, 2), 1), ((1, 1), 1), ((2, 0), 3), ((3, 3), 1)]);
    assert_eq!(KMergeIters::<_, u64, WithMerge<CountRepeats>>::new(iters()).count(), 5);
    assert_eq!(KMergeIters::<_, u64, Dedup>::new(iters()).count(), 5);
    assert_eq!(KMergeIters::<_, u64>::new(iters()).len(), 8);
}
```

`test_batch_codec.rs`: `test_grouped_gaps_codec_merge` and
`test_gaps_codec_merge`: encode with `with_policy(FixedWidth::<u64>::new(),
WithMerge(CountRepeats))` (for `GapsCodec`, serializer and deserializer
both `FixedWidth::<u64>::new()`) and `L = u32` the unsorted batch
`[((2, 3), 9), ((0, 1), 9), ((2, 3), 9), ((0, 1), 9), ((0, 2), 9), ((0, 1), 9)]`;
assert `stats.total_triples() == 3` and that decoding yields
`[((0, 1), 3), ((0, 2), 1), ((2, 3), 2)]` with `len() == 3`.

- [ ] **Step 2: Verify they fail**

Run: `cargo test -p webgraph --test test_sort_merge --test test_batch_codec`
Expected: compile errors on `NoMerge`/`Dedup`/`WithMerge` type arguments
and `with_policy`.

- [ ] **Step 3: `KMergeIters`**

Replace `DEDUP` and `last_pair` with a `policy: P` field. `next`: take the
minimum as today; if `!P::COLLAPSES` return `(pair, policy.finish(state))`;
otherwise, while `heap.peek()` has the same pair, take its head the same
way (replace with the tail's next or pop) and `policy.merge(&mut state,
other)`; then return `(pair, policy.finish(state))`. Use `peek` for the
comparison and `peek_mut` only to take, so an unchanged top is not sifted.
`count`: fold when `P::COLLAPSES`, sum of lengths otherwise.
`ExactSizeIterator` only for `NoMerge`. Constructors and trait impls as in
Shared Interfaces. Update the type docs (no `DEDUP`; the three policies).

- [ ] **Step 4: `BatchCodec` and codecs**

Trait as in Shared Interfaces; `create_batch(path, batch, P::COLLAPSES)`.
`GroupedGapsCodec::encode_sorted_batch`: one `buffer` per call
(`self.policy.buffer()`); per source group, `outdegree =
num_distinct_pairs(group)` if `COLLAPSES`, else the group size; per run
(maximal equal `dst` if `COLLAPSES`, single triple otherwise) write the
`dst` gap and `self.sd.serialize(self.policy.state(&mut buffer, run), ..)`.
`GapsCodec` the same without outdegrees. `merge_policy()` returns
`self.policy.clone()`. Both codec docs: replace `DEDUP` by the policy and
`L`.

- [ ] **Step 5: Sorters and `Sorter`**

`impl_sorter_conf!` with `P` and a `PhantomData<P>` field; `new_dedup`
returns the `Dedup` sorter. `Sorter::into_split_iters` builds each
partition with `KMergeIters::with_policy_and_anchor(batches,
self.batch_codec.merge_policy(), Arc::clone(&self.tmp_dir))`. Labeled
methods move to `impl ParSortIters` / `impl ParSortPairs` (NoMerge only;
docs say the policy is the codec's). `strip_labels` and
`utils::SortedPairIter` take `P`.

- [ ] **Step 6: Aliases, configurations, transforms**

`par_sorted_graph.rs`: `LabeledCodec<SD, P, L>` built with
`with_policy(sd, self.policy)`; configurations hold `policy: P`;
`into_par_sort_iters`/`into_par_sort_pairs` return the `NoMerge` sorters.
`transform/*`: `SortedPairIter<true>` → `SortedPairIter<Dedup>`, `const
DEDUP` → `P`. Keep `ParSortedLabeledGraph` bounds as they are (`Copy` is
relaxed in Task 6).

- [ ] **Step 7: Run everything**

Run: `cargo fmt --all && cargo clippy --workspace --all-targets && cargo test --workspace`
Expected: no warnings; all tests pass, including the three new ones.

- [ ] **Step 8: Commit**

`git commit -m "Merge policies replace DEDUP in the sorting code"`

### Task 6: `merge_labels`, non-`Copy` labels, end-to-end tests

**Files:**
- Modify: `webgraph/src/graphs/par_sorted_graph.rs`, `webgraph/src/traits/bit_serde.rs`
  (`LabelSerDeser`), `webgraph/src/transform/transpose.rs`,
  `webgraph/src/graphs/bvgraph/comp/{bvcomp,bvcompz}.rs` (doc link)
- Create: `webgraph/tests/test_merge_labels.rs`

**Interfaces:**
- Consumes: Tasks 2–5.
- Produces: `ParSortedLabeledGraphConf::merge_labels<M: MergeLabels>(self,
  merger: M) -> ParSortedLabeledGraphConf<PL, WithMerge<M>>`;
  `LabelSerDeser` with `SerType: Clone + Send + Sync + 'static`;
  `ParSortedLabeledGraph` implementations with `L: Clone` (no `Copy`).

- [ ] **Step 1: Write the failing tests** (`test_merge_labels.rs`)

Base graph (6 nodes): arcs `0→1, 0→2, 0→3, 1→2, 1→3, 2→3, 3→1, 5→0`
(cycle 1→3→1; outdegrees 3, 2, 1, 1, 0, 1; indegrees 1, 2, 2, 3, 0, 0).
Multiplicity of `(s, d)`: `1 + (s + d) % 3`. Pairs: first one copy of
every arc in the order above, then the remaining copies of each arc,
adjacent, arcs in reverse order; label of every copy `(10 * s + d) as
u32`, except in `test_collect_names`, where the `k`-th copy has label
`100 * s + 10 * d + k`. Configuration: `num_lenders(3)` and
`memory_usage(MemoryUsage::BatchSize(4))`.

- `test_count_repeats_sort_pairs`: `.merge_labels(CountRepeats).sort_pairs(6,
  FixedWidth::<u64>::new(), pairs)`; `graph::eq_labeled` with the
  `LabeledVecGraph` of multiplicities (`u64`). Then
  `BvComp::with_basename(..).par_comp_labeled::<BE, _, _>(&graph,
  BitStreamStoreLabelsConf::<BE, _>::new(FixedWidth::<u64>::new()))`, reload
  as in `mod labeled` of `test_bvgraph_roundtrip.rs`, and compare again.
- `test_count_repeats_par_sort_pair_iters`: same, pairs split into two
  iterators (even and odd positions).
- `test_count_repeats_par_sort_pairs`: same, `pairs.into_par_iter()`.
- `test_collect_names`: a collector `Names` (`State = Vec<u32>`, `Output =
  Box<[u32]>`, as in Task 2's doc example), `.merge_labels(Names)` with
  `VecOf(FixedWidth::<u32>::new())`; expected labels are the sorted copies'
  labels per arc; compare twice with `graph::eq_labeled`.
- `test_no_merge_keeps_repeats`: no policy, `FixedWidth::<u32>`;
  `LabeledVecGraph` deduplicates, so collect every `((src, dst), label)`
  of the sorted graph into a `Vec`, sort it, and `assert_eq!` it with the
  sorted input pairs (the order among equal pairs is unspecified).
- `test_dedup_keeps_one`: `.dedup()`, `FixedWidth::<u32>`; equals the base
  graph labeled `10 * s + d`.

Doc test on `merge_labels`: the spec's "The API" example, compressing to a
temporary directory.

- [ ] **Step 2: Verify they fail**

Run: `cargo test -p webgraph --test test_merge_labels`
Expected: compile errors (`merge_labels` missing; `Vec<u32>` not `Copy`).

- [ ] **Step 3: Implement**

`merge_labels`; relax `LabelSerDeser`; add `SD::SerType: Copy` where an
`L = SD::SerType` is sorted (the labeled transposes); relax `L: Clone +
Copy` to `L: Clone` in `par_sorted_graph.rs`. Docs: `merge_labels` (what
the merger sees, `finish` once per arc, the `AddLabel<L>` bound checked at
the terminal methods), `dedup` (one arbitrary label; `merge_labels` to
combine), `ParSortedLabeledGraph` (repeats kept by default; compressors
reject them), and the `push` docs of Task 1 now link `merge_labels`.

- [ ] **Step 4: Run**

Run: `cargo test -p webgraph --test test_merge_labels`, then
`cargo fmt --all && cargo clippy --workspace --all-targets && cargo test --workspace`
Expected: all pass, no warnings.

- [ ] **Step 5: Commit**

`git commit -m "ParSortedLabeledGraphConf::merge_labels merges the labels of repeated arcs while sorting"`

### Task 7: Documentation, changelog, benchmark

**Files:**
- Modify: `webgraph/CHANGELOG.md` (unreleased 0.8.0 section),
  `webgraph/src/graphs/par_sorted_graph.rs` (module docs)

- [ ] **Step 1: Docs**

`par_sorted_graph` module docs: a "Repeated arcs" paragraph (kept,
`.dedup()`, `.merge_labels`) with the spec's API example. CHANGELOG: drop
the compression-side entries; add the new traits, `KeepOne`,
`CountRepeats`, `VecOf`, the policies replacing `DEDUP` (with the
migration: `<true>` → `Dedup`, labeled sorters take the policy from the
codec), `merge_labels`, the relaxed `Copy` bounds, `LabelSerDeser`, and the
compressor errors. Run `cargo doc -p webgraph --no-deps` with
`RUSTDOCFLAGS="-D warnings"`: no broken links.

- [ ] **Step 2: Benchmark**

Build: `cargo build --release -p webgraph-cli`; verify
`"rustflags":["-C","target-cpu=native"` in
`target/release/.fingerprint/webgraph-cli-*/bin-webgraph.json`. Run
`$SCRATCH/bench/bench.sh target/release/webgraph new 5`.
Expected: both medians within 2% of the baseline (4.29 s, 20.71 s). If
either is above, rerun baseline and new interleaved (3 rounds of `base`,
`new`) before concluding; a confirmed regression is investigated
(`cargo asm`/profiling of `KMergeIters::next` and `encode_sorted_batch`)
before the task completes.

- [ ] **Step 3: Commit**

`git commit -m "Documentation and changelog for MergeLabels while sorting"`
