# MergeLabel Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the BV compressors merge the labels of repeated arcs through a user-supplied `MergeLabel`, and turn repeated or unsorted successors into explicit errors when no merger is configured.

**Architecture:** A public `MergeLabel<L>` trait (with `KeepOne` and `CountRepeats`) lives in `traits/labels.rs`. The compressors (`BvComp`, `BvCompZ`) gain a merge-policy type parameter (`NoMerge` by default, `WithMerge<M>` when a merger is set), dispatched through a sealed `MergePolicy` trait and a shared iterator adapter, `MergedSucc`, that groups runs of equal successors, pushes one label per run to the `StoreLabels`, yields each successor once, and records ordering errors. `BvCompConf` carries the policy as a type parameter set by `merge_labels`.

**Tech Stack:** Rust (edition of the workspace), `anyhow`, `lender`, `dsi-bitstream`, `rayon`.

**Spec:** `docs/superpowers/specs/2026-10-22-merge-label-design.md`

## Global Constraints

- Work on a new branch `merge-label` created from `main` (never commit to `main`); never use `git stash` (use a worktree if isolation is needed).
- Run `cargo fmt --all` after every batch of code changes.
- Breaking changes are acceptable; update every call site, doc example, and test they break.
- Without a merger, no new bounds on label types (in particular, no `Default`).
- Error messages, exactly: `Repeated successor {succ} in successor list of node {node}` and `Successor list of node {node} is not sorted: {succ} follows {prev}`.
- Documentation is in the third person ("Merges …", not "Merge …"); `MergeLabel` methods carry the doc comments given in the spec.
- Tests: no tests for `Debug`/`Display`; use `graph::eq`, `graph::eq_labeled`, and the other ready-made comparison functions; exact assertions only.
- The order of labels within a run is unspecified; no code or test may depend on it.

## Review Focus

- A conf with a merger configured must still compress unlabeled graphs (`comp_graph`, `par_comp`) normally: test `test_unlabeled_with_merger_configured` in Task 4.
- A run that ends the successor list (last successor repeated) must still flush its merged label: covered by `test_push_merges_runs` (Task 2/3) and by `MULTIPLICITIES` in Task 4 (nodes 1, 3, 5 end with a repeated successor).
- The merge buffer must be reset at each run and across nodes, including after a node with no successors: `test_push_merges_runs` pushes an empty node between two nodes with runs.
- Label `.properties` must count merged arcs, not input pairs: Task 4 asserts `num_arcs == 11`.
- In parallel compression a repeated successor must surface as an `Err` carrying the cause, not as a panic: `test_repeated_without_merger` in Task 4 checks the full error chain.

---

### Task 1: `MergeLabel` trait and ready-made mergers

**Files:**
- Modify: `webgraph/src/traits/labels.rs` (new section after the `SortedIterator`/`AssumeSortedIterator` section)

**Interfaces:**
- Produces:
  - `pub trait MergeLabel<L> { type Merged: Default; fn init(&self, merged: &mut Self::Merged) /* provided */; fn merge(&self, merged: &mut Self::Merged, label: L); }`
  - `#[derive(Debug, Clone, Copy, Default)] pub struct KeepOne;` with `impl<L: Default> MergeLabel<L> for KeepOne { type Merged = L; }`
  - `#[derive(Debug, Clone, Copy, Default)] pub struct CountRepeats;` with `impl<L> MergeLabel<L> for CountRepeats { type Merged = u64; }`
  - All three reachable as `webgraph::traits::{MergeLabel, KeepOne, CountRepeats}` and through `webgraph::prelude::*` (already re-exported via `pub use labels::*`).

- [ ] **Step 1: Write the doc tests (failing)**

The trait's doc comment explains the run semantics (compressor calls `init` once per run of equal successors, then `merge` for every label of the run including the first; order within a run unspecified; one merger shared by reference by all compression threads). The link to `BvCompConf::merge_labels` is added in Task 4, when the method exists. Its example defines a counting merger and checks it:

```rust
use webgraph::traits::MergeLabel;

struct Count;
impl<L> MergeLabel<L> for Count {
    type Merged = usize;
    fn merge(&self, merged: &mut usize, _label: L) {
        *merged += 1;
    }
}

let mut merged = 0;
for label in ["a", "b", "c"] {
    Count.merge(&mut merged, label);
}
assert_eq!(merged, 3);
// `init` cannot infer `L` by itself
MergeLabel::<&str>::init(&Count, &mut merged);
assert_eq!(merged, 0);
```

`KeepOne` example: `MergeLabel::<u32>::init(&KeepOne, &mut m)` then `merge(&mut m, 7)` → `assert_eq!(m, 7)`. Its doc says it keeps one label of the run, which one being unspecified, and requires `L: Default`.

`CountRepeats` example: three `merge` calls with arbitrary labels → `assert_eq!(m, 3)`; then `init` → `assert_eq!(m, 0)`. Its doc says it ignores labels and counts the occurrences of each arc.

- [ ] **Step 2: Run the doc tests to verify they fail**

Run: `cargo test -p webgraph --doc traits::labels`
Expected: FAIL, `cannot find trait MergeLabel` / `cannot find value KeepOne`.

- [ ] **Step 3: Implement the trait and the two mergers**

The trait with the exact doc comments from the spec ("Resets `merged` to the empty merge. …" with the reuse paragraph; "Merges `label` into `merged`."). `KeepOne::merge` assigns; `CountRepeats::merge` increments.

- [ ] **Step 4: Run the doc tests to verify they pass**

Run: `cargo test -p webgraph --doc traits::labels`
Expected: PASS.

- [ ] **Step 5: Format, lint, commit**

```bash
cargo fmt --all && cargo clippy -p webgraph --all-targets
git add webgraph/src/traits/labels.rs docs/superpowers
git commit -m "MergeLabel trait with KeepOne and CountRepeats"
```

---

### Task 2: Merge policies and `BvComp`

**Files:**
- Modify: `webgraph/src/graphs/bvgraph/comp/bvcomp.rs`
- Modify: `webgraph/src/graphs/bvgraph/comp/mod.rs` (test-only `VecStore`)
- Modify: `webgraph/src/graphs/bvgraph/comp/impls.rs:644,870` (pass `NoMerge` to `BvComp::new`)
- Modify: `webgraph/src/fuzz/bvcomp_and_read.rs:31` (pass `NoMerge`)

**Interfaces:**
- Consumes: `MergeLabel` (Task 1); `StoreLabels` (`traits/store.rs`).
- Produces (all in `bvcomp.rs`, public ones re-exported by `pub use bvcomp::*`):
  - `#[derive(Debug, Clone, Copy, Default)] pub struct NoMerge;`: no merging; repeated successors are an error.
  - `#[derive(Debug, Clone, Copy)] pub struct WithMerge<M>(pub M);`: merges the labels of repeated successors with `M`.
  - `pub trait MergePolicy<L, S>: sealed::Sealed` (private `mod sealed { pub trait Sealed {} }`), implemented for:
    - `NoMerge` with `S = L`;
    - `WithMerge<M>` with `M: MergeLabel<L, Merged = S>`;
    - `&P` for every `P: MergePolicy<L, S>` (so compression threads share one policy by reference).
    The trait's only method is `#[doc(hidden)]`:
    ```rust
    fn next_succ<I, SL>(
        &self,
        iter: &mut I,
        lookahead: &mut Option<Option<(usize, L)>>,
        merged: &mut Option<S>,
        store: &mut SL,
    ) -> anyhow::Result<Option<usize>>
    where
        I: Iterator<Item = (usize, L)>,
        SL: StoreLabels<Label = S>;
    ```
    It consumes the labels of the next run, pushes the stored label to `store`, and returns the run's successor (`Ok(None)` at the end).
  - `pub(crate) struct MergedSucc<'a, I, L, S, P, SL>`: `Iterator<Item = usize>` over the distinct successors of one node; `pub(crate) fn new(iter: I, policy: &'a P, merged: &'a mut Option<S>, store: &'a mut SL, node: usize) -> Self` and `pub(crate) fn into_result(self) -> anyhow::Result<()>` (the first error, if any).
  - `pub struct BvComp<E, W: Write, SL: StoreLabels = (), P = NoMerge>` with new fields `merge_policy: P` and `merged: Option<SL::Label>` (the reusable buffer, created lazily with `Default` only by `WithMerge`).
  - `pub fn new(encoder: E, offsets_writer: OffsetsWriter<W>, compression_window: usize, max_ref_count: usize, min_interval_length: usize, start_node: usize, store_labels: SL, merge_policy: P) -> Self`
  - `pub fn push<L, I: IntoIterator<Item = (usize, L)>>(&mut self, succ_iter: I) -> anyhow::Result<()> where P: MergePolicy<L, SL::Label>`
  - `pub fn extend<L, I>(&mut self, iter_nodes: I) -> anyhow::Result<()> where I: IntoLender, I::Lender: for<'next> NodeLabelsLender<'next, Label = (usize, L)>, P: MergePolicy<L, SL::Label>`
  - Test-only, in `comp/mod.rs`: `#[cfg(test)] pub(super) mod test_store` with `#[derive(Debug, Default)] pub struct VecStore<T>(pub Vec<T>)` implementing `StoreLabels<Label = T>` for `T: Clone` (`push_label` pushes a clone; everything else is a no-op returning `Ok`/`0`). Used by Task 3 too.

- [ ] **Step 1: Write the failing unit tests in the `test` module of `bvcomp.rs`**

Build the compressor in memory as `webgraph/src/fuzz/bvcomp_and_read.rs` does (`BufBitWriter` over `MemWordWriterVec`, `DynCodesEncoder` with `CompFlags::default()`, `OffsetsWriter::from_write`, `start_node = 0`), with a `VecStore`. These tests touch no files, so they must not be gated on `not(miri)`.

```rust
#[test]
fn test_push_merges_runs() -> anyhow::Result<()> {
    // WithMerge(CountRepeats), store VecStore<u64>
    bvcomp.push([(1, 'a'), (1, 'b'), (2, 'c'), (3, 'd'), (3, 'e'), (3, 'f')])?;
    bvcomp.push(core::iter::empty::<(usize, char)>())?;
    bvcomp.push([(0, 'x'), (0, 'y')])?;
    assert_eq!(bvcomp.stats().num_arcs, 4);
    // after flush, the store holds
    assert_eq!(store.0, vec![2, 1, 3, 2]);
    Ok(())
}

#[test]
fn test_push_repeated_without_merger() -> anyhow::Result<()> {
    // NoMerge, store VecStore<char>
    let err = bvcomp.push([(1, 'a'), (1, 'b')]).unwrap_err();
    assert_eq!(err.to_string(), "Repeated successor 1 in successor list of node 0");
    Ok(())
}

#[test]
fn test_push_unsorted() -> anyhow::Result<()> {
    // NoMerge
    let err = bvcomp.push([(2, 'a'), (1, 'b')]).unwrap_err();
    assert_eq!(err.to_string(), "Successor list of node 0 is not sorted: 1 follows 2");
    // WithMerge(CountRepeats), fresh compressor
    let err = bvcomp.push([(2, 'a'), (2, 'b'), (1, 'c')]).unwrap_err();
    assert_eq!(err.to_string(), "Successor list of node 0 is not sorted: 1 follows 2");
    Ok(())
}
```

(The store is owned by the compressor: get the labels back either by making `VecStore` hold an `Rc<RefCell<Vec<T>>>` or by giving `BvComp` a `#[cfg(test)]` accessor; implementer's choice.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p webgraph --lib bvgraph::comp::bvcomp::test::test_push`
Expected: FAIL to compile (`NoMerge`, `WithMerge`, `VecStore` undefined; `BvComp::new` takes 7 arguments).

- [ ] **Step 3: Implement the policies, `MergedSucc`, and the new `BvComp`**

`NoMerge::next_succ` ignores `lookahead` and `merged`: takes one item, pushes its label, returns its successor. `WithMerge::next_succ` needs one item of lookahead, kept across calls in `lookahead` (`None`: nothing buffered; `Some(Some(x))`: buffered item; `Some(None)`: input exhausted, so the input is never polled after it returned `None`):

```text
take the first item from lookahead, else from iter; if none, return Ok(None)
m = merged.get_or_insert_with(Default::default); policy.init(m); policy.merge(m, label)
loop: next = iter.next()
      if next has the same successor: merge its label
      else: lookahead = Some(next); break
store.push_label(m)?; return Ok(Some(succ))
```

`MergedSucc::next` stops (returns `None`) once an error is recorded; otherwise it calls `next_succ` and checks the returned successor against the previous one: equal → `Repeated successor …` error, smaller → `… is not sorted …` error (exact messages in Global Constraints). With `WithMerge` "equal" cannot happen, as runs absorb repeats.

`BvComp::push` replaces its loop with `succ_vec.extend(&mut succs)` on a `MergedSucc` built from disjoint borrows of `self` (`backrefs`, `merge_policy`, `merged`, `store_labels`), then `succs.into_result()?`, keeping the existing shrink logic. Update the docs of `push` (successors must be sorted; repeated successors are merged by the policy or rejected) and of the struct (the `P` parameter). Pass `NoMerge` at the two `BvComp::new` call sites in `impls.rs` and in the fuzz harness.

- [ ] **Step 4: Run the tests to verify they pass, then the existing suite**

Run: `cargo test -p webgraph --lib bvgraph::comp`
Expected: PASS (new tests and the existing `egst_writer_*`).
Run: `cargo check -p webgraph --features fuzz`
Expected: no errors.

- [ ] **Step 5: Format, lint, commit**

```bash
cargo fmt --all && cargo clippy -p webgraph --all-targets
git add -u webgraph
git commit -m "Merge policies for BvComp; repeated and unsorted successors are errors"
```

---

### Task 3: `BvCompZ`

**Files:**
- Modify: `webgraph/src/graphs/bvgraph/comp/bvcompz.rs`
- Modify: `webgraph/src/graphs/bvgraph/comp/impls.rs:623,849` (pass `NoMerge` to `BvCompZ::new`)
- Modify: `webgraph/tests/test_bvcomp.rs:111` (pass `NoMerge`)

**Interfaces:**
- Consumes: `NoMerge`, `WithMerge`, `MergePolicy`, `MergedSucc`, `VecStore` (Task 2).
- Produces:
  - `pub struct BvCompZ<E, W: Write, SL: StoreLabels = (), P = NoMerge>` with fields `merge_policy: P` and `merged: Option<SL::Label>`.
  - `pub fn new(encoder: E, offsets_writer: OffsetsWriter<W>, compression_window: usize, chunk_size: usize, max_ref_count: usize, min_interval_length: usize, start_node: usize, store_labels: SL, merge_policy: P) -> Self`
  - `push` and `extend` with the same generics and bounds as `BvComp`'s in Task 2.

- [ ] **Step 1: Write the failing unit tests**

Add a `test` module to `bvcompz.rs` (or extend the existing one) with `test_push_merges_runs`, `test_push_repeated_without_merger`, and `test_push_unsorted`: same inputs and assertions as Task 2, on an in-memory `BvCompZ` (`chunk_size` 2 or more, so the empty node and the last node fall in the same chunk as others).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p webgraph --lib bvgraph::comp::bvcompz`
Expected: FAIL to compile (`BvCompZ::new` takes 8 arguments).

- [ ] **Step 3: Implement**

Replace the `from_fn` closure and `label_result` in `BvCompZ::push` with `self.backrefs.push(&mut succs)` on a `MergedSucc`, followed by `succs.into_result()?`. Update the docs of `push` and of the struct as in Task 2. Pass `NoMerge` at the `BvCompZ::new` call sites.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p webgraph --lib bvgraph::comp && cargo test -p webgraph --test test_bvcomp`
Expected: PASS.

- [ ] **Step 5: Format, lint, commit**

```bash
cargo fmt --all && cargo clippy -p webgraph --all-targets
git add -u webgraph
git commit -m "Merge policies for BvCompZ"
```

---

### Task 4: `BvCompConf::merge_labels`, end-to-end tests, docs, changelog

**Files:**
- Modify: `webgraph/src/graphs/bvgraph/comp/impls.rs`
- Modify: `webgraph/src/graphs/par_sorted_graph.rs` (docs only)
- Modify: `webgraph/src/traits/labels.rs` (intra-doc link from `MergeLabel` to `BvCompConf::merge_labels`)
- Modify: every caller of `comp_labeled_lender` / `par_comp_labeled` with a turbofish (find with `grep -rn 'comp_labeled_lender::<\|par_comp_labeled::<' webgraph cli algo`), including doc examples in `impls.rs`, `labels/`, `traits/store.rs`, and `webgraph/tests/`
- Modify: `webgraph/CHANGELOG.md`
- Test: `webgraph/tests/test_bvgraph_roundtrip.rs` (new module `merge_labels`, next to `labeled`)

**Interfaces:**
- Consumes: Tasks 1–3 (`MergeLabel`, `KeepOne`, `CountRepeats`, `NoMerge`, `WithMerge`, `MergePolicy` including the `&P` implementation).
- Produces:
  - `pub struct BvCompConf<PL = Option<ProgressLogger>, P = NoMerge>` with field `merge_policy: P`; `BvCompConf::new` sets `NoMerge`; every builder `impl` becomes generic over `P`; `progress_logger<PL2>(self, pl: PL2) -> BvCompConf<PL2, P>`.
  - `pub fn merge_labels<M>(self, merger: M) -> BvCompConf<PL, WithMerge<M>>`
  - private `fn no_merge(&mut self) -> BvCompConf<&mut PL, NoMerge>` (clones the plain fields, reborrows `pl`; `&mut PL: ProgressLog` is provided by `dsi-progress-logger`).
  - In `impl<PL: ProgressLog, P> BvCompConf<PL, P>`:
    - `comp_graph<E>`, `comp_lender<E, L>`, `par_comp<E, G>`: unchanged signatures, implemented as `self.no_merge().<labeled method>(…)` with unit labels, so they ignore the configured merger.
    - `comp_labeled_graph<E, L, SLC: SeqStoreLabelsConf>(&mut self, graph: impl LabeledSequentialGraph<L>, store_labels_config: SLC) -> Result<u64> where P: MergePolicy<L, <SLC::StoreLabels as StoreLabels>::Label>` (replaces `SLC::StoreLabels: StoreLabels<Label = L>`).
    - `comp_labeled_lender<E, L, SLC, T>(&mut self, iter: L, store_labels_config: SLC, expected_num_nodes: Option<usize>) -> Result<u64> where L::Lender: for<'next> NodeLabelsLender<'next, Label = (usize, T)>, P: MergePolicy<T, <SLC::StoreLabels as StoreLabels>::Label>`.
    - `par_comp_labeled<E, G, SLC, T>(&mut self, graph: G, store_labels_config: SLC) -> Result<u64> where G: for<'a> IntoParLenders<ParLender: NodeLabelsLender<'a, Label = (usize, T)>>, P: MergePolicy<T, <SLC::StoreLabels as StoreLabels>::Label> + Sync` (other bounds unchanged).
    - The compressors are built with `&self.merge_policy`.

- [ ] **Step 1: Write the failing end-to-end tests**

In `webgraph/tests/test_bvgraph_roundtrip.rs`, add `#[cfg(not(miri))] mod merge_labels` with:

```rust
/// Multiplicities of the arcs of `common::TEST_GRAPH_ARCS`, in order: the
/// first, a middle, and the last successor of a node are repeated, and node 3
/// has a single successor repeated three times.
const MULTIPLICITIES: [u64; 11] = [2, 1, 1, 3, 2, 1, 3, 1, 1, 2, 1];
```

Helpers: `fn conf(basename: &Path, z: bool) -> BvCompConf` (`BvCompZ::with_basename` if `z`, else `BvComp::with_basename`); `fn pairs(label: impl Fn(usize, u64) -> u32) -> Vec<((usize, usize), u32)>` repeating the `i`-th arc of `TEST_GRAPH_ARCS` `MULTIPLICITIES[i]` times with label `label(i, k)` for the `k`-th copy. Input graph: `ParSortedLabeledGraph::from_pairs(8, FixedWidth::<u32>::new(), pairs(…))?` (default `DEDUP = false`, the path of the original report). Sequential mode: `comp_labeled_lender::<BE, _, _, _>(&graph, store_conf, Some(8))`; parallel mode: `par_comp_labeled::<BE, _, _, _>(&graph, store_conf)`. Every test loops over `z in [false, true]` and both modes, attaching `with_context(|| format!("z: {z}, mode: {mode:?}"))`.

- `test_count_repeats`: labels `100 * i + k` (all distinct); `merge_labels(CountRepeats)`; store conf `BitStreamStoreLabelsConf::<BE, _>::new(FixedWidth::<u64>::new())`. Expected: `LabeledVecGraph::from_arcs` of `TEST_GRAPH_ARCS` zipped with `MULTIPLICITIES`. Check `graph::eq_labeled(&expected, &Zip(seq, BitStreamLabelingSeq::<BE, _, _>::load(&labels_basename, FixedWidth::<u64>::new())?))?`, and with `parse_label_properties::<BE>(&labels_basename)?`: `num_nodes == 8`, `num_arcs == 11`.
- `test_keep_one`: label of every copy of arc `(src, dst)` is `(10 * src + dst) as u32`; `merge_labels(KeepOne)`; store `FixedWidth::<u32>`; expected: the graph of `labeled::test_labeled_roundtrip` (same labels, each arc once).
- `test_repeated_without_merger`: input `TEST_GRAPH_ARCS` once each plus one extra `((0, 1), 0)`; no `merge_labels`; store `FixedWidth::<u32>`; the call returns `Err(e)` and `format!("{e:#}")` contains `Repeated successor 1 in successor list of node 0`.
- `test_unsorted_successors`: sequential only, input `ArcListGraph::new_labeled(8, vec![((0, 2), 0u32), ((0, 1), 0)].into_iter())`, both with no merger and with `merge_labels(KeepOne)`; `format!("{e:#}")` contains `Successor list of node 0 is not sorted: 1 follows 2`.
- `test_unlabeled_with_merger_configured`: `conf(z).merge_labels(CountRepeats).comp_graph::<BE>(&common::test_graph())?` and, separately, `.par_comp::<BE, _>(&common::test_graph())?`; reload with `BvGraphSeq` and `graph::eq(&common::test_graph(), &seq)?`. Then `conf(z).comp_graph::<BE>(ArcListGraph::new(8, vec![(0, 1), (0, 1)].into_iter()))` returns an error whose chain contains `Repeated successor 1 in successor list of node 0`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p webgraph --test test_bvgraph_roundtrip merge_labels`
Expected: FAIL to compile (`merge_labels` undefined; four type arguments given to `par_comp_labeled`, which takes three).

- [ ] **Step 3: Implement the `BvCompConf` changes**

As in Interfaces. Then update every turbofish call site found by the grep above (`::<BE, _, _>` becomes `::<BE, _, _, _>` for `comp_labeled_lender` and `par_comp_labeled`; `comp_labeled_graph` keeps its arity).

- [ ] **Step 4: Run the new tests, then the whole crate**

Run: `cargo test -p webgraph --test test_bvgraph_roundtrip merge_labels`
Expected: PASS.
Run: `cargo test -p webgraph`
Expected: PASS (including doc tests).

- [ ] **Step 5: Documentation**

- `merge_labels`: what it does, that the store's serializer must handle `M::Merged`, that unlabeled methods ignore it, with a doc example compressing `ParSortedLabeledGraph::from_pairs` pairs containing repeats with `CountRepeats` through `par_comp_labeled` and checking the reloaded labels with `graph::eq_labeled`.
- `comp_labeled_graph`, `comp_labeled_lender`, `par_comp_labeled`: one paragraph: successors must be sorted; repeated successors are merged by the merger set with `merge_labels`, or rejected with an error.
- `par_sorted_graph.rs`: module docs, `ParSortedLabeledGraph`, and `ParSortedLabeledGraphConf::dedup`: with `DEDUP = false` repeated pairs are kept, and compressing them requires `BvCompConf::merge_labels`; with `DEDUP = true` one arbitrary label is kept.
- `MergeLabel` docs: the intra-doc link to `BvCompConf::merge_labels`.
- `webgraph/CHANGELOG.md`: new top section `## [0.8.0] - unreleased` with `### New` (`MergeLabel`, `KeepOne`, `CountRepeats`, `BvCompConf::merge_labels`, `NoMerge`/`WithMerge`/`MergePolicy`) and `### Changed` (`BvComp::new`/`BvCompZ::new` take a merge policy; `BvCompConf` has a merge-policy type parameter; `comp_labeled_lender` and `par_comp_labeled` have a new type parameter for the input label; repeated or unsorted successors now cause an error instead of a panic), in the style of the existing entries.

Run: `cargo doc -p webgraph --no-deps`
Expected: no broken intra-doc link warnings.

- [ ] **Step 6: Whole-workspace verification**

Run: `cargo fmt --all && cargo clippy --workspace --all-targets && cargo test --workspace`
Expected: no warnings, all tests PASS (the CLI must still compile with `BvCompConf<PL>`).

- [ ] **Step 7: Commit**

```bash
git add -u && git add webgraph/tests/test_bvgraph_roundtrip.rs
git commit -m "BvCompConf::merge_labels for merging the labels of repeated arcs"
```
