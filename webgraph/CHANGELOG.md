# Change Log

## [0.7.0] - unreleased

### New

- `par_map_fold_ord` family of methods that work like `par_map_fold`, but
  guarantee to process results in the same order as the input.

- `SplitLabeling` has a new `split_iter_at` method that makes it possible to
  select arbitrary cut points. CLI support makes it possible to use the degree
  cumulative function to split the compression work in a more balanced way.

- New declarative API for parallel compression and graph sorting in general.
  The trait `IntoParLenders` provides parallel lenders on consecutive chunks of
  nodes. The types `ParSortedGraph`/`ParSortedLabeledGraph` can be built from a graph or
  an iterator on pairs and implement `IntoParLenders`. Transparent wrappers such
  as `ParGraph` and `ParDcfGraph` can alter the default `IntoParLenders` splitting.
  Parallel compression methods take an `IntoParLenders` implementation.

- New labeled compression API based on the `StoreLabelsConf` trait. A number of
  basic (de)serializers (fixed width, ɣ, etc.) are provided.

- New free functions to build the Elias–Fano representation of offsets.

- `ParSortIters`/`ParSortPairs` have configurable `ProgressLogger` support. As a
  consequence, all transformation methods (`simplify*`, `symmetrize*`,
  `transpose`, etc.) have a `&mut impl ProgressLog` argument.

### Fixed

- `Zip` now zips labels strictly (panicking on mismatched per-node label
  counts instead of silently truncating), checks node streams in release
  builds, and forwards `num_arcs_hint`, as does `UnitLabelGraph`.

- `NoSelfLoopsGraph::num_arcs_hint` no longer reports the arc count of the
  underlying graph, which overstates the filtered graph.

- The bulk construction methods of `VecGraph`/`LabeledVecGraph` deduplicate
  arcs (keeping the label of the last occurrence, like `BTreeGraph`) instead
  of panicking with a misleading message.

- `check_offsets` verifies the final length entry of the offsets file, and
  the random-access load paths reject offsets files with a number of entries
  different from the number of nodes plus one (e.g., stale `.ef` files).

- Compression workers now report failures and empty lenders through the
  ordered merge, so `par_comp` returns contextual errors instead of
  panicking, and legal interior empty segments compress correctly.

- `ParGraph::with_cutpoints` and `ParSortedLabeledGraph::from_parts`
  validate their documented invariants at construction.

- `Matrix` indexing debug-asserts that the column index is in bounds, which
  would otherwise silently alias an element of the next row.

- Partition-boundary computations no longer overflow for node counts near
  `usize::MAX`.

- The all-nodes BFS iterator handles empty graphs, and the sequential BFS
  consults the filter before emitting `Revisit` events, matching the
  parallel implementations.

- `to_properties` omits ratio metrics with degenerate denominators
  (zero-node, zero-arc, or complete graphs) instead of emitting NaN/inf.

- The sorters clamp their batch size to at least one element per buffer, so
  a `MemoryUsage` smaller than one element no longer allocates
  zero-capacity buffers with an arbitrary effective batch size.

- The `Decoder` aliases of `ConstCodesDecoderFactory` were not forwarding the
  const code parameters, so `Static` dispatch with non-default codes silently
  decoded with the default codes.

- The sequential `split_iter_at` helper ignored a nonzero first cutpoint,
  returning the wrong node ranges.

- `ParSortPairs`/`ParSortIters` now validate destination node ids, so
  `map_*`, `permute_*`, and pair sorting cannot silently produce graphs whose
  successors exceed the number of nodes.

- `ParSortedGraph::iter_from` returns an empty lender past the last node
  instead of panicking.

- Parallel compression now works on missing chunks instead of silently
  writing a truncated graph, and stores temporary files in a fresh
  subdirectory of the configured temporary directory instead of deleting the
  caller-supplied directory wholesale.

- `Granularity` conversions are clamped to at least one node/arc, avoiding
  divisions by zero with zero granularities.

- `UnionGraph::split_iter_at` panicked when the two graphs had different
  numbers of nodes.

- `ParGraph::with_dcf` panicked on zero-arc graphs.

- `par_map_fold` and its variants deadlocked in a single-thread Rayon pool.

- `CompFlags::from_properties` panicked on malformed `.properties` content.

- `BvCompZ::push` panicked on label-store errors instead of propagating them.

- `FixedWidth` now rejects types wider than 64 bits, whose negative values
  could not be deserialized correctly.

- The successor iterators of `Left`/`Right` projections now forward
  `size_hint`.

- `NonZeroUsize` has been replaced everywhere by `usize`, as there were no niche
  optimizations involved.

- Replaced a number of wrong `num_cpus::get` calls with
  `rayon::current_num_threads`.

- `MmapHelper::new` did not correctly set the file length.

- `CsrGraph` construction from an empty lender was returning a graph
  with one node instead of zero nodes.

- `symmetrize_sorted_seq` now sorts just the reverse arcs, as documented;
  it requires `SplitLabeling` and returns lazily merged partitions like
  `symmetrize_sorted_par`.

- `ParSortPairs::num_partitions` now panics if the number of partitions is
  zero, like `ParSortIters::num_partitions` (previously, a zero value caused
  a division by zero when sorting).

- The default destination code of `GapsIter` is now δ, matching the default
  of `GapsCodec`.

- The `add_exact_lender` methods of `VecGraph`/`LabeledVecGraph` now add
  successor nodes, consistently with `add_sorted_lender`.

- `LabeledBTreeGraph::add_lender` now returns `&mut Self`, like all other
  `add_lender` methods.

- The `compratio` property is now computed in floating point, avoiding
  overflows for graphs with more than 2³² nodes.

- `check_offsets` now returns `Ok(false)` if the stored offsets do not
  match, instead of panicking.

### Changed

- All breadth-first visits now emit the same initial event sequence: `Init`,
  then `FrontierSize` at distance zero with the number of accepted roots, and
  then the `Visit` events for the roots. The `FrontierSize` event is now
  always emitted as soon as the frontier of nodes at a given distance has
  been entirely computed.

- Upgraded to `sux` 0.14.0. This is a major release in which the structure
  of types has changed significantly, so the `EF` and `DCF` types are no
  longer compatible (in particular, their serializations).

- Upgraded to `dsi-bitstream` 0.9.0.

- Reallocation strategies in `BvComp` and `BvCompZ` do not shrink below capacity 1024.

- Removed spurious `reset` inherent method in sequential visits.

- `par_comp_lenders_endianness` lost its useless `num_nodes` argument.

- Removed dead `current_node` field in `split::seq::Iter`.

- Removed `num_of_residuals` method from `Encode` and `Decode`.

- `code_to_str` now actually returns `None` instead of panicking.

- Now also `ParSortIters` uses Rayon threads.

- Removed dependency from `common_traits`, replaced by `num-traits`.

- There are no more parallel instances of `SortPairs`. All parallel
  code uses `ParSortPairs` or `ParSortIters`.

- All transformation methods acting on splittable graphs take an
  optional list of cutpoints to split the work in a more balanced way.

- `simplify*` methods are now `symmetrize*` methods, with an option
  to remove self-loops (i.e., the old `simplify*` behavior).

- The methods `par_comp_endianness*` have been removed, and replaced
  by a `par_comp_lenders!` macro in the `webgraph-cli` crate. Consequently,
  the features `le_bins` and `be_bins` have been removed.

- `PermutedGraph` has now a proper constructor that checks at least
  for the length of the permutation, and has `into_parts`, too.

- `SortPairs` is gone, replaced by `*_seq` equivalent methods in `ParSortPairs`
  which can additionally partition the output. Thus it is possible, say, to
  transpose a graph that cannot be split sequentially but then compress it in
  parallel. Consequently, the module `sort_pairs` has been renamed
  `k_merge_iters`.

- `UnitLender` has been renamed `UnitLabelLender`, and `UnitSucc` has
  been renamed `UnitLabelSucc` for consistency.

- The trait required for offsets is no longer `IndexedSeq`, but rather
  `SliceByValue`, which is simpler and more appropriate.

- `IntoPairs`/`IntoLabeledPairs` no longer implement `Clone`, as the
  implementation was unsound.

- `MemoryUsage::default` has now a more sensible, nonlinear growth strategy.

- The setters in `BvCompConfig` dropped the `with_` prefix.

- Configuration structures now all end in `Conf` for consistency.

- All parallel structures have a `Par` prefix for consistency.

- All transformation methods have been uniformly renamed with `_par`/`_seq`
  suffixes to distinguish between the parallel and sequential versions.
  `_split` is no longer used.

### Improved

- `ParSortPairs`, `ParSortIters`, and `KMergeIters` now have a
  `const DEDUP: bool` type parameter that enables deduplication at compile time.

- Added `FusedIterator` implementations to `BfsOrder` and `DfsOrder`.

- In the symmetric case, the ExactSumSweep implementation computes the
  connected components using a parallel visit.

## [0.6.1] - 2026-02-23

### Fixed

- The reallocation strategy of `BvCompZ` was plainly wrong, whereas that of
  `BvComp` was deallocating, reallocating and copying instead of using
  `Vec::shrink_to`.

- `BvGraphSeq` was sub-preallocating the successor vector (+1% speed).

### Improved

- Temporary compression files are now immediately removed when they
  are no longer necessary.

## [0.6.0] - 2026-02-18

### New

- Zuckerli-inspired reference-resolution code that improves compression ratios
  significantly, at the price of a longer compression time.

- New compression API based on `BvComp::with_basename` (traditional compressor)
  and `BvCompZ::with_basename` (Zuckerli-like reference selection) followed by
  setters, and flexible single-thread or parallel methods.

- Support for π codes, both in the file format and in the CLI tools.

- `SequentialLabeling::build_dcf` new default method for building a degree
  cumulative function. It is overridden by more efficient implementations, for
  example, in `BvGraphSeq`. All iterators have fast constant-time
  implementations of `count` whenever possible to support the method.

### Changed

- All parallel methods now use the current Rayon global thread pool rather than
  accepting a `&ThreadPool` argument.

- Upgraded to `rand` 0.10.0.

- Upgraded to `Lender` 0.6.0.

- `IteratorImpl` has been renamed `LenderImpl` for consistency.

- All implementations of `NodeLabelsLender` have been renamed `NodeLabels` (was:
  `Iter`).

- Tentatively, all implementations of iterators on successors end in `Succ`, and
  on labels end in `Labels`. When a random-access and a sequential
  implementation is available, the latter starts with `Seq`. If the type
  returned is actually an `IntoIterator`, it is named `IntoSucc` or
  `IntoLabels`.

### Fixed

- `BfsOrder` and `BfsOrderFromRoots` were reporting wrong distances,
  and reporting roots multiple times.

- `JavaPermutation::set_unchecked` was not properly handling endianness.

### Improved

- The `'static` bound on the `MapWhile`, `Scan` and `FilterMap` implementations
  of `NodeLabelsLender` has been removed thanks to support from the `Lender`
  crate.

- Major coverage increase in tests, leading to fixes for several minor bugs.

## [0.5.0] - 2025-11-28

### Changed

- Switched to the 2024 edition.

## [0.4.0] - 2025-11-15

### New

- Updated to last versions of dependencies.

- `labels::eq_sorted` function that checks equality between sorted labelings.

- `labels::check_impl` associated function that checks that the sequential and
  random-access implementations of a random-access labeling return the same
  results.

- `graph::eq` function that checks equality between graphs with sorted lenders.

- `graph::eq_labeled` function that checks equality between labeled graphs with
  sorted lenders.

- `SortedIter` has been renamed `AssumeSortedIterator` for consistency with
  `AssumeSortedLender`.

- `ArcListGraph::new` now returns directly a left projection.

- Visits have been moved here.

- `VecGraph` has new, more efficient constructors from sorted lenders returning
  `ExactSizeIterator`.

### Changed

- Several methods previously accepting a `&ThreadPool` no
  longer do. The user can use the standard Rayon global thread pool
  or configure their own and use `ThreadPool::install`.

- `JavaPermutation` just implements `SliceByValue` and `SliceByValueMut`,
  rather than `BitFieldSlice` and `BitFieldSliceMut`.

### Fixed

- The successors of `LabeledVecGraph` now implement `SortedIterator`.

## [0.3.0] - 2025-05-23

### Changed

- There is a workspace containing three crates: `webgraph` (basic
  infrastructure), `algo` (algorithms), and `cli` (command line
  interface).

- Layered Label Propagation has been moved to the `algo` crate.

## [0.2.1] - 2025-03-28

### New

- The `pad` command now takes a file instead of a basename, making it possible
  to pad offset files.

- The CLI has been rewritten using `clap`'s declarative interface.

## [0.2.0] - 2025-03-27

### New

- Four new mutable structures: `LabeledVecGraph`, `VecGraph`, `LabeledBTreeGraph`
  and `BTreeGraph`. The latter two structures implement the functionality of the
  old `VecGraph` structure. Migration from the old `VecGraph` requires usually
  just dropping the `Left` projector. The main source of incompatibility is that
  in the new `VecGraph` arcs can be added only in increasing successor order.
  Moreover, `LabeledVecGraph` and `VecGraph` are now two different types. All
  structures can be serialized with ε-serde.

- We now rely on the `dsi-bitstream` mechanism for dynamic code dispatch.

- All dependencies have been updated.

- LLP can be run in split mode.

- New `Granularity` enum to specify granularity of parallel computations.

- `ParMapFold` for generic parallel iteration without some of the
  bottlenecks of Rayon's `ParallelBridge`.

- Layered Label Propagation has been split into a label-generation phase
  and a label-combination phase that can be run separately.

- Log4J-like logging format that includes the thread id.

### Improved

- Arguments specifying a thread pool are now simply references.

## [0.1.4] - 2024-08-09

### Fixed

- Wrong class name (BvGraph) in properties.

## [0.1.3] - 2024-08-08

### Fixed

- Triple fields are now public.

## [0.1.2] - 2024-07-31

### Fixed

- Fixed README links.

## [0.1.1] - 2024-07-31

### New

- First release.
