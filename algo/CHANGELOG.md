# Change Log

## [0.8.0] - unreleased

### Changed

- ExactSumSweep is now compiled once for each pair of graph types, rather
  than once for each level and value of `use_tot`. This reduces
  significantly code size, compile time, and compile memory of code using
  several levels: the `webgraph-cli` library, for example, is about a third
  of its previous size.

## [0.7.0] - 2026-10-12

### Changed

- The ExactSumSweep outputs `output_symm::RadiusDiameter`,
  `output_symm::Diameter`, and `output_symm::Radius`, returned by
  `run_symm`, are now re-exports of `output::RadiusDiameter`,
  `output::Diameter`, and `output::Radius`, respectively, returned by `run`,
  as each pair had exactly the same fields. `output_symm::All` is still a
  separate type, as it contains a single array of eccentricities.

- `BiRank::new` now panics also if the transpose has a different number of
  arcs than the graph.

- License is now MIT or Apache.

- Upgraded to `sux` 0.15.0, ε-serde 0.13.2, `lender` 0.8.0, and
  `value-traits` 0.3.1; all other dependencies have been upgraded to their
  latest version.

- The `PageRank` preference vector is now a `SliceByValue`, making it possible
  to have functionally or algorithmically generated preference vectors without
  the need to materialize them in memory.

- Upgraded to `card-est-array` 0.6.0.

- The LLP computation now uses a functional permutation from the `funcperm` crate
  instead of a permutation array. As a result, LLP computation now requires two
  `usize` per node instead of three. It is also possible to pass the identity
  permutation to exploit locality in the graph, if present.

### Improved

- The space occupancy of `ExactSumSweep` has been reduced by a factor of two for
  symmetric graphs, and in general by 1/3 if you give up on the "total"
  heuristics tie-breaker.

- `HyperBall` has now an external version (as in Java) using half the memory.
  Moreover, counter initialization has been parallelized.

### New

- New `BiRank` implementation.

- `USE_TOT` boolean type parameter decides whether to use the "total" heuristics
  tie-breaker in `ExactSumSweep`.

### Fixed

- Kosaraju's algorithm logged its second phase on a progress logger that
  had already been stopped by the topological sort.

- With node weights, `HyperBall` now seeds the radius-0 neighborhood
  function with the total weight (the exact number of elements loaded in
  the counters) instead of the number of nodes; moreover, empty graphs and
  malformed weights are handled with clean errors instead of panics deep in
  the counter machinery.

- `PageRank` validates the stochasticity of preference vectors in
  production builds (it used to check only under `cfg(test)`) and `BiRank`
  rejects negative or non-finite query vectors, as NaN norm deltas prevent
  threshold-based stopping criteria from ever being satisfied. The
  divergence from the Java LAW default mode is now documented.

- The first log-gap cost of LLP is computed without signed casts, which
  wrapped for node ids above `isize::MAX`.

- `ExactSumSweep` could return a non-minimal radius on symmetric graphs, as
  backward sweeps were not updating the radius upper bound; moreover, the
  iteration count of the all-CC bound step was over-reported in the symmetric
  case.

- `BiRank` returned an all-zero rank vector when all nodes were on the same
  side of the bipartition.

- All `HyperBall` builder constructors now validate that the transpose has
  the same number of nodes and arcs as the graph.

- Fixed problems with granularity interpretation in HyperBall.

- Fixed minor discrepancies with the ExactSumSweep paper.

- `layered_label_propagation` now returns an empty permutation on an empty
  graph instead of panicking with a division by zero.

- HyperBall log messages no longer overflow for graphs with more than 2³²
  nodes.

## [0.6.2] - 2026-05-01

### Fixed

- The dependency on `dsi-progress-logger` is no longer pinned to version
  0.8.2, so that dependents can use later versions. This release has the
  same sources as 0.6.1: it was published from the same commit, with just
  this change, so there is no corresponding tag.

## [0.6.1] - 2026-02-23

### New

- New `PageRank` parallel implementation based on the Gauss–Seidel iterative
  method.

## [0.6.0] - 2026-02-18

### Changed

- Moved to `rand` 0.10.0.

## [0.5.0] - 2025-11-28

### Changed

- Switched to the 2024 edition.

## [0.4.0] - 2025-11-15

### Changed

- Several methods previously accepting a `&ThreadPool` no
  longer do. The user can use the standard Rayon global thread pool
  or configure their own and use `ThreadPool::install`.

- Visits have been moved to the main WebGraph crate.

## [0.2.0] - 2025-05-23

### Changed

- Reviewed constructors of parallel visit to offer default granularity.

- Moved in Layered Label Propagation.

- Revamped ExactSumSweep implementation.

## [0.1.1] - 2025-04-01

### New

- ExactSumSweep algorithm for eccentricities, radius, and diameter.

- Strongly connected components.

### Fixed

- Fixed crate name.

## [0.1.0] - 2025-03-31

### New

- First release.
