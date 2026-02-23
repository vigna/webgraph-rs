# Change Log

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

- Several methods previously accepting a `&ThreadPool` now
  they don't. The user can use the standard Rayon global thread pool
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
