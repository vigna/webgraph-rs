# Change Log

## [0.4.0] - 2025-11-15

* Several methods previously accepting a `&ThreadPool` now
  they don't. The user can use the standard rayon global thread pool
  or configure their own and use `ThreadPool::install`.

## [0.3.0] -

### Changed

* Visits have been moved to the main WebGraph crate.

## [0.2.0] - 2025-05-23

### Changed

* Reviewed constructors of parallel visit to offer default granularity.

* Moved in Layered Label Propagation.

* Revamped ExactSumSweep implementation.

## [0.1.1] - 2025-04-01

### New

* ExactSumSweep algorithm for eccentricities, radius, and diameter.

* Strongly connected components.

### Fixed

* Fixed crate name.

## [0.1.0] - 2025-03-31

### New

* First release.
