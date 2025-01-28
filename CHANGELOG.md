# Change Log

## [0.3.0] - 2025-01-28

### New

Four new mutable structures: `LabeledVecGraph`, `VecGraph`, `LabeledBTreeGraph`
and `BTreeGraph`. The latter two structures implements the functionality of the
old `VecGraph` structure. Migration from the old `VecGraph` requires usually
just dropping the `Left` projector. The main source of incompatibility is that
in the new `VecGraph` arcs can be added only in increasing successor order.
Moreover, `LabeledVecGraph` and `VecGraph` are two different types.

### Improved

* Argument specifying a thread pool are now simply references.

## [0.2.0] - 2024-08-09

### Improved

* Argument specifying a thread pool are now simply references.

## [0.1.4] - 2024-08-09

### Fixed

* Wrong class name (BvGraph) in properties.

## [0.1.3] - 2024-08-08

### Fixed

* Triple fields are now public.

## [0.1.2] - 2024-07-31

### Fixed

* Fixed README links.

## [0.1.1] - 2024-07-31

### New

* First release.
