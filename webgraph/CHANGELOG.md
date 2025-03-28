# Change Log

## [0.2.1] - 2025-03-28

### New

* The `pad` command now takes a file instead of a basename, making it possible
  to pad offset files.

* The CLI has been rewritten using `clap`'s declarative interface.

## [0.2.0] - 2025-03-27

### New

* Four new mutable structures: `LabeledVecGraph`, `VecGraph`, `LabeledBTreeGraph`
  and `BTreeGraph`. The latter two structures implements the functionality of the
  old `VecGraph` structure. Migration from the old `VecGraph` requires usually
  just dropping the `Left` projector. The main source of incompatibility is that
  in the new `VecGraph` arcs can be added only in increasing successor order.
  Moreover, `LabeledVecGraph` and `VecGraph` are now two different types. All
  structures can be serialized with ε-serde.

* We now rely on the `dsi-bitstream` mechanism for dynamic code dispatch.

* All dependencies have been updated.

* LLP can be run in split mode.

* New `Granularity` enum to specify granularity of parallel computations.

* `ParMapFold` for generic parallel iteration without some of the
  bottlenecks of `rayon`s `ParallelBridge`.

* Layered Label Propagation has been split into a label-generation phase
  and a label-combination phase that can be run separately.

* Log4J-like logging format that includes the thread id.

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
