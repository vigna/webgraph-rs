# Change Log

## [0.5.0] - unreleased

### New

- New `transform perm` command that acts like `bvgraph to --perm`, but can
  be found easily.

- Extensive support for optionally loading a degree cumulative function
  to split work on a graph by arcs rather than by nodes.

- Everything reading or writing integers or floats has ASCII as default
  format, and JSON, Java and ε-serde storage of boxed slices or `BitFieldVec`
  instances as alternatives. Note that some commands previously defaulting to
  the Java format now default to ASCII.

- All commands descriptions now have a zero-width space after the final period,
  to prevent Clap from stripping it.

- New `seq` subcommand providing bidirectional conversion between all
  available formats for sequences.

- Parallel DCF construction (by default) using `par_map_fold_ord`.

### Changed

- `webgraph transform` now defaults to the parallel version.

- Removed dependency from `common_traits`, replaced by `num-traits`.

- `to bvgraph` default endianness is now the source endianness (it
  used to be big endian).

- Complete overhaul of the loading/storing framework for slices of values (like
  permutations); the framework is now based on `usize` rather than `u64`, and it
  is available also on 32-bit platforms (except for Java 64-bit permutations).

- Load methods return boxed slices when they do not return specific types (e.g.,
  `JavaPermutation`).

- Except for storing floats, all JSON I/O is handled by `serde_json`.

- `*Vector*` types are now `*Slice*` types.

- `GlobalArgs` has been removed, and the log interval is now passed as an
  argument to the commands that need it.

- Help is wrapped to at most 100 columns.

- Subcommand descriptions start from the same line of the subcommand, but option
  descriptions are forced on the next line.

### Fixed

- Sequential transposition was using big-endian format regardless of the
  specified endianness.

- The setting of the log interval does not appear anymore in all commands, but
  just in the ones that do logging.

- `to ascii` was never calling `ProgressLog::update`.

- `to endianness` was using an additional `length` property that is not
  part of the properties emitted by Java code, making impossible to change endianness
  of graphs compressed in Java.

## [0.4.1] - 2026-02-23

### New

- New `webgraph-rank` CLI supporting centrality measures.

- Support for loading vectors parallel to previous support for writing vectors.

### Improved

- Floats without a specified precision are now printed using the `zmij` crate,
  which brings a 3-4x speed improvement.

## [0.4.0] - 2026-02-18

### New

- Support for π codes in the CLI tools.

- `webgraph-dist` now supports running the ExactSumSweep algorithm.

### Changed

- Moved to `rand` 0.10.0.

### Fixed

- Check CLI commands `ef` and `eq` lacked endianness support.

## [0.3.0] - 2025-11-28

### Changed

- Switched to the 2024 edition.

## [0.2.0] - 2025-11-15

### Changed

- Code has been updated to the new WebGraph version.

## [0.1.0] - 2025-05-23

### Changed

- Layered Label Propagation is now accessed through the `webgraph-algo` CLI.
