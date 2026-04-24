# Label Compression in BvGraph Compressors

**Date**: 2026-04-24
**Scope**: Sequential compression only (parallel deferred)

## Problem

The BvGraph compressors (`BvComp`, `BvCompZ`) currently accept iterators over
successor IDs (`usize`). When a graph carries arc labels, the iterator yields
`(successor, label)` pairs. We need a way to route labels to a label-specific
compression pipeline that runs alongside the graph compressor.

## Design

### Principle: labels-only path

All compression methods operate on **labeled** iterators (`(usize, L)` pairs).
Unlabeled compression is the special case where `L = ()` and the label
compressor is `()`. The existing `UnitLabelGraph` / `UnitLender` / `UnitSucc`
types already provide the `(usize, ())` adaptation.

### `LabelComp` trait

```rust
pub trait LabelComp {
    type Label;
    fn init(&mut self) -> Result<()>;
    fn push_node(&mut self) -> Result<()>;
    fn push_label(&mut self, label: &Self::Label) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
}
```

`impl LabelComp for ()` with `Label = ()` — all methods are no-ops (compiled
away by monomorphization).

**Semantics of `push_node`**: signals the start of a new node's labels. On
every call except the first, it writes the accumulated bit count for the
*previous* node to the offsets stream, then resets the accumulator.

**Semantics of `push_label`**: serializes one label to the bitstream and adds
the number of bits written to the per-node accumulator.

**Semantics of `flush`**: writes the final node's accumulated bit count to the
offsets stream, then flushes both the label bitstream and the offsets stream.

**Semantics of `init`**: performs any setup (e.g., writing the initial zero
offset). Called before any `push_node`.

### `BitStreamLabelComp`

```rust
pub struct BitStreamLabelComp<E: Endianness, S> {
    serializer: S,
    bitstream: BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>,
    offsets_writer: OffsetsWriter<File>,
    bits_for_current_node: u64,
    started: bool,
}
```

The **label file** contains only serialized label values — no node IDs, no
degrees. The number of labels per node equals the graph's outdegree, which is
already encoded in the graph bitstream.

The **label offsets file** stores delta-encoded (γ-coded) bit positions, one per
node, using the same `OffsetsWriter` infrastructure as graph offsets. Combined
with the initial zero written by `init()`, this gives `n + 1` cumulative
offsets for `n` nodes — exactly the format that `BitStreamLabeling` expects via
its `IndexedSeq` offsets.

`S` is a `BitSerializer<E, BufBitWriter<E, ...>>` whose `SerType` matches
`LabelComp::Label`.

**Method behavior**:

- `init()`: the `OffsetsWriter` writes the initial γ-coded 0.
- `push_node()`: if `started`, writes `bits_for_current_node` via
  `offsets_writer.push()`; sets `started = true`; resets accumulator to 0.
- `push_label(label)`: calls `serializer.serialize(label, &mut bitstream)`;
  adds the returned bit count to `bits_for_current_node`.
- `flush()`: if `started`, writes the final `bits_for_current_node` to offsets;
  flushes bitstream and offsets writer.

### Changes to `BvComp` and `BvCompZ`

Both structs gain a generic parameter `LC: LabelComp = ()` and a field
`label_comp: LC`.

**`push` signature change** — from:

```rust
pub fn push<I: IntoIterator<Item = usize>>(&mut self, succ_iter: I) -> Result<()>
```

to:

```rust
pub fn push<I: IntoIterator<Item = (usize, LC::Label)>>(&mut self, succ_iter: I) -> Result<()>
```

**Inside `push`**:

1. Call `self.label_comp.push_node()`.
2. Iterate items: push `.0` (successor) into backrefs; call
   `self.label_comp.push_label(&item.1)` for each label.
3. Compress successors as before (unchanged logic).

**`flush`**: additionally calls `self.label_comp.flush()`.

**`extend`**: the `Label` bound changes from `usize` to `(usize, LC::Label)`.

### `BvCompConfig` methods

Four public methods; all delegate to `comp_labeled_lender`:

| Method | Graph input | Label comp | Wrapping |
|---|---|---|---|
| `comp_graph` | `SequentialGraph` | `()` | `UnitLabelGraph` |
| `comp_lender` | Lender with `Label = usize` | `()` | `UnitLender` |
| `comp_labeled_graph` | `LabeledSequentialGraph<L>` | caller-provided `LC` | none |
| `comp_labeled_lender` | Lender with `Label = (usize, L)` | caller-provided `LC` | none |

**Delegation chain**:

- `comp_graph(graph)` → `comp_labeled_graph(UnitLabelGraph(graph), ())`
- `comp_labeled_graph(graph, lc)` → `comp_labeled_lender(graph.iter(), lc, Some(n))`
- `comp_lender(iter, n)` → `comp_labeled_lender(UnitLender(iter), (), n)`
- `comp_labeled_lender(iter, lc, n)` — core implementation

The `LabelComp` value is passed to the compression method, not constructed by
`BvCompConfig`. This keeps `BvCompConfig` focused on graph compression
parameters and lets the caller own the full label-compression lifecycle
(serializer choice, file paths, bit widths, etc.).

Inside `comp_labeled_lender`, the label comp's `init()` is called before the
compression loop, and `flush()` is called via `BvComp::flush()` /
`BvCompZ::flush()` at the end.

### File layout

For a graph with basename `foo` and label name `bar`:

| File | Contents |
|---|---|
| `foo.graph` | compressed successor bitstream |
| `foo.offsets` | γ-coded delta offsets into graph bitstream |
| `foo.properties` | graph metadata |
| `foo-bar.labels` | serialized label bitstream (labels only, no nodes/degrees) |
| `foo-bar.labeloffsets` | γ-coded delta offsets into label bitstream |

The label file naming follows the Java WebGraph convention of
`basename-LABELNAME.labels` / `.labeloffsets`.

### What is NOT in scope

- **Parallel labeled compression**: deferred. When we tackle it, the
  `LabelComp` is already inside `BvComp`/`BvCompZ`, so each parallel chunk
  naturally gets its own label compressor.
- **Changes to `BitStreamLabeling`** (the reading side): the produced files are
  already compatible with the existing `BitStreamLabeling` reader.
- **Properties file for labels**: deferred (may be needed later to record
  serializer metadata).
