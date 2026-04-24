# Label Compression Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Integrate label compression into BvGraph's sequential compressors so that labeled graphs can be compressed in a single pass.

**Architecture:** A `LabelComp` trait with `init`/`push_node`/`push_label`/`flush` methods is added to the `traits` module. `BvComp` and `BvCompZ` gain an `LC: LabelComp` generic parameter; their `push` methods accept `(usize, LC::Label)` pairs instead of plain `usize`. `BvCompConfig` gets four methods: `comp_graph` and `comp_lender` (unlabeled, delegate via `UnitLabelGraph`/`UnitLender`), and `comp_labeled_graph` and `comp_labeled_lender` (labeled, accept a caller-provided `LabelComp`).

**Tech Stack:** Rust, dsi-bitstream, lender, webgraph traits (`BitSerializer`, `OffsetsWriter`, `UnitLabelGraph`)

---

## File Map

| File | Action | Purpose |
|------|--------|---------|
| `webgraph/src/traits/label_comp.rs` | Create | `LabelComp` trait + `impl LabelComp for ()` |
| `webgraph/src/traits/mod.rs` | Modify | Add `pub mod label_comp; pub use label_comp::*;` |
| `webgraph/src/labels/bitstream_comp.rs` | Create | `BitStreamLabelComp` struct |
| `webgraph/src/labels/mod.rs` | Modify | Add `pub mod bitstream_comp; pub use bitstream_comp::*;` |
| `webgraph/src/graphs/bvgraph/comp/bvcomp.rs` | Modify | Add `LC` generic param, change `push`/`extend`/`flush`/`new` |
| `webgraph/src/graphs/bvgraph/comp/bvcompz.rs` | Modify | Same changes as `bvcomp.rs` |
| `webgraph/src/graphs/bvgraph/comp/impls.rs` | Modify | Add `comp_labeled_graph`/`comp_labeled_lender`, refactor `comp_graph`/`comp_lender`/`par_comp` |
| `webgraph/tests/test_bvgraph_roundtrip.rs` | Modify | Add labeled compression round-trip test |

---

### Task 1: `LabelComp` trait and `()` impl

**Files:**
- Create: `webgraph/src/traits/label_comp.rs`
- Modify: `webgraph/src/traits/mod.rs`

- [ ] **Step 1: Create the trait file**

```rust
// webgraph/src/traits/label_comp.rs

/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Trait for compressing arc labels alongside graph compression.

use anyhow::Result;

/// Compresses arc labels written alongside a graph compressor.
///
/// Implementations receive labels one arc at a time via [`push_label`],
/// grouped by node via [`push_node`]. The [`init`] method performs any
/// setup (e.g., writing an initial offset), and [`flush`] finalizes the
/// output.
///
/// The unit type `()` implements this trait with `Label = ()`, making
/// every method a no-op that is compiled away by monomorphization.
///
/// [`push_label`]: Self::push_label
/// [`push_node`]: Self::push_node
/// [`init`]: Self::init
/// [`flush`]: Self::flush
pub trait LabelComp {
    /// The arc-label type that this compressor accepts.
    type Label;

    /// Performs any setup before compression begins.
    fn init(&mut self) -> Result<()>;

    /// Signals the start of a new node's labels.
    ///
    /// On every call except the first, implementations typically
    /// record the accumulated bit count for the previous node.
    fn push_node(&mut self) -> Result<()>;

    /// Compresses a single arc label.
    fn push_label(&mut self, label: &Self::Label) -> Result<()>;

    /// Finalizes compression and flushes all output.
    fn flush(&mut self) -> Result<()>;
}

impl LabelComp for () {
    type Label = ();

    #[inline(always)]
    fn init(&mut self) -> Result<()> {
        Ok(())
    }

    #[inline(always)]
    fn push_node(&mut self) -> Result<()> {
        Ok(())
    }

    #[inline(always)]
    fn push_label(&mut self, _label: &()) -> Result<()> {
        Ok(())
    }

    #[inline(always)]
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}
```

- [ ] **Step 2: Wire it into the traits module**

In `webgraph/src/traits/mod.rs`, add after the last `pub use` line (currently line 66):

```rust
pub mod label_comp;
pub use label_comp::*;
```

- [ ] **Step 3: Verify it compiles**

Run: `cargo build -p webgraph 2>&1 | tail -5`
Expected: successful build (or only pre-existing warnings)

- [ ] **Step 4: Commit**

```bash
git add webgraph/src/traits/label_comp.rs webgraph/src/traits/mod.rs
git commit -m "Add LabelComp trait and no-op impl for ()"
```

---

### Task 2: `BitStreamLabelComp`

**Files:**
- Create: `webgraph/src/labels/bitstream_comp.rs`
- Modify: `webgraph/src/labels/mod.rs`

- [ ] **Step 1: Create the struct file**

```rust
// webgraph/src/labels/bitstream_comp.rs

/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! A [`LabelComp`] that serializes labels to a bitstream using a
//! [`BitSerializer`], recording per-node offsets for random access.

use crate::prelude::*;
use anyhow::{Context, Result};
use dsi_bitstream::prelude::*;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;

/// Compresses arc labels into a bitstream file with a companion
/// delta-encoded offsets file.
///
/// The label file contains only serialized label values — no node IDs,
/// no degrees. The number of labels per node equals the graph's
/// outdegree, which is already encoded in the graph bitstream.
///
/// The offsets file stores γ-coded deltas of bit positions, one per
/// node, using the same [`OffsetsWriter`] as graph offsets. Together
/// with the initial zero written by [`init`], this gives _n_ + 1
/// cumulative offsets for _n_ nodes — exactly the format that
/// [`BitStreamLabeling`] expects.
///
/// [`init`]: LabelComp::init
/// [`BitStreamLabeling`]: crate::labels::BitStreamLabeling
pub struct BitStreamLabelComp<E: Endianness, S> {
    serializer: S,
    bitstream: BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>,
    offsets_writer: OffsetsWriter<File>,
    bits_for_current_node: u64,
    started: bool,
}

impl<E: Endianness, S> BitStreamLabelComp<E, S> {
    /// Creates a new label compressor writing to the given paths.
    ///
    /// The `labels_path` receives the serialized label bitstream,
    /// and `offsets_path` receives the γ-coded delta offsets.
    pub fn new(
        serializer: S,
        labels_path: impl AsRef<Path>,
        offsets_path: impl AsRef<Path>,
    ) -> Result<Self> {
        let labels_path = labels_path.as_ref();
        let offsets_path = offsets_path.as_ref();
        let bitstream = buf_bit_writer::from_path::<E, usize>(labels_path)
            .with_context(|| format!("Could not create {}", labels_path.display()))?;
        let offsets_writer = OffsetsWriter::from_path(offsets_path, false)?;
        Ok(Self {
            serializer,
            bitstream,
            offsets_writer,
            bits_for_current_node: 0,
            started: false,
        })
    }
}

impl<E: Endianness, S> LabelComp for BitStreamLabelComp<E, S>
where
    S: BitSerializer<E, BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>>,
{
    type Label = S::SerType;

    fn init(&mut self) -> Result<()> {
        self.offsets_writer.push(0)?;
        Ok(())
    }

    fn push_node(&mut self) -> Result<()> {
        if self.started {
            self.offsets_writer.push(self.bits_for_current_node)?;
        }
        self.started = true;
        self.bits_for_current_node = 0;
        Ok(())
    }

    fn push_label(&mut self, label: &Self::Label) -> Result<()> {
        let bits = self
            .serializer
            .serialize(label, &mut self.bitstream)
            .map_err(|e| anyhow::anyhow!("{}", e))?;
        self.bits_for_current_node += bits as u64;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.started {
            self.offsets_writer.push(self.bits_for_current_node)?;
        }
        BitWrite::flush(&mut self.bitstream)?;
        self.offsets_writer.flush()?;
        Ok(())
    }
}
```

- [ ] **Step 2: Wire it into the labels module**

In `webgraph/src/labels/mod.rs`, add after the last `pub use` line:

```rust
pub mod bitstream_comp;
pub use bitstream_comp::*;
```

- [ ] **Step 3: Verify it compiles**

Run: `cargo build -p webgraph 2>&1 | tail -5`
Expected: successful build

- [ ] **Step 4: Commit**

```bash
git add webgraph/src/labels/bitstream_comp.rs webgraph/src/labels/mod.rs
git commit -m "Add BitStreamLabelComp for label serialization"
```

---

### Task 3: Modify `BvComp` to accept `LabelComp`

**Files:**
- Modify: `webgraph/src/graphs/bvgraph/comp/bvcomp.rs`

- [ ] **Step 1: Add `LC` generic parameter to the struct**

Change the struct definition (line 80) from:

```rust
pub struct BvComp<E, W: Write> {
```

to:

```rust
pub struct BvComp<E, W: Write, LC: LabelComp = ()> {
```

Add a field after `stats` (line 111):

```rust
    label_comp: LC,
```

- [ ] **Step 2: Update `with_basename` impl block**

Change the impl block (line 113) from:

```rust
impl BvComp<(), std::io::Sink> {
```

to:

```rust
impl BvComp<(), std::io::Sink, ()> {
```

- [ ] **Step 3: Update the main impl block and `new`**

Change the impl block (line 384) from:

```rust
impl<E: EncodeAndEstimate, W: Write> BvComp<E, W> {
```

to:

```rust
impl<E: EncodeAndEstimate, W: Write, LC: LabelComp> BvComp<E, W, LC> {
```

Add `label_comp: LC` parameter to `new` (after `start_node: usize,`):

```rust
    pub fn new(
        encoder: E,
        offsets_writer: OffsetsWriter<W>,
        compression_window: usize,
        max_ref_count: usize,
        min_interval_length: usize,
        start_node: usize,
        label_comp: LC,
    ) -> Self {
```

And add the field in the struct literal (after `stats: CompStats::default(),`):

```rust
            label_comp,
```

- [ ] **Step 4: Change `push` to accept `(usize, LC::Label)` pairs**

Change the signature (line 418) from:

```rust
    pub fn push<I: IntoIterator<Item = usize>>(&mut self, succ_iter: I) -> anyhow::Result<()> {
```

to:

```rust
    pub fn push<I: IntoIterator<Item = (usize, LC::Label)>>(&mut self, succ_iter: I) -> anyhow::Result<()> {
```

Replace the collection block (lines 421–428):

```rust
        {
            let succ_vec = &mut self.backrefs[self.curr_node];
            succ_vec.clear();
            succ_vec.extend(succ_iter);
            if succ_vec.len().max(1024) < succ_vec.capacity() / 4 {
                succ_vec.shrink_to(succ_vec.capacity() / 2);
            }
        }
```

with:

```rust
        self.label_comp.push_node()?;
        {
            let succ_vec = &mut self.backrefs[self.curr_node];
            succ_vec.clear();
            for (succ, label) in succ_iter {
                succ_vec.push(succ);
                self.label_comp.push_label(&label)?;
            }
            if succ_vec.len().max(1024) < succ_vec.capacity() / 4 {
                succ_vec.shrink_to(succ_vec.capacity() / 2);
            }
        }
```

- [ ] **Step 5: Update `flush`**

Change `flush` (line 534) from:

```rust
    pub fn flush(mut self) -> anyhow::Result<CompStats> {
        self.encoder.flush()?;
        self.offsets_writer.flush()?;
        Ok(self.stats)
    }
```

to:

```rust
    pub fn flush(mut self) -> anyhow::Result<CompStats> {
        self.encoder.flush()?;
        self.offsets_writer.flush()?;
        self.label_comp.flush()?;
        Ok(self.stats)
    }
```

- [ ] **Step 6: Update `extend`**

Change `extend` (lines 546–555) from:

```rust
    pub fn extend<L>(&mut self, iter_nodes: L) -> anyhow::Result<()>
    where
        L: IntoLender,
        L::Lender: for<'next> NodeLabelsLender<'next, Label = usize>,
    {
        for_! ( (_, succ) in iter_nodes {
            self.push(succ.into_iter())?;
        });
        Ok(())
    }
```

to:

```rust
    pub fn extend<I>(&mut self, iter_nodes: I) -> anyhow::Result<()>
    where
        I: IntoLender,
        I::Lender: for<'next> NodeLabelsLender<'next, Label = (usize, LC::Label)>,
    {
        for_! ( (_, succ) in iter_nodes {
            self.push(succ)?;
        });
        Ok(())
    }
```

- [ ] **Step 7: Verify it compiles**

Run: `cargo build -p webgraph 2>&1 | tail -20`
Expected: compilation errors in `impls.rs` (callers of `BvComp::new` and `push` not yet updated) — that is expected and will be fixed in Task 5.

- [ ] **Step 8: Commit**

```bash
git add webgraph/src/graphs/bvgraph/comp/bvcomp.rs
git commit -m "Add LC: LabelComp parameter to BvComp"
```

---

### Task 4: Modify `BvCompZ` to accept `LabelComp`

**Files:**
- Modify: `webgraph/src/graphs/bvgraph/comp/bvcompz.rs`

- [ ] **Step 1: Add `LC` generic parameter to the struct**

Change (line 59):

```rust
pub struct BvCompZ<E, W: Write> {
```

to:

```rust
pub struct BvCompZ<E, W: Write, LC: LabelComp = ()> {
```

Add field after `stats` (line 93):

```rust
    label_comp: LC,
```

- [ ] **Step 2: Update `with_basename` impl block**

Change (line 95):

```rust
impl BvCompZ<(), std::io::Sink> {
```

to:

```rust
impl BvCompZ<(), std::io::Sink, ()> {
```

- [ ] **Step 3: Update main impl block and `new`**

Change (line 108):

```rust
impl<E: EncodeAndEstimate, W: Write> BvCompZ<E, W> {
```

to:

```rust
impl<E: EncodeAndEstimate, W: Write, LC: LabelComp> BvCompZ<E, W, LC> {
```

Add `label_comp: LC` parameter to `new` (after `start_node: usize,`):

```rust
    pub fn new(
        encoder: E,
        offsets_writer: OffsetsWriter<W>,
        compression_window: usize,
        chunk_size: usize,
        max_ref_count: usize,
        min_interval_length: usize,
        start_node: usize,
        label_comp: LC,
    ) -> Self {
```

And add in the struct literal (after `stats: CompStats::default(),`):

```rust
            label_comp,
```

- [ ] **Step 4: Change `push` to accept `(usize, LC::Label)` pairs**

Change signature (line 149):

```rust
    pub fn push<I: IntoIterator<Item = usize>>(&mut self, succ_iter: I) -> anyhow::Result<()> {
```

to:

```rust
    pub fn push<I: IntoIterator<Item = (usize, LC::Label)>>(&mut self, succ_iter: I) -> anyhow::Result<()> {
```

Replace the collection line (line 152):

```rust
        self.backrefs.push(succ_iter);
```

with:

```rust
        self.label_comp.push_node()?;
        {
            let mut tmp = Vec::new();
            for (succ, label) in succ_iter {
                tmp.push(succ);
                self.label_comp.push_label(&label)?;
            }
            self.backrefs.push(tmp);
        }
```

Note: `RaggedArray::push` takes `IntoIterator<Item = T>` so we collect successors into a `Vec<usize>` first, then push it. This preserves the existing `RaggedArray` contract.

- [ ] **Step 5: Update `flush`**

Change `flush` (line 260) from:

```rust
    pub fn flush(mut self) -> anyhow::Result<CompStats> {
        if self.compression_window > 0 {
            self.comp_refs()?;
        }
        // Flush bits are just padding
        self.encoder.flush()?;
        self.offsets_writer.flush()?;
        Ok(self.stats)
    }
```

to:

```rust
    pub fn flush(mut self) -> anyhow::Result<CompStats> {
        if self.compression_window > 0 {
            self.comp_refs()?;
        }
        // Flush bits are just padding
        self.encoder.flush()?;
        self.offsets_writer.flush()?;
        self.label_comp.flush()?;
        Ok(self.stats)
    }
```

- [ ] **Step 6: Update `extend`**

Change `extend` (lines 276–285) from:

```rust
    pub fn extend<L>(&mut self, iter_nodes: L) -> anyhow::Result<()>
    where
        L: IntoLender,
        L::Lender: for<'next> NodeLabelsLender<'next, Label = usize>,
    {
        for_! ( (_, succ) in iter_nodes {
            self.push(succ.into_iter())?;
        });
        Ok(())
    }
```

to:

```rust
    pub fn extend<I>(&mut self, iter_nodes: I) -> anyhow::Result<()>
    where
        I: IntoLender,
        I::Lender: for<'next> NodeLabelsLender<'next, Label = (usize, LC::Label)>,
    {
        for_! ( (_, succ) in iter_nodes {
            self.push(succ)?;
        });
        Ok(())
    }
```

- [ ] **Step 7: Verify it compiles (expect errors in impls.rs)**

Run: `cargo build -p webgraph 2>&1 | tail -20`
Expected: errors in `impls.rs` — to be fixed in Task 5.

- [ ] **Step 8: Commit**

```bash
git add webgraph/src/graphs/bvgraph/comp/bvcompz.rs
git commit -m "Add LC: LabelComp parameter to BvCompZ"
```

---

### Task 5: Refactor `BvCompConfig` methods

**Files:**
- Modify: `webgraph/src/graphs/bvgraph/comp/impls.rs`

This is the largest task. We add `comp_labeled_lender` (the core) and `comp_labeled_graph`, then rewrite `comp_graph` and `comp_lender` to delegate via `UnitLabelGraph`/`UnitLender`.

- [ ] **Step 1: Add `comp_labeled_lender` — the core method**

Add this method to `impl BvCompConfig` (after `comp_lender`, before `par_comp`):

```rust
    /// Compresses sequentially a labeled [`NodeLabelsLender`] and returns
    /// the number of bits written to the graph bitstream.
    ///
    /// The `label_comp` parameter receives arc labels alongside graph
    /// compression. Use `()` for unlabeled graphs.
    pub fn comp_labeled_lender<E, L, LC>(
        &mut self,
        iter: L,
        mut label_comp: LC,
        expected_num_nodes: Option<usize>,
    ) -> Result<u64>
    where
        E: Endianness,
        L: IntoLender,
        LC: LabelComp,
        L::Lender: for<'next> NodeLabelsLender<'next, Label = (usize, LC::Label)>,
        BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>: CodesWrite<E>,
    {
        let graph_path = self.basename.with_extension(GRAPH_EXTENSION);

        // Compress the graph
        let bit_write = buf_bit_writer::from_path::<E, usize>(&graph_path)
            .with_context(|| format!("Could not create {}", graph_path.display()))?;

        let codes_writer = DynCodesEncoder::new(bit_write, &self.comp_flags)?;

        // create a file for offsets
        let offsets_path = self.basename.with_extension(OFFSETS_EXTENSION);
        let offset_writer = OffsetsWriter::from_path(offsets_path, true)?;

        label_comp.init()?;

        let mut pl = progress_logger![
            display_memory = true,
            item_name = "node",
            expected_updates = expected_num_nodes,
        ];
        pl.start("Compressing successors...");
        let comp_stats = if self.bvgraphz {
            let mut bvcompz = BvCompZ::new(
                codes_writer,
                offset_writer,
                self.comp_flags.compression_window,
                self.chunk_size,
                self.comp_flags.max_ref_count,
                self.comp_flags.min_interval_length,
                0,
                label_comp,
            );

            for_! ( (_node_id, successors) in iter {
                bvcompz.push(successors).context("Could not push successors")?;
                log_comp_stats(&bvcompz.stats(), false);
                pl.update();
            });
            log_comp_stats(&bvcompz.stats(), true);
            pl.done();

            bvcompz.flush()?
        } else {
            let mut bvcomp = BvComp::new(
                codes_writer,
                offset_writer,
                self.comp_flags.compression_window,
                self.comp_flags.max_ref_count,
                self.comp_flags.min_interval_length,
                0,
                label_comp,
            );

            for_! ( (_node_id, successors) in iter {
                bvcomp.push(successors).context("Could not push successors")?;
                log_comp_stats(&bvcomp.stats(), false);
                pl.update();
            });
            log_comp_stats(&bvcomp.stats(), true);
            pl.done();

            bvcomp.flush()?
        };

        if let Some(num_nodes) = expected_num_nodes {
            if num_nodes != comp_stats.num_nodes {
                log::warn!(
                    "The expected number of nodes is {num_nodes} but the actual number of nodes is {}",
                    comp_stats.num_nodes,
                );
            }
        }

        log::info!("Writing the .properties file");
        let properties = self
            .comp_flags
            .to_properties::<E>(&comp_stats)
            .context("Could not serialize properties")?;
        let properties_path = self.basename.with_extension(PROPERTIES_EXTENSION);
        std::fs::write(&properties_path, properties)
            .with_context(|| format!("Could not write {}", properties_path.display()))?;

        Ok(comp_stats.written_bits)
    }
```

- [ ] **Step 2: Add `comp_labeled_graph`**

Add this method (before `comp_labeled_lender`):

```rust
    /// Compresses sequentially a [`LabeledSequentialGraph`] and returns
    /// the number of bits written to the graph bitstream.
    ///
    /// The `label_comp` parameter receives arc labels alongside graph
    /// compression.
    pub fn comp_labeled_graph<E: Endianness, L, LC: LabelComp<Label = L>>(
        &mut self,
        graph: impl LabeledSequentialGraph<L>,
        label_comp: LC,
    ) -> Result<u64>
    where
        BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>: CodesWrite<E>,
    {
        let num_nodes = graph.num_nodes();
        self.comp_labeled_lender::<E, _, _>(graph.iter(), label_comp, Some(num_nodes))
    }
```

- [ ] **Step 3: Rewrite `comp_graph` to delegate**

Replace the existing `comp_graph` body (lines 321–326):

```rust
    pub fn comp_graph<E: Endianness>(&mut self, graph: impl SequentialGraph) -> Result<u64>
    where
        BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>: CodesWrite<E>,
    {
        self.comp_labeled_graph::<E, (), ()>(UnitLabelGraph(graph), ())
    }
```

- [ ] **Step 4: Rewrite `comp_lender` to delegate**

Replace the existing `comp_lender` body (lines 333–418):

```rust
    pub fn comp_lender<E, L>(&mut self, iter: L, expected_num_nodes: Option<usize>) -> Result<u64>
    where
        E: Endianness,
        L: IntoLender,
        L::Lender: for<'next> NodeLabelsLender<'next, Label = usize>,
        BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>: CodesWrite<E>,
    {
        self.comp_labeled_lender::<E, _, _>(UnitLender(iter.into_lender()), (), expected_num_nodes)
    }
```

- [ ] **Step 5: Update `par_comp` — pass `()` to `BvComp::new`/`BvCompZ::new`**

In `par_comp` (inside the `s.spawn` closure), update the two `BvCompZ::new` and `BvComp::new` calls to pass `()` as the last argument. Change (around lines 488–496):

```rust
                        let mut bvcomp = BvCompZ::new(
                            codes_encoder,
                            OffsetsWriter::from_path(&chunk_offsets_path, false).unwrap(),
                            cp_flags.compression_window,
                            chunk_size,
                            cp_flags.max_ref_count,
                            cp_flags.min_interval_length,
                            node_id,
                            (),
                        );
```

And (around lines 507–514):

```rust
                        let mut bvcomp = BvComp::new(
                            codes_encoder,
                            OffsetsWriter::from_path(&chunk_offsets_path, false).unwrap(),
                            cp_flags.compression_window,
                            cp_flags.max_ref_count,
                            cp_flags.min_interval_length,
                            node_id,
                            (),
                        );
```

Also update the two `bvcomp.push(successors)` calls inside the spawn closure. Since `par_comp` still works with `Label = usize`, the `UnitSucc` wrapper converts `usize` to `(usize, ())`. The existing code does:

```rust
bvcomp.push(successors).unwrap();
```

where `successors` is an `IntoIterator<Item = usize>`. Since `push` now expects `(usize, ())`, wrap it:

```rust
bvcomp.push(UnitSucc(successors.into_iter())).unwrap();
```

Do the same for the second occurrence of `bvcomp.push(successors)` and for the two `bvcomp.push(succ.into_iter())` inside the `for_!` loops — change to:

```rust
bvcomp.push(UnitSucc(succ.into_iter())).unwrap();
```

There are four `push` calls total in `par_comp` (two for bvgraphz branch, two for bvcomp branch) that all need this `UnitSucc` wrapping.

- [ ] **Step 6: Verify the full crate compiles**

Run: `cargo build -p webgraph 2>&1 | tail -20`
Expected: successful build

- [ ] **Step 7: Run existing tests**

Run: `cargo test -p webgraph 2>&1 | tail -20`
Expected: all existing tests pass

- [ ] **Step 8: Commit**

```bash
git add webgraph/src/graphs/bvgraph/comp/impls.rs
git commit -m "Add comp_labeled_graph/comp_labeled_lender, refactor existing methods"
```

---

### Task 6: Labeled compression round-trip test

**Files:**
- Modify: `webgraph/tests/test_bvgraph_roundtrip.rs`

- [ ] **Step 1: Add labeled round-trip test**

Add at the end of the test file:

```rust
#[test]
fn test_bvcomp_labeled_roundtrip() -> Result<()> {
    use webgraph::traits::{BitDeserializer, FixedWidth, LabelComp};

    let graph =
        webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (0, 2), (1, 3), (2, 3), (3, 0)]);
    // Per-arc labels matching the arc order above
    let labels_per_node: Vec<Vec<u32>> = vec![
        vec![10, 20], // node 0 → 1, 0 → 2
        vec![30],     // node 1 → 3
        vec![40],     // node 2 → 3
        vec![50],     // node 3 → 0
    ];

    let tmp = tempfile::TempDir::new()?;
    let basename = tmp.path().join("labeled");
    let labels_path = tmp.path().join("labeled.labels");
    let label_offsets_path = tmp.path().join("labeled.labeloffsets");

    // --- Compress graph + labels using the low-level BvComp API ---
    let serializer = FixedWidth::<u32>::new();
    let mut label_comp = webgraph::labels::BitStreamLabelComp::<BE, _>::new(
        serializer,
        &labels_path,
        &label_offsets_path,
    )?;
    label_comp.init()?;

    let bit_write = dsi_bitstream::utils::buf_bit_writer::from_path::<BE, usize>(
        basename.with_extension("graph"),
    )?;
    let codes_writer =
        webgraph::graphs::bvgraph::DynCodesEncoder::new(bit_write, &CompFlags::default())?;
    let offset_writer = OffsetsWriter::from_path(basename.with_extension("offsets"), true)?;

    let mut bvcomp = BvComp::new(codes_writer, offset_writer, 7, 3, 4, 0, label_comp);

    for (node, succs) in graph.iter() {
        let succs_vec: Vec<usize> = succs.collect();
        let pairs: Vec<(usize, u32)> = succs_vec
            .iter()
            .zip(labels_per_node[node].iter())
            .map(|(&s, &l)| (s, l))
            .collect();
        bvcomp.push(pairs)?;
    }
    bvcomp.flush()?;

    // --- Verify graph round-trips ---
    let seq = BvGraphSeq::with_basename(&basename)
        .endianness::<BE>()
        .load()?;
    labels::eq_sorted(&graph, &seq)?;

    // --- Verify labels by reading back the bitstream ---
    // Read label offsets (gamma-coded deltas)
    let of =
        webgraph::utils::MmapHelper::<u32>::mmap(&label_offsets_path, mmap_rs::MmapFlags::empty())?;
    let mut offset_reader: BufBitReader<BE, _> =
        BufBitReader::new(MemWordReader::new(of.as_ref()));

    let num_nodes = graph.num_nodes();
    let mut cumulative_offsets = Vec::with_capacity(num_nodes + 1);
    let mut acc = 0u64;
    for _ in 0..num_nodes + 1 {
        acc += offset_reader.read_gamma()?;
        cumulative_offsets.push(acc);
    }

    // Read label values using the cumulative offsets
    let lf =
        webgraph::utils::MmapHelper::<u32>::mmap(&labels_path, mmap_rs::MmapFlags::empty())?;
    let mut label_reader: BufBitReader<BE, _> =
        BufBitReader::new(MemWordReader::new(lf.as_ref()));
    let deser = FixedWidth::<u32>::new();

    for node in 0..num_nodes {
        label_reader.set_bit_pos(cumulative_offsets[node])?;
        let mut got = Vec::new();
        while label_reader.bit_pos()? < cumulative_offsets[node + 1] {
            got.push(deser.deserialize(&mut label_reader)?);
        }
        assert_eq!(got, labels_per_node[node], "labels mismatch at node {node}");
    }

    Ok(())
}
```

This test verifies both the graph round-trip and the label values by reading back the bitstream and comparing against the expected per-node labels.

- [ ] **Step 2: Run the new test**

Run: `cargo test -p webgraph test_bvcomp_labeled_roundtrip -- --nocapture 2>&1 | tail -20`
Expected: PASS

- [ ] **Step 3: Run full test suite**

Run: `cargo test -p webgraph 2>&1 | tail -10`
Expected: all tests pass

- [ ] **Step 4: Commit**

```bash
git add webgraph/tests/test_bvgraph_roundtrip.rs
git commit -m "Add labeled compression round-trip test"
```
