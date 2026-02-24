/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use dsi_bitstream::prelude::*;
use webgraph::prelude::*;

#[test]
fn test_memory_flags_to_mmap_flags() {
    use webgraph::graphs::bvgraph::MemoryFlags;

    // Test default (empty)
    let default_flags = MemoryFlags::default();
    assert!(default_flags.is_empty());
    let mmap_flags: mmap_rs::MmapFlags = default_flags.into();
    assert!(mmap_flags.is_empty());

    // Test sequential flag
    let seq_flags = MemoryFlags::SEQUENTIAL;
    let mmap_flags: mmap_rs::MmapFlags = seq_flags.into();
    assert!(mmap_flags.contains(mmap_rs::MmapFlags::SEQUENTIAL));

    // Test random access flag
    let ra_flags = MemoryFlags::RANDOM_ACCESS;
    let mmap_flags: mmap_rs::MmapFlags = ra_flags.into();
    assert!(mmap_flags.contains(mmap_rs::MmapFlags::RANDOM_ACCESS));

    // Test transparent huge pages flag
    let thp_flags = MemoryFlags::TRANSPARENT_HUGE_PAGES;
    let mmap_flags: mmap_rs::MmapFlags = thp_flags.into();
    assert!(mmap_flags.contains(mmap_rs::MmapFlags::TRANSPARENT_HUGE_PAGES));

    // Test combined flags
    let combined = MemoryFlags::SEQUENTIAL | MemoryFlags::RANDOM_ACCESS;
    let mmap_flags: mmap_rs::MmapFlags = combined.into();
    assert!(mmap_flags.contains(mmap_rs::MmapFlags::SEQUENTIAL));
    assert!(mmap_flags.contains(mmap_rs::MmapFlags::RANDOM_ACCESS));
}

#[test]
fn test_memory_flags_to_epserde_flags() {
    use webgraph::graphs::bvgraph::MemoryFlags;

    let seq = MemoryFlags::SEQUENTIAL;
    let deser_flags: epserde::deser::Flags = seq.into();
    assert!(deser_flags.contains(epserde::deser::Flags::SEQUENTIAL));

    let ra = MemoryFlags::RANDOM_ACCESS;
    let deser_flags: epserde::deser::Flags = ra.into();
    assert!(deser_flags.contains(epserde::deser::Flags::RANDOM_ACCESS));

    let thp = MemoryFlags::TRANSPARENT_HUGE_PAGES;
    let deser_flags: epserde::deser::Flags = thp.into();
    assert!(deser_flags.contains(epserde::deser::Flags::TRANSPARENT_HUGE_PAGES));
}

#[test]
fn test_file_factory_creation() -> Result<()> {
    use dsi_bitstream::prelude::CodesReaderFactory;
    use webgraph::graphs::bvgraph::FileFactory;

    // Compress a graph to create a .graph file
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2)]);
    let tmp = tempfile::tempdir()?;
    let basename = tmp.path().join("graph");
    BvComp::with_basename(&basename).comp_graph::<BE>(&graph)?;

    // Create a FileFactory for the .graph file
    let graph_path = basename.with_extension("graph");
    let factory = FileFactory::<BE>::new(&graph_path)?;

    // Create a reader from the factory
    let _reader = factory.new_reader();
    Ok(())
}

#[test]
fn test_file_factory_nonexistent_fails() {
    use webgraph::graphs::bvgraph::FileFactory;
    assert!(FileFactory::<BE>::new("/nonexistent/path/graph.graph").is_err());
}

#[test]
fn test_memory_factory_from_data() {
    use dsi_bitstream::prelude::CodesReaderFactory;
    use webgraph::graphs::bvgraph::MemoryFactory;

    let data: Box<[u32]> = vec![0u32; 10].into_boxed_slice();
    let factory = MemoryFactory::<BE, _>::from_data(data);
    let _reader = factory.new_reader();
}

#[test]
fn test_memory_factory_new_mem() -> Result<()> {
    use dsi_bitstream::prelude::CodesReaderFactory;
    use webgraph::graphs::bvgraph::MemoryFactory;

    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1)]);
    let tmp = tempfile::tempdir()?;
    let basename = tmp.path().join("graph");
    BvComp::with_basename(&basename).comp_graph::<BE>(&graph)?;

    let graph_path = basename.with_extension("graph");
    let factory = MemoryFactory::<BE, _>::new_mem(&graph_path)?;
    let _reader = factory.new_reader();
    Ok(())
}

#[test]
fn test_memory_factory_new_mmap() -> Result<()> {
    use dsi_bitstream::prelude::CodesReaderFactory;
    use webgraph::graphs::bvgraph::{MemoryFactory, MemoryFlags};

    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1)]);
    let tmp = tempfile::tempdir()?;
    let basename = tmp.path().join("graph");
    BvComp::with_basename(&basename).comp_graph::<BE>(&graph)?;

    let graph_path = basename.with_extension("graph");
    let factory = MemoryFactory::<BE, _>::new_mmap(&graph_path, MemoryFlags::empty())?;
    let _reader = factory.new_reader();
    Ok(())
}

#[test]
fn test_mmap_helper_basic() -> Result<()> {
    use mmap_rs::MmapFlags;
    use webgraph::utils::MmapHelper;

    let tmp = tempfile::NamedTempFile::new()?;
    let path = tmp.path();
    // Write some u32 data as native-endian bytes
    let data: Vec<u32> = vec![1, 2, 3, 4, 5];
    let bytes: Vec<u8> = data.iter().flat_map(|x| x.to_ne_bytes()).collect();
    std::fs::write(path, &bytes)?;

    let helper = MmapHelper::<u32>::mmap(path, MmapFlags::empty())?;
    assert_eq!(helper.as_ref().len(), 5);
    assert_eq!(helper.as_ref()[0], 1);
    assert_eq!(helper.as_ref()[4], 5);
    Ok(())
}

#[test]
fn test_mmap_helper_mut() -> Result<()> {
    use mmap_rs::{MmapFlags, MmapMut};
    use webgraph::utils::MmapHelper;

    let tmp = tempfile::NamedTempFile::new()?;
    let path = tmp.path();
    // Write initial data to create a file of the right size
    let data: Vec<u32> = vec![0, 0, 0, 0];
    let bytes: Vec<u8> = data.iter().flat_map(|x| x.to_ne_bytes()).collect();
    std::fs::write(path, &bytes)?;
    // Open as mutable mmap
    let mut helper = MmapHelper::<u32, MmapMut>::mmap_mut(path, MmapFlags::empty())?;
    helper.as_mut()[0] = 42;
    helper.as_mut()[3] = 99;
    assert_eq!(helper.as_ref()[0], 42);
    assert_eq!(helper.as_ref()[3], 99);
    assert_eq!(helper.as_ref().len(), 4);
    Ok(())
}
