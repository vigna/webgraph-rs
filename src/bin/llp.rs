use bitvec::prelude::*;
use clap::Parser;
use java_properties;
use log::info;
use mmap_rs::*;
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::Rng;
use rand::SeedableRng;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::io::Seek;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use sux::prelude::*;
use webgraph::prelude::*;
use webgraph::utils::ProgressLogger;

type ReadType = u32;
type BufferType = u64;

#[derive(Parser, Debug)]
#[command(about = "Performs an LLP round", long_about = None)]
struct Args {
    /// The basename of the graph.
    basename: String,
}

fn mmap_file(path: &str) -> Mmap {
    let mut file = std::fs::File::open(path).unwrap();
    let file_len = file.seek(std::io::SeekFrom::End(0)).unwrap();
    unsafe {
        MmapOptions::new(file_len as _)
            .unwrap()
            .with_file(file, 0)
            .map()
            .unwrap()
    }
}

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    let f = File::open(format!("{}.properties", args.basename))?;
    let map = java_properties::read(BufReader::new(f))?;

    let num_nodes = map.get("nodes").unwrap().parse::<u64>()?;

    // Read the offsets
    let data_offsets = mmap_file(&format!("{}.offsets", args.basename));
    let data_graph = mmap_file(&format!("{}.graph", args.basename));

    let offsets_slice = unsafe {
        core::slice::from_raw_parts(
            data_offsets.as_ptr() as *const ReadType,
            (data_offsets.len() + core::mem::size_of::<ReadType>() - 1)
                / core::mem::size_of::<ReadType>(),
        )
    };
    let graph_slice = unsafe {
        core::slice::from_raw_parts(
            data_graph.as_ptr() as *const ReadType,
            (data_graph.len() + core::mem::size_of::<ReadType>() - 1)
                / core::mem::size_of::<ReadType>(),
        )
    };

    let mut reader =
        BufferedBitStreamRead::<M2L, BufferType, _>::new(MemWordReadInfinite::new(&offsets_slice));

    let mut pr_offsets = ProgressLogger::default();
    pr_offsets.item_name = "offset".to_string();
    pr_offsets.start("Loading offsets...");
    // Read the offsets gammas
    let mut offsets = EliasFanoBuilder::new(
        (data_graph.len() * 8 * core::mem::size_of::<ReadType>()) as u64,
        num_nodes,
    );

    let mut offset = 0;
    for _ in 0..num_nodes {
        offset += reader.read_gamma::<true>().unwrap() as usize;
        offsets.push(offset as _).unwrap();
        pr_offsets.update();
    }

    pr_offsets.done_with_count(num_nodes as _);

    let offsets: EliasFano<SparseIndex<BitMap<Vec<u64>>, Vec<u64>, 8>, CompactArray<Vec<u64>>> =
        offsets.build().convert_to().unwrap();

    let code_reader = DefaultCodesReader::new(BufferedBitStreamRead::<M2L, BufferType, _>::new(
        MemWordReadInfinite::new(&graph_slice),
    ));
    let random_reader = WebgraphReaderRandomAccess::new(code_reader, offsets.clone(), 4);

    let mut glob_pr = ProgressLogger::default().display_memory();
    glob_pr.item_name = "update".to_string();

    let mut can_change = bitvec![usize, Lsb0; 0];
    can_change.resize(num_nodes as _, true);

    let gamma = 0.0;
    
    glob_pr.start("Starting updates...");

    let mut labels = Vec::with_capacity(num_nodes as _);
    let mut volumes = Vec::with_capacity(num_nodes as _);
    for l in 0..num_nodes as usize {
        labels.push(AtomicUsize::new(l));
        volumes.push(AtomicUsize::new(1));
    }

    let mut rand = SmallRng::seed_from_u64(0);
    let mut perm = (0..num_nodes).into_iter().collect::<Vec<_>>();

    for _ in 0..100 {
        perm.chunks_mut(100000).for_each(|chunk| chunk.shuffle(&mut rand));
        let mut pr = ProgressLogger::default();
        pr.item_name = "node".to_string();
        pr.local_speed = true;
        pr.expected_updates = Some(num_nodes as usize);
        pr.start("Updating...");

        let mut modified: usize = 0;
        let mut map = HashMap::<usize, usize>::new();
        let mut delta = 0.0;

        for &node in &perm {
            if !can_change[node as usize] {
                continue;   
            }
            
            can_change.set(node as usize, false);

            // This can be set at start time
            if random_reader.successors(node).unwrap().len() == 0 {
                continue;
            }

            map.clear();
            let curr_label = labels[node as usize].load(Relaxed);
            volumes[curr_label].fetch_sub(1, Relaxed);
            for succ in random_reader.successors(node as u64).unwrap() {
                let succ_label = labels[succ as usize].load(Relaxed);
                map.entry(succ_label).and_modify(|counter| *counter += 1).or_insert(1);
            }

            map.entry(curr_label).or_insert(0);

            let mut max = f64::MIN;
            let mut old = 0.0;
            let mut majorities = vec![];
            
            for (&label, &count) in map.iter() {
                let volume = volumes[label].load(Relaxed);

                let val = count as f64 - gamma * (volume + 1 - count) as f64;
				
                if max == val {
                    majorities.push(label);
                }

                if max < val {
                    majorities.clear();
                    max = val;
                    majorities.push(label);
                }

                if label == curr_label {
                    old = val;
                }
            }

            // We have always the current label in
            let next_label = *majorities.choose(&mut rand).unwrap();
            if next_label != curr_label {
                modified += 1;
                for succ in random_reader.successors(node as u64).unwrap() {
                    can_change.set(succ as usize, true);
                }
            }

            labels[node as usize].store(next_label, Relaxed);
            volumes[next_label].fetch_add(1, Relaxed);
            delta += max - old;

            pr.update();
        }

        pr.done();
        info!("Modified: {} Delta: {}", modified, delta);
        glob_pr.update_and_display();
    }
    glob_pr.done();
    Ok(())
}
