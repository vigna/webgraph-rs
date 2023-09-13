use anyhow::{Context, Result};
use clap::Parser;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::ProgressLogger;
use epserde::prelude::*;
use log::info;
use std::fs::File;
use std::io::{BufReader, BufWriter, Seek};
use sux::prelude::*;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(about = "Create the '.ef' file for a graph", long_about = None)]
struct Args {
    /// The basename of the graph.
    basename: String,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    let f = File::open(format!("{}.properties", args.basename)).with_context(|| {
        format!(
            "Could not load properties file: {}.properties",
            args.basename
        )
    })?;
    let map = java_properties::read(BufReader::new(f))?;
    let num_nodes = map.get("nodes").unwrap().parse::<usize>()?;

    let mut file = File::open(format!("{}.graph", args.basename))?;
    let file_len = 8 * file.seek(std::io::SeekFrom::End(0))?;

    let mut efb = EliasFanoBuilder::new(num_nodes + 1, file_len as usize);

    let mut ef_file = BufWriter::new(File::create(format!("{}.ef", args.basename))?);

    // Create the offsets file
    let of_file_str = format!("{}.offsets", args.basename);
    let of_file_path = std::path::Path::new(&of_file_str);

    let mut pr = ProgressLogger::default().display_memory();
    pr.expected_updates = Some(num_nodes);
    pr.item_name = "offset";

    // if the offset files exists, read it to build elias-fano
    if of_file_path.exists() {
        info!("The offsets file exists, reading it to build Elias-Fano");
        let of_file = BufReader::with_capacity(1 << 20, File::open(of_file_path)?);
        // create a bit reader on the file
        let mut reader =
            BufferedBitStreamRead::<BE, u64, _>::new(<FileBackend<u32, _>>::new(of_file));
        // progress bar
        pr.start("Translating offsets to EliasFano...");
        // read the graph a write the offsets
        let mut offset = 0;
        for _node_id in 0..num_nodes + 1 {
            // write where
            offset += reader.read_gamma()?;
            efb.push(offset as _)?;
            // decode the next nodes so we know where the next node_id starts
            pr.light_update();
        }
    } else {
        info!("The offsets file does not exists, reading the graph to build Elias-Fano");
        let seq_graph = webgraph::graph::bvgraph::load_seq(&args.basename)?;
        let seq_graph = seq_graph.map_codes_reader_builder(DynamicCodesReaderSkipperBuilder::from);
        // otherwise directly read the graph
        // progress bar
        pr.start("Building EliasFano...");
        // read the graph a write the offsets
        let mut iter = seq_graph.iter_degrees();
        for (new_offset, _node_id, _degree) in iter.by_ref() {
            // write where
            efb.push(new_offset as _)?;
            // decode the next nodes so we know where the next node_id starts
            pr.light_update();
        }
        efb.push(iter.get_pos() as _)?;
    }
    pr.done();

    let ef = efb.build();

    let mut pr = ProgressLogger::default().display_memory();
    pr.start("Building the Index over the ones in the high-bits...");
    let ef: webgraph::EF<_> = ef.convert_to().unwrap();
    pr.done();

    let mut pr = ProgressLogger::default().display_memory();
    pr.start("Writing to disk...");
    // serialize and dump the schema to disk
    let schema = ef.serialize_with_schema(&mut ef_file)?;
    std::fs::write(format!("{}.ef.schema", args.basename), schema.to_csv())?;

    pr.done();
    Ok(())
}
