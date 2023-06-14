//use super::*;

pub const PLZCOMPILE: usize = 0;

/*
pub fn load(basename: &str) -> BVGraphDefault<Vec<u64>> {
    let f = File::open(format!("{}.properties", basename))?;
    let map = java_properties::read(BufReader::new(f))?;

    let mut file = std::fs::File::open(format!("{}.graph", basename)).unwrap();
    let mut file_len = file.seek(std::io::SeekFrom::End(0)).unwrap();

    // align the len to readtypes, TODO!: arithmize
    while file_len % std::mem::size_of::<ReadType>() as u64 != 0 {
        file_len += 1;
    }

    let data = unsafe {
        MmapOptions::new(file_len as _)
            .unwrap()
            .with_file(file, 0)
            .map()
            .unwrap()
    };

    let code_reader = ConstCodesReader::new(
        BufferedBitStreamRead::<BE, BufferType, _>::new(MemWordReadInfinite::new(
            MmapBackend::new(data),
        )),
        &CompFlags::default(),
    )?;
    let seq_reader = WebgraphSequentialIter::new(
        code_reader,
        map.get("windowsize").unwrap().parse::<usize>()?,
        map.get("minintervallength").unwrap().parse::<usize>()?,
        map.get("nodes").unwrap().parse::<usize>()?,
    );

    Ok(seq_reader)
}


pub fn load_mapped(basename: &str) -> BVGraphDefault<Vec<u64>> {
    todo!();
}

pub fn load_seq(basename: &str) -> BVGraphDefault<Vec<u64>> {
    todo!();
}

pub fn load_seq_mapped(basename: &str) -> BVGraphDefault<Vec<u64>> {
    todo!();
}
*/
