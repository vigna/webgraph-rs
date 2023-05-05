use anyhow::Result;

pub trait WebGraphCodesReader {
    fn read_outdegree(&mut self) -> Result<u64>;

    // node reference
    fn read_reference_offset(&mut self) -> Result<u64>;

    // run length reference copy
    fn read_block_count(&mut self) -> Result<u64>;
    fn read_blocks(&mut self) -> Result<u64>;

    // intervallizzation
    fn read_interval_count(&mut self) -> Result<u64>;
    fn read_interval_start(&mut self) -> Result<u64>;
    fn read_interval_len(&mut self) -> Result<u64>;

    // extra nodes
    fn read_first_residual(&mut self) -> Result<u64>;
    fn read_residual(&mut self) -> Result<u64>;
}

pub trait WebGraphCodesWriter {
    fn write_outdegree(&mut self) -> Result<u64>;

    // node reference
    fn write_reference_offset(&mut self) -> Result<u64>;

    // run length reference copy
    fn write_block_count(&mut self) -> Result<u64>;
    fn write_blocks(&mut self) -> Result<u64>;

    // intervallizzation
    fn write_interval_count(&mut self) -> Result<u64>;
    fn write_interval_start(&mut self) -> Result<u64>;
    fn write_interval_len(&mut self) -> Result<u64>;

    // extra nodes
    fn write_first_residual(&mut self) -> Result<u64>;
    fn write_residual(&mut self) -> Result<u64>;
}
