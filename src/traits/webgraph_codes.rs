use anyhow::Result;

/// An object that can create code readers, this is done so that the builder can
/// own the data, and the readers can be created and thrown away freely
pub trait WebGraphCodesReaderBuilder {
    type Reader<'a>: WebGraphCodesReader + 'a
    where
        Self: 'a;

    /// Create a new reader at bit-offset `offset`
    fn get_reader(&self, offset: usize) -> Result<Self::Reader<'_>>;
}

pub trait WebGraphCodesSkipper {
    fn skip_outdegree(&mut self);

    // node reference
    fn skip_reference_offset(&mut self);

    // run length reference copy
    fn skip_block_count(&mut self);
    fn skip_block(&mut self);

    // intervallizzation
    fn skip_interval_count(&mut self);
    fn skip_interval_start(&mut self);
    fn skip_interval_len(&mut self);

    // extra nodes
    fn skip_first_residual(&mut self);
    fn skip_residual(&mut self);
}

pub trait WebGraphCodesReader {
    fn read_outdegree(&mut self) -> u64;

    // node reference
    fn read_reference_offset(&mut self) -> u64;

    // run length reference copy
    fn read_block_count(&mut self) -> u64;
    fn read_blocks(&mut self) -> u64;

    // intervallizzation
    fn read_interval_count(&mut self) -> u64;
    fn read_interval_start(&mut self) -> u64;
    fn read_interval_len(&mut self) -> u64;

    // extra nodes
    fn read_first_residual(&mut self) -> u64;
    fn read_residual(&mut self) -> u64;
}

pub trait WebGraphCodesWriter {
    type MockWriter: WebGraphCodesWriter;
    /// Returns a mock writer that does not write anything.
    fn mock(&self) -> Self::MockWriter;

    fn write_outdegree(&mut self, value: u64) -> Result<usize>;

    // node reference
    fn write_reference_offset(&mut self, value: u64) -> Result<usize>;

    // run length reference copy
    fn write_block_count(&mut self, value: u64) -> Result<usize>;
    fn write_blocks(&mut self, value: u64) -> Result<usize>;

    // intervallizzation
    fn write_interval_count(&mut self, value: u64) -> Result<usize>;
    fn write_interval_start(&mut self, value: u64) -> Result<usize>;
    fn write_interval_len(&mut self, value: u64) -> Result<usize>;

    // extra nodes
    fn write_first_residual(&mut self, value: u64) -> Result<usize>;
    fn write_residual(&mut self, value: u64) -> Result<usize>;

    fn flush(self) -> Result<()>;
}
