use super::*;
use anyhow::bail;
use anyhow::Result;
use dsi_bitstream::prelude::*;

/// Temporary constants while const enum generics are not stable
pub mod const_codes {
    pub const UNARY: usize = 0;
    pub const GAMMA: usize = 1;
    pub const DELTA: usize = 2;
    pub const ZETA: usize = 3;
}

/// Temporary convertion function while const enum generics are not stable
pub(crate) fn code_to_const(code: Code) -> Result<usize> {
    Ok(match code {
        Code::Unary => const_codes::UNARY,
        Code::Gamma => const_codes::GAMMA,
        Code::Delta => const_codes::DELTA,
        Code::Zeta { k: _ } => const_codes::ZETA,
        _ => bail!("Only unary, ɣ, δ, and ζ codes are allowed"),
    })
}

#[repr(transparent)]
/// An implementation of [`WebGraphCodesReader`]  with compile-time defined codes
#[derive(Clone)]
pub struct ConstCodesReader<
    E: Endianness,
    CR: ReadCodes<E>,
    const OUTDEGREES: usize = { const_codes::GAMMA },
    const REFERENCES: usize = { const_codes::UNARY },
    const BLOCKS: usize = { const_codes::GAMMA },
    const INTERVALS: usize = { const_codes::GAMMA },
    const RESIDUALS: usize = { const_codes::ZETA },
    const K: u64 = 3,
> {
    pub(crate) code_reader: CR,
    pub(crate) _marker: core::marker::PhantomData<E>,
}

impl<
        E: Endianness,
        CR: ReadCodes<E> + BitSeek,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
        const K: u64,
    > BitSeek for ConstCodesReader<E, CR, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>
{
    fn set_pos(&mut self, bit_index: usize) -> Result<()> {
        self.code_reader.set_pos(bit_index)
    }

    fn get_pos(&self) -> usize {
        self.code_reader.get_pos()
    }
}

impl<
        E: Endianness,
        CR: ReadCodes<E>,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
        const K: u64,
    > ConstCodesReader<E, CR, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>
{
    pub fn new(code_reader: CR, comp_flags: &CompFlags) -> Result<Self> {
        if code_to_const(comp_flags.outdegrees)? != OUTDEGREES {
            bail!("Code for outdegrees does not match");
        }
        if code_to_const(comp_flags.references)? != REFERENCES {
            bail!("Cod for references does not match");
        }
        if code_to_const(comp_flags.blocks)? != BLOCKS {
            bail!("Code for blocks does not match");
        }
        if code_to_const(comp_flags.intervals)? != INTERVALS {
            bail!("Code for intervals does not match");
        }
        if code_to_const(comp_flags.residuals)? != RESIDUALS {
            bail!("Code for residuals does not match");
        }
        Ok(Self {
            code_reader,
            _marker: core::marker::PhantomData::default(),
        })
    }
}

macro_rules! select_code_read {
    ($self:ident, $code:expr, $k: expr) => {
        match $code {
            const_codes::UNARY => $self.code_reader.read_unary().unwrap(),
            const_codes::GAMMA => $self.code_reader.read_gamma().unwrap(),
            const_codes::DELTA => $self.code_reader.read_delta().unwrap(),
            const_codes::ZETA if $k == 1 => $self.code_reader.read_gamma().unwrap(),
            const_codes::ZETA if $k == 3 => $self.code_reader.read_zeta3().unwrap(),
            const_codes::ZETA => $self.code_reader.read_zeta(K).unwrap(),
            _ => panic!("Only values in the range [0..4) are allowed to represent codes"),
        }
    };
}

macro_rules! select_code_skip {
    ($self:ident, $code:expr, $k: expr, $n:expr) => {
        match $code {
            const_codes::UNARY => $self.code_reader.skip_unary($n).unwrap(),
            const_codes::GAMMA => $self.code_reader.skip_gamma($n).unwrap(),
            const_codes::DELTA => $self.code_reader.skip_delta($n).unwrap(),
            const_codes::ZETA if $k == 1 => $self.code_reader.skip_gamma($n).unwrap(),
            const_codes::ZETA if $k == 3 => $self.code_reader.skip_zeta3($n).unwrap(),
            const_codes::ZETA => $self.code_reader.skip_zeta(K, $n).unwrap(),
            _ => panic!("Only values in the range [0..4) are allowed to represent codes"),
        }
    };
}

impl<
        E: Endianness,
        CR: ReadCodes<E>,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
        const K: u64,
    > WebGraphCodesReader
    for ConstCodesReader<E, CR, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>
{
    #[inline(always)]
    fn read_outdegree(&mut self) -> u64 {
        select_code_read!(self, OUTDEGREES, K)
    }

    #[inline(always)]
    fn read_reference_offset(&mut self) -> u64 {
        select_code_read!(self, REFERENCES, K)
    }

    #[inline(always)]
    fn read_block_count(&mut self) -> u64 {
        select_code_read!(self, BLOCKS, K)
    }
    #[inline(always)]
    fn read_blocks(&mut self) -> u64 {
        select_code_read!(self, BLOCKS, K)
    }

    #[inline(always)]
    fn read_interval_count(&mut self) -> u64 {
        select_code_read!(self, INTERVALS, K)
    }
    #[inline(always)]
    fn read_interval_start(&mut self) -> u64 {
        select_code_read!(self, INTERVALS, K)
    }
    #[inline(always)]
    fn read_interval_len(&mut self) -> u64 {
        select_code_read!(self, INTERVALS, K)
    }

    #[inline(always)]
    fn read_first_residual(&mut self) -> u64 {
        select_code_read!(self, RESIDUALS, K)
    }
    #[inline(always)]
    fn read_residual(&mut self) -> u64 {
        select_code_read!(self, RESIDUALS, K)
    }
}

impl<
        E: Endianness,
        CR: ReadCodes<E>,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
        const K: u64,
    > WebGraphCodesSkipper
    for ConstCodesReader<E, CR, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>
{
    #[inline(always)]
    fn skip_outdegrees(&mut self, n: usize) -> usize {
        select_code_skip!(self, OUTDEGREES, K, n)
    }

    #[inline(always)]
    fn skip_reference_offsets(&mut self, n: usize) -> usize {
        select_code_skip!(self, REFERENCES, K, n)
    }

    #[inline(always)]
    fn skip_block_counts(&mut self, n: usize) -> usize {
        select_code_skip!(self, BLOCKS, K, n)
    }
    #[inline(always)]
    fn skip_blocks(&mut self, n: usize) -> usize {
        select_code_skip!(self, BLOCKS, K, n)
    }

    #[inline(always)]
    fn skip_interval_counts(&mut self, n: usize) -> usize {
        select_code_skip!(self, INTERVALS, K, n)
    }
    #[inline(always)]
    fn skip_interval_starts(&mut self, n: usize) -> usize {
        select_code_skip!(self, INTERVALS, K, n)
    }
    #[inline(always)]
    fn skip_interval_lens(&mut self, n: usize) -> usize {
        select_code_skip!(self, INTERVALS, K, n)
    }

    #[inline(always)]
    fn skip_first_residuals(&mut self, n: usize) -> usize {
        select_code_skip!(self, RESIDUALS, K, n)
    }
    #[inline(always)]
    fn skip_residuals(&mut self, n: usize) -> usize {
        select_code_skip!(self, RESIDUALS, K, n)
    }
}

#[repr(transparent)]
/// An implementation of [`WebGraphCodesWriter`] with compile time defined codes
#[derive(Clone)]
pub struct ConstCodesWriter<
    E: Endianness,
    CW: WriteCodes<E>,
    const OUTDEGREES: usize = { const_codes::GAMMA },
    const REFERENCES: usize = { const_codes::UNARY },
    const BLOCKS: usize = { const_codes::GAMMA },
    const INTERVALS: usize = { const_codes::GAMMA },
    const RESIDUALS: usize = { const_codes::ZETA },
    const K: u64 = 3,
> {
    code_writer: CW,
    _marker: core::marker::PhantomData<E>,
}

impl<
        E: Endianness,
        CW: WriteCodes<E> + BitSeek,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
        const K: u64,
    > BitSeek for ConstCodesWriter<E, CW, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>
{
    fn set_pos(&mut self, bit_index: usize) -> Result<()> {
        self.code_writer.set_pos(bit_index)
    }

    fn get_pos(&self) -> usize {
        self.code_writer.get_pos()
    }
}

impl<
        E: Endianness,
        CW: WriteCodes<E>,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
        const K: u64,
    > ConstCodesWriter<E, CW, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>
{
    pub fn new(code_writer: CW) -> Self {
        Self {
            code_writer,
            _marker: core::marker::PhantomData::default(),
        }
    }
}

macro_rules! select_code_write {
    ($self:ident, $code:expr, $k: expr, $value:expr) => {
        match $code {
            const_codes::UNARY => $self.code_writer.write_unary($value),
            const_codes::GAMMA => $self.code_writer.write_gamma($value),
            const_codes::DELTA => $self.code_writer.write_delta($value),
            const_codes::ZETA if $k == 1 => $self.code_writer.write_gamma($value),
            const_codes::ZETA if $k == 3 => $self.code_writer.write_zeta3($value),
            const_codes::ZETA => $self.code_writer.write_zeta($value, K),
            _ => panic!("Only values in the range [0..4) are allowed to represent codes"),
        }
    };
}

impl<
        E: Endianness,
        CW: WriteCodes<E>,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
        const K: u64,
    > WebGraphCodesWriter
    for ConstCodesWriter<E, CW, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>
{
    type MockWriter = ConstCodesMockWriter<OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>;
    fn mock(&self) -> Self::MockWriter {
        ConstCodesMockWriter::new()
    }

    #[inline(always)]
    fn write_outdegree(&mut self, value: u64) -> Result<usize> {
        select_code_write!(self, OUTDEGREES, K, value)
    }

    #[inline(always)]
    fn write_reference_offset(&mut self, value: u64) -> Result<usize> {
        select_code_write!(self, REFERENCES, K, value)
    }

    #[inline(always)]
    fn write_block_count(&mut self, value: u64) -> Result<usize> {
        select_code_write!(self, BLOCKS, K, value)
    }
    #[inline(always)]
    fn write_blocks(&mut self, value: u64) -> Result<usize> {
        select_code_write!(self, BLOCKS, K, value)
    }

    #[inline(always)]
    fn write_interval_count(&mut self, value: u64) -> Result<usize> {
        select_code_write!(self, INTERVALS, K, value)
    }
    #[inline(always)]
    fn write_interval_start(&mut self, value: u64) -> Result<usize> {
        select_code_write!(self, INTERVALS, K, value)
    }
    #[inline(always)]
    fn write_interval_len(&mut self, value: u64) -> Result<usize> {
        select_code_write!(self, INTERVALS, K, value)
    }

    #[inline(always)]
    fn write_first_residual(&mut self, value: u64) -> Result<usize> {
        select_code_write!(self, RESIDUALS, K, value)
    }
    #[inline(always)]
    fn write_residual(&mut self, value: u64) -> Result<usize> {
        select_code_write!(self, RESIDUALS, K, value)
    }

    fn flush(self) -> Result<()> {
        self.code_writer.flush()
    }
}

#[repr(transparent)]
/// An implementation of [`WebGraphCodesWriter`] that doesn't write but just
/// returns the number of bits that would be written.
#[derive(Clone, Default)]
pub struct ConstCodesMockWriter<
    const OUTDEGREES: usize = { const_codes::GAMMA },
    const REFERENCES: usize = { const_codes::UNARY },
    const BLOCKS: usize = { const_codes::GAMMA },
    const INTERVALS: usize = { const_codes::GAMMA },
    const RESIDUALS: usize = { const_codes::ZETA },
    const K: u64 = 3,
>;

impl<
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
        const K: u64,
    > ConstCodesMockWriter<OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>
{
    pub fn new() -> Self {
        Self
    }
}

macro_rules! select_code_mock_write {
    ( $code:expr, $k: expr, $value:expr) => {
        Ok(match $code {
            const_codes::UNARY => len_unary($value),
            const_codes::GAMMA => len_gamma($value),
            const_codes::DELTA => len_delta($value),
            const_codes::ZETA => len_zeta($value, K),
            _ => panic!("Only values in the range [0..4) are allowed to represent codes"),
        })
    };
}

impl<
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
        const K: u64,
    > WebGraphCodesWriter
    for ConstCodesMockWriter<OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>
{
    type MockWriter = Self;
    fn mock(&self) -> Self::MockWriter {
        ConstCodesMockWriter::new()
    }

    #[inline(always)]
    fn write_outdegree(&mut self, value: u64) -> Result<usize> {
        select_code_mock_write!(OUTDEGREES, K, value)
    }

    #[inline(always)]
    fn write_reference_offset(&mut self, value: u64) -> Result<usize> {
        select_code_mock_write!(REFERENCES, K, value)
    }

    #[inline(always)]
    fn write_block_count(&mut self, value: u64) -> Result<usize> {
        select_code_mock_write!(BLOCKS, K, value)
    }
    #[inline(always)]
    fn write_blocks(&mut self, value: u64) -> Result<usize> {
        select_code_mock_write!(BLOCKS, K, value)
    }

    #[inline(always)]
    fn write_interval_count(&mut self, value: u64) -> Result<usize> {
        select_code_mock_write!(INTERVALS, K, value)
    }
    #[inline(always)]
    fn write_interval_start(&mut self, value: u64) -> Result<usize> {
        select_code_mock_write!(INTERVALS, K, value)
    }
    #[inline(always)]
    fn write_interval_len(&mut self, value: u64) -> Result<usize> {
        select_code_mock_write!(INTERVALS, K, value)
    }

    #[inline(always)]
    fn write_first_residual(&mut self, value: u64) -> Result<usize> {
        select_code_mock_write!(RESIDUALS, K, value)
    }
    #[inline(always)]
    fn write_residual(&mut self, value: u64) -> Result<usize> {
        select_code_mock_write!(RESIDUALS, K, value)
    }

    fn flush(self) -> Result<()> {
        Ok(())
    }
}
