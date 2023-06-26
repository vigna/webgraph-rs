use super::*;
use anyhow::{bail, Result};
use dsi_bitstream::prelude::*;

type BitReader<'a, E> = BufferedBitStreamRead<E, u64, MemWordReadInfinite<u32, &'a [u32]>>;

pub struct DynamicCodesReaderBuilder<E: Endianness, B: AsRef<[u32]>> {
    data: B,
    compression_flags: CompFlags,
    read_outdegree: for<'a> fn(&mut BitReader<'a, E>) -> u64,
    read_reference_offset: for<'a> fn(&mut BitReader<'a, E>) -> u64,
    read_block_count: for<'a> fn(&mut BitReader<'a, E>) -> u64,
    read_blocks: for<'a> fn(&mut BitReader<'a, E>) -> u64,
    read_interval_count: for<'a> fn(&mut BitReader<'a, E>) -> u64,
    read_interval_start: for<'a> fn(&mut BitReader<'a, E>) -> u64,
    read_interval_len: for<'a> fn(&mut BitReader<'a, E>) -> u64,
    read_first_residual: for<'a> fn(&mut BitReader<'a, E>) -> u64,
    read_residual: for<'a> fn(&mut BitReader<'a, E>) -> u64,
    _marker: core::marker::PhantomData<E>,
}

impl<E: Endianness, B: AsRef<[u32]>> DynamicCodesReaderBuilder<E, B>
where
    for<'a> BitReader<'a, E>: ReadCodes<E> + BitSeek,
{
    const READ_UNARY: for<'a> fn(&mut BitReader<'a, E>) -> u64 = |cr| cr.read_unary().unwrap();
    const READ_GAMMA: for<'a> fn(&mut BitReader<'a, E>) -> u64 = |cr| cr.read_gamma().unwrap();
    const READ_DELTA: for<'a> fn(&mut BitReader<'a, E>) -> u64 = |cr| cr.read_delta().unwrap();
    const READ_ZETA2: for<'a> fn(&mut BitReader<'a, E>) -> u64 = |cr| cr.read_zeta(2).unwrap();
    const READ_ZETA3: for<'a> fn(&mut BitReader<'a, E>) -> u64 = |cr| cr.read_zeta3().unwrap();
    const READ_ZETA4: for<'a> fn(&mut BitReader<'a, E>) -> u64 = |cr| cr.read_zeta(4).unwrap();
    const READ_ZETA5: for<'a> fn(&mut BitReader<'a, E>) -> u64 = |cr| cr.read_zeta(5).unwrap();
    const READ_ZETA6: for<'a> fn(&mut BitReader<'a, E>) -> u64 = |cr| cr.read_zeta(6).unwrap();
    const READ_ZETA7: for<'a> fn(&mut BitReader<'a, E>) -> u64 = |cr| cr.read_zeta(7).unwrap();
    const READ_ZETA1: for<'a> fn(&mut BitReader<'a, E>) -> u64 = Self::READ_GAMMA;

    #[inline(always)]
    pub fn get_compression_flags(&self) -> CompFlags {
        self.compression_flags.clone()
    }

    pub fn new(data: B, cf: CompFlags) -> Result<Self> {
        macro_rules! select_code {
            ($code:expr) => {
                match $code {
                    Code::Unary => Self::READ_UNARY,
                    Code::Gamma => Self::READ_GAMMA,
                    Code::Delta => Self::READ_DELTA,
                    Code::Zeta { k: 1 } => Self::READ_ZETA1,
                    Code::Zeta { k: 2 } => Self::READ_ZETA2,
                    Code::Zeta { k: 3 } => Self::READ_ZETA3,
                    Code::Zeta { k: 4 } => Self::READ_ZETA4,
                    Code::Zeta { k: 5 } => Self::READ_ZETA5,
                    Code::Zeta { k: 6 } => Self::READ_ZETA6,
                    Code::Zeta { k: 7 } => Self::READ_ZETA7,
                    code => bail!(
                        "Only unary, ɣ, δ, and ζ₁-ζ₇ codes are allowed, {:?} is not supported",
                        code
                    ),
                }
            };
        }

        Ok(Self {
            data,
            read_outdegree: select_code!(cf.outdegrees),
            read_reference_offset: select_code!(cf.references),
            read_block_count: select_code!(cf.blocks),
            read_blocks: select_code!(cf.blocks),
            read_interval_count: select_code!(cf.intervals),
            read_interval_start: select_code!(cf.intervals),
            read_interval_len: select_code!(cf.intervals),
            read_first_residual: select_code!(cf.residuals),
            read_residual: select_code!(cf.residuals),
            compression_flags: cf,
            _marker: core::marker::PhantomData::default(),
        })
    }
}

impl<E: Endianness, B: AsRef<[u32]>> WebGraphCodesReaderBuilder for DynamicCodesReaderBuilder<E, B>
where
    for<'a> BitReader<'a, E>: ReadCodes<E> + BitSeek,
{
    type Reader<'a> =
        DynamicCodesReader<E, BitReader<'a, E>>
    where
        Self: 'a;

    fn get_reader(&self, offset: usize) -> Result<Self::Reader<'_>> {
        let mut code_reader: BitReader<'_, E> =
            BufferedBitStreamRead::new(MemWordReadInfinite::new(self.data.as_ref()));
        code_reader.set_pos(offset)?;

        Ok(DynamicCodesReader {
            code_reader,
            read_outdegree: self.read_outdegree,
            read_reference_offset: self.read_reference_offset,
            read_block_count: self.read_block_count,
            read_blocks: self.read_blocks,
            read_interval_count: self.read_interval_count,
            read_interval_start: self.read_interval_start,
            read_interval_len: self.read_interval_len,
            read_first_residual: self.read_first_residual,
            read_residual: self.read_residual,
            _marker: Default::default(),
        })
    }
}

pub struct DynamicCodesReaderSkipperBuilder<E: Endianness, B: AsRef<[u32]>> {
    data: B,
    compression_flags: CompFlags,

    read_outdegree: for<'a> fn(&mut BitReader<'a, E>) -> u64,
    read_reference_offset: for<'a> fn(&mut BitReader<'a, E>) -> u64,
    read_block_count: for<'a> fn(&mut BitReader<'a, E>) -> u64,
    read_blocks: for<'a> fn(&mut BitReader<'a, E>) -> u64,
    read_interval_count: for<'a> fn(&mut BitReader<'a, E>) -> u64,
    read_interval_start: for<'a> fn(&mut BitReader<'a, E>) -> u64,
    read_interval_len: for<'a> fn(&mut BitReader<'a, E>) -> u64,
    read_first_residual: for<'a> fn(&mut BitReader<'a, E>) -> u64,
    read_residual: for<'a> fn(&mut BitReader<'a, E>) -> u64,

    skip_outdegrees: for<'a> fn(&mut BitReader<'a, E>, usize),
    skip_reference_offsets: for<'a> fn(&mut BitReader<'a, E>, usize),
    skip_block_counts: for<'a> fn(&mut BitReader<'a, E>, usize),
    skip_blocks: for<'a> fn(&mut BitReader<'a, E>, usize),
    skip_interval_counts: for<'a> fn(&mut BitReader<'a, E>, usize),
    skip_interval_starts: for<'a> fn(&mut BitReader<'a, E>, usize),
    skip_interval_lens: for<'a> fn(&mut BitReader<'a, E>, usize),
    skip_first_residuals: for<'a> fn(&mut BitReader<'a, E>, usize),
    skip_residuals: for<'a> fn(&mut BitReader<'a, E>, usize),

    _marker: core::marker::PhantomData<E>,
}

impl<E: Endianness, B: AsRef<[u32]>> DynamicCodesReaderSkipperBuilder<E, B>
where
    for<'a> BitReader<'a, E>: ReadCodes<E> + BitSeek,
{
    const READ_UNARY: for<'a> fn(&mut BitReader<'a, E>) -> u64 = |cr| cr.read_unary().unwrap();
    const READ_GAMMA: for<'a> fn(&mut BitReader<'a, E>) -> u64 = |cr| cr.read_gamma().unwrap();
    const READ_DELTA: for<'a> fn(&mut BitReader<'a, E>) -> u64 = |cr| cr.read_delta().unwrap();
    const READ_ZETA2: for<'a> fn(&mut BitReader<'a, E>) -> u64 = |cr| cr.read_zeta(2).unwrap();
    const READ_ZETA3: for<'a> fn(&mut BitReader<'a, E>) -> u64 = |cr| cr.read_zeta3().unwrap();
    const READ_ZETA4: for<'a> fn(&mut BitReader<'a, E>) -> u64 = |cr| cr.read_zeta(4).unwrap();
    const READ_ZETA5: for<'a> fn(&mut BitReader<'a, E>) -> u64 = |cr| cr.read_zeta(5).unwrap();
    const READ_ZETA6: for<'a> fn(&mut BitReader<'a, E>) -> u64 = |cr| cr.read_zeta(6).unwrap();
    const READ_ZETA7: for<'a> fn(&mut BitReader<'a, E>) -> u64 = |cr| cr.read_zeta(7).unwrap();
    const READ_ZETA1: for<'a> fn(&mut BitReader<'a, E>) -> u64 = Self::READ_GAMMA;

    const SKIP_UNARY: for<'a> fn(&mut BitReader<'a, E>, usize) = |cr, n| cr.skip_unary(n).unwrap();
    const SKIP_GAMMA: for<'a> fn(&mut BitReader<'a, E>, usize) = |cr, n| cr.skip_gamma(n).unwrap();
    const SKIP_DELTA: for<'a> fn(&mut BitReader<'a, E>, usize) = |cr, n| cr.skip_delta(n).unwrap();
    const SKIP_ZETA2: for<'a> fn(&mut BitReader<'a, E>, usize) =
        |cr, n| cr.skip_zeta(2, n).unwrap();
    const SKIP_ZETA3: for<'a> fn(&mut BitReader<'a, E>, usize) = |cr, n| cr.skip_zeta3(n).unwrap();
    const SKIP_ZETA4: for<'a> fn(&mut BitReader<'a, E>, usize) =
        |cr, n| cr.skip_zeta(4, n).unwrap();
    const SKIP_ZETA5: for<'a> fn(&mut BitReader<'a, E>, usize) =
        |cr, n| cr.skip_zeta(5, n).unwrap();
    const SKIP_ZETA6: for<'a> fn(&mut BitReader<'a, E>, usize) =
        |cr, n| cr.skip_zeta(6, n).unwrap();
    const SKIP_ZETA7: for<'a> fn(&mut BitReader<'a, E>, usize) =
        |cr, n| cr.skip_zeta(7, n).unwrap();
    const SKIP_ZETA1: for<'a> fn(&mut BitReader<'a, E>, usize) = Self::SKIP_GAMMA;

    #[inline(always)]
    pub fn get_compression_flags(&self) -> CompFlags {
        self.compression_flags.clone()
    }

    pub fn new(data: B, cf: CompFlags) -> Result<Self> {
        macro_rules! select_code {
            ($code:expr) => {
                match $code {
                    Code::Unary => Self::READ_UNARY,
                    Code::Gamma => Self::READ_GAMMA,
                    Code::Delta => Self::READ_DELTA,
                    Code::Zeta { k: 1 } => Self::READ_ZETA1,
                    Code::Zeta { k: 2 } => Self::READ_ZETA2,
                    Code::Zeta { k: 3 } => Self::READ_ZETA3,
                    Code::Zeta { k: 4 } => Self::READ_ZETA4,
                    Code::Zeta { k: 5 } => Self::READ_ZETA5,
                    Code::Zeta { k: 6 } => Self::READ_ZETA6,
                    Code::Zeta { k: 7 } => Self::READ_ZETA7,
                    code => bail!(
                        "Only unary, ɣ, δ, and ζ₁-ζ₇ codes are allowed, {:?} is not supported",
                        code
                    ),
                }
            };
        }

        macro_rules! select_skip_code {
            ($code:expr) => {
                match $code {
                    Code::Unary => Self::SKIP_UNARY,
                    Code::Gamma => Self::SKIP_GAMMA,
                    Code::Delta => Self::SKIP_DELTA,
                    Code::Zeta { k: 1 } => Self::SKIP_ZETA1,
                    Code::Zeta { k: 2 } => Self::SKIP_ZETA2,
                    Code::Zeta { k: 3 } => Self::SKIP_ZETA3,
                    Code::Zeta { k: 4 } => Self::SKIP_ZETA4,
                    Code::Zeta { k: 5 } => Self::SKIP_ZETA5,
                    Code::Zeta { k: 6 } => Self::SKIP_ZETA6,
                    Code::Zeta { k: 7 } => Self::SKIP_ZETA7,
                    code => bail!(
                        "Only unary, ɣ, δ, and ζ₁-ζ₇ codes are allowed, {:?} is not supported",
                        code
                    ),
                }
            };
        }

        Ok(Self {
            data,

            read_outdegree: select_code!(cf.outdegrees),
            read_reference_offset: select_code!(cf.references),
            read_block_count: select_code!(cf.blocks),
            read_blocks: select_code!(cf.blocks),
            read_interval_count: select_code!(cf.intervals),
            read_interval_start: select_code!(cf.intervals),
            read_interval_len: select_code!(cf.intervals),
            read_first_residual: select_code!(cf.residuals),
            read_residual: select_code!(cf.residuals),

            skip_outdegrees: select_skip_code!(cf.outdegrees),
            skip_reference_offsets: select_skip_code!(cf.references),
            skip_block_counts: select_skip_code!(cf.blocks),
            skip_blocks: select_skip_code!(cf.blocks),
            skip_interval_counts: select_skip_code!(cf.intervals),
            skip_interval_starts: select_skip_code!(cf.intervals),
            skip_interval_lens: select_skip_code!(cf.intervals),
            skip_first_residuals: select_skip_code!(cf.residuals),
            skip_residuals: select_skip_code!(cf.residuals),

            compression_flags: cf,
            _marker: core::marker::PhantomData::default(),
        })
    }
}

impl<E: Endianness, B: AsRef<[u32]>> WebGraphCodesReaderBuilder
    for DynamicCodesReaderSkipperBuilder<E, B>
where
    for<'a> BitReader<'a, E>: ReadCodes<E> + BitSeek,
{
    type Reader<'a> =
        DynamicCodesReaderSkipper<E, BitReader<'a, E>>
    where
        Self: 'a;

    #[inline(always)]
    fn get_reader(&self, offset: usize) -> Result<Self::Reader<'_>> {
        let mut code_reader: BitReader<'_, E> =
            BufferedBitStreamRead::new(MemWordReadInfinite::new(self.data.as_ref()));
        code_reader.set_pos(offset)?;
        Ok(DynamicCodesReaderSkipper {
            code_reader,
            read_outdegree: self.read_outdegree,
            read_reference_offset: self.read_reference_offset,
            read_block_count: self.read_block_count,
            read_blocks: self.read_blocks,
            read_interval_count: self.read_interval_count,
            read_interval_start: self.read_interval_start,
            read_interval_len: self.read_interval_len,
            read_first_residual: self.read_first_residual,
            read_residual: self.read_residual,
            skip_outdegrees: self.skip_outdegrees,
            skip_reference_offsets: self.skip_reference_offsets,
            skip_block_counts: self.skip_block_counts,
            skip_blocks: self.skip_blocks,
            skip_interval_counts: self.skip_interval_counts,
            skip_interval_starts: self.skip_interval_starts,
            skip_interval_lens: self.skip_interval_lens,
            skip_first_residuals: self.skip_first_residuals,
            skip_residuals: self.skip_residuals,
            _marker: core::marker::PhantomData,
        })
    }
}

impl<E: Endianness, B: AsRef<[u32]>> From<DynamicCodesReaderBuilder<E, B>>
    for DynamicCodesReaderSkipperBuilder<E, B>
where
    for<'a> BitReader<'a, E>: ReadCodes<E> + BitSeek,
{
    #[inline(always)]
    fn from(value: DynamicCodesReaderBuilder<E, B>) -> Self {
        Self::new(value.data, value.compression_flags).unwrap()
    }
}

impl<E: Endianness, B: AsRef<[u32]>> From<DynamicCodesReaderSkipperBuilder<E, B>>
    for DynamicCodesReaderBuilder<E, B>
where
    for<'a> BitReader<'a, E>: ReadCodes<E> + BitSeek,
{
    #[inline(always)]
    fn from(value: DynamicCodesReaderSkipperBuilder<E, B>) -> Self {
        Self::new(value.data, value.compression_flags).unwrap()
    }
}

pub struct ConstCodesReaderBuilder<
    E: Endianness,
    B: AsRef<[u32]>,
    const OUTDEGREES: usize = { const_codes::GAMMA },
    const REFERENCES: usize = { const_codes::UNARY },
    const BLOCKS: usize = { const_codes::GAMMA },
    const INTERVALS: usize = { const_codes::GAMMA },
    const RESIDUALS: usize = { const_codes::ZETA },
    const K: u64 = 3,
> {
    data: B,
    _marker: core::marker::PhantomData<E>,
}

impl<
        E: Endianness,
        B: AsRef<[u32]>,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
        const K: u64,
    > ConstCodesReaderBuilder<E, B, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>
{
    pub fn new(data: B, comp_flags: CompFlags) -> Result<Self> {
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
            data,
            _marker: core::marker::PhantomData::default(),
        })
    }
}

impl<
        E: Endianness,
        B: AsRef<[u32]>,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
        const K: u64,
    > WebGraphCodesReaderBuilder
    for ConstCodesReaderBuilder<E, B, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>
where
    for<'a> BitReader<'a, E>: ReadCodes<E> + BitSeek,
{
    type Reader<'a> =
        ConstCodesReader<E, BitReader<'a, E>>
    where
        Self: 'a;

    fn get_reader(&self, offset: usize) -> Result<Self::Reader<'_>> {
        let mut code_reader: BitReader<'_, E> =
            BufferedBitStreamRead::new(MemWordReadInfinite::new(self.data.as_ref()));
        code_reader.set_pos(offset)?;

        Ok(ConstCodesReader {
            code_reader,
            _marker: Default::default(),
        })
    }
}
