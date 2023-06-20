use super::*;
use anyhow::{bail, Result};
use dsi_bitstream::prelude::*;

type BitReader<'a> = BufferedBitStreamRead<BE, u64, MemWordReadInfinite<u32, &'a [u32]>>;

pub struct DynamicCodesReaderBuilder<E: Endianness, B: AsRef<[u32]>> {
    data: B,
    read_outdegree: for<'a> fn(&mut BitReader<'a>) -> u64,
    read_reference_offset: for<'a> fn(&mut BitReader<'a>) -> u64,
    read_block_count: for<'a> fn(&mut BitReader<'a>) -> u64,
    read_blocks: for<'a> fn(&mut BitReader<'a>) -> u64,
    read_interval_count: for<'a> fn(&mut BitReader<'a>) -> u64,
    read_interval_start: for<'a> fn(&mut BitReader<'a>) -> u64,
    read_interval_len: for<'a> fn(&mut BitReader<'a>) -> u64,
    read_first_residual: for<'a> fn(&mut BitReader<'a>) -> u64,
    read_residual: for<'a> fn(&mut BitReader<'a>) -> u64,
    _marker: core::marker::PhantomData<E>,
}

impl<E: Endianness, B: AsRef<[u32]>> DynamicCodesReaderBuilder<E, B> {
    pub fn new(data: B, cf: &CompFlags) -> Result<Self> {
        macro_rules! select_code {
            ($code:expr) => {
                match $code {
                    Code::Unary => |x| x.read_unary().unwrap(),
                    Code::Gamma => |x| x.read_gamma().unwrap(),
                    Code::Delta => |x| x.read_delta().unwrap(),
                    Code::Zeta { k: 1 } => |x| x.read_gamma().unwrap(),
                    Code::Zeta { k: 2 } => |x| x.read_zeta(2).unwrap(),
                    Code::Zeta { k: 3 } => |x| x.read_zeta3().unwrap(),
                    Code::Zeta { k: 4 } => |x| x.read_zeta(4).unwrap(),
                    Code::Zeta { k: 5 } => |x| x.read_zeta(5).unwrap(),
                    Code::Zeta { k: 6 } => |x| x.read_zeta(6).unwrap(),
                    Code::Zeta { k: 7 } => |x| x.read_zeta(7).unwrap(),
                    _ => bail!("Only unary, ɣ, δ, and ζ₁-ζ₇ codes are allowed"),
                }
            };
        }

        Ok(Self {
            data,
            read_outdegree: select_code!(&cf.outdegrees),
            read_reference_offset: select_code!(&cf.references),
            read_block_count: select_code!(&cf.blocks),
            read_blocks: select_code!(&cf.blocks),
            read_interval_count: select_code!(&cf.intervals),
            read_interval_start: select_code!(&cf.intervals),
            read_interval_len: select_code!(&cf.intervals),
            read_first_residual: select_code!(&cf.residuals),
            read_residual: select_code!(&cf.residuals),
            _marker: core::marker::PhantomData::default(),
        })
    }
}

impl<E: Endianness, B: AsRef<[u32]>> WebGraphCodesReaderBuilder
    for DynamicCodesReaderBuilder<E, B>
{
    type Reader<'a> =
        DynamicCodesReader<BE, BitReader<'a>>
    where
        Self: 'a;

    fn get_reader(&self, offset: usize) -> Result<Self::Reader<'_>> {
        let mut code_reader: BitReader<'_> =
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
    // TODO!: This can be optimized by caching the skip pointers
    dynamic_codes_reader_builder: DynamicCodesReaderBuilder<E, B>,
}

impl<E: Endianness, B: AsRef<[u32]>> WebGraphCodesReaderBuilder
    for DynamicCodesReaderSkipperBuilder<E, B>
{
    type Reader<'a> =
        DynamicCodesReaderSkipper<BE, BitReader<'a>>
    where
        Self: 'a;

    #[inline(always)]
    fn get_reader(&self, offset: usize) -> Result<Self::Reader<'_>> {
        self.dynamic_codes_reader_builder
            .get_reader(offset)
            .map(|x| x.into())
    }
}

impl<E: Endianness, B: AsRef<[u32]>> From<DynamicCodesReaderBuilder<E, B>>
    for DynamicCodesReaderSkipperBuilder<E, B>
{
    #[inline(always)]
    fn from(value: DynamicCodesReaderBuilder<E, B>) -> Self {
        Self {
            dynamic_codes_reader_builder: value,
        }
    }
}

impl<E: Endianness, B: AsRef<[u32]>> From<DynamicCodesReaderSkipperBuilder<E, B>>
    for DynamicCodesReaderBuilder<E, B>
{
    #[inline(always)]
    fn from(value: DynamicCodesReaderSkipperBuilder<E, B>) -> Self {
        value.dynamic_codes_reader_builder
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
    pub fn new(data: B, comp_flags: &CompFlags) -> Result<Self> {
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
{
    type Reader<'a> =
        ConstCodesReader<BE, BitReader<'a>>
    where
        Self: 'a;

    fn get_reader(&self, offset: usize) -> Result<Self::Reader<'_>> {
        let mut code_reader: BitReader<'_> =
            BufferedBitStreamRead::new(MemWordReadInfinite::new(self.data.as_ref()));
        code_reader.set_pos(offset)?;

        Ok(ConstCodesReader {
            code_reader,
            _marker: Default::default(),
        })
    }
}
