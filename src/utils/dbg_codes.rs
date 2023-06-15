use crate::traits::*;
use anyhow::Result;
use dsi_bitstream::prelude::*;

/// A wrapper over a code reader that prints on stdout all the codes read
pub struct DbgCodeRead<E: Endianness, CR: ReadCodes<E>> {
    reader: CR,
    _marker: core::marker::PhantomData<E>,
}

impl<E: Endianness, CR: ReadCodes<E>> DbgCodeRead<E, CR> {
    pub fn new(cr: CR) -> Self {
        Self {
            reader: cr,
            _marker: Default::default(),
        }
    }
}

impl<E: Endianness, CR: ReadCodes<E>> BitRead<E> for DbgCodeRead<E, CR>
where
    CR::PeekType: core::fmt::Display,
{
    type PeekType = CR::PeekType;

    fn peek_bits(&mut self, n_bits: usize) -> Result<Self::PeekType> {
        let value = self.reader.peek_bits(n_bits)?;
        println!("peek_bits({}): {}", n_bits, value);
        Ok(value)
    }
    fn skip_bits(&mut self, n_bits: usize) -> Result<()> {
        println!("skip_bits({})", n_bits);
        self.reader.skip_bits(n_bits)
    }
    fn read_bits(&mut self, n_bits: usize) -> Result<u64> {
        let value = self.reader.read_bits(n_bits)?;
        println!("read_bits({}): {}", n_bits, value);
        Ok(value)
    }
    fn read_unary(&mut self) -> Result<u64> {
        let value = self.reader.read_unary()?;
        println!("{{U:{}}}", value);
        Ok(value)
    }
}

impl<E: Endianness, CR: ReadCodes<E>> GammaRead<E> for DbgCodeRead<E, CR>
where
    CR::PeekType: core::fmt::Display,
{
    fn read_gamma(&mut self) -> Result<u64> {
        let value = self.reader.read_gamma()?;
        println!("{{g:{}}}", value);
        Ok(value)
    }

    fn skip_gammas(&mut self, n: usize) -> Result<usize> {
        let value = self.reader.skip_gammas(n)?;
        println!("{{skip {} g:{}}}", n, value);
        Ok(value)
    }
}

impl<E: Endianness, CR: ReadCodes<E>> DeltaRead<E> for DbgCodeRead<E, CR>
where
    CR::PeekType: core::fmt::Display,
{
    fn read_delta(&mut self) -> Result<u64> {
        let value = self.reader.read_delta()?;
        println!("{{d:{}}}", value);
        Ok(value)
    }
}

impl<E: Endianness, CR: ReadCodes<E>> ZetaRead<E> for DbgCodeRead<E, CR>
where
    CR::PeekType: core::fmt::Display,
{
    fn read_zeta3(&mut self) -> Result<u64> {
        let value = self.reader.read_zeta3()?;
        println!("{{z3:{}}}", value);
        Ok(value)
    }

    fn read_zeta(&mut self, k: u64) -> Result<u64> {
        let value = self.reader.read_zeta(k)?;
        println!("{{z{}:{}}}", k, value);
        Ok(value)
    }
}

/// A wrapper over a code writer that prints on stdout all the codes written
pub struct DbgCodeWrite<E: Endianness, CW: WriteCodes<E>> {
    writer: CW,
    _marker: core::marker::PhantomData<E>,
}

impl<E: Endianness, CW: WriteCodes<E>> DbgCodeWrite<E, CW> {
    pub fn new(cw: CW) -> Self {
        Self {
            writer: cw,
            _marker: Default::default(),
        }
    }
}

impl<E: Endianness, CW: WriteCodes<E>> BitWrite<E> for DbgCodeWrite<E, CW> {
    fn write_bits(&mut self, value: u64, n_bits: usize) -> Result<usize> {
        println!("write_bits({}, {})", value, n_bits);
        self.writer.write_bits(value, n_bits)
    }
    fn write_unary(&mut self, value: u64) -> Result<usize> {
        println!("{{U:{}}}", value);
        self.writer.write_unary(value)
    }
    fn flush(self) -> Result<()> {
        self.writer.flush()
    }
}

impl<E: Endianness, CW: WriteCodes<E>> GammaWrite<E> for DbgCodeWrite<E, CW> {
    fn write_gamma(&mut self, value: u64) -> Result<usize> {
        println!("{{g:{}}}", value);
        self.writer.write_gamma(value)
    }
}

impl<E: Endianness, CW: WriteCodes<E>> DeltaWrite<E> for DbgCodeWrite<E, CW> {
    fn write_delta(&mut self, value: u64) -> Result<usize> {
        println!("{{d:{}}}", value);
        self.writer.write_delta(value)
    }
}

impl<E: Endianness, CW: WriteCodes<E>> ZetaWrite<E> for DbgCodeWrite<E, CW> {
    fn write_zeta(&mut self, value: u64, k: u64) -> Result<usize> {
        println!("{{z{}:{}}}", value, k);
        self.writer.write_zeta(value, k)
    }
    fn write_zeta3(&mut self, value: u64) -> Result<usize> {
        println!("{{z3:{}}}", value);
        self.writer.write_zeta3(value)
    }
}
