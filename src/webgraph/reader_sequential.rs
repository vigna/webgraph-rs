use super::*;
use crate::utils::nat2int;
use core::iter::Peekable;

pub struct WebgraphReaderSequential<'a, CR: WebGraphCodesReader> {
    codes_reader: &'a mut CR,
    backrefs: CircularBuffer,
    min_interval_length: usize,
}