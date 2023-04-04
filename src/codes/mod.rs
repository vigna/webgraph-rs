//! This modules contains all the logic to read and write codes. While it's used
//! by webgraph it's not a part of webgraph. In the future we should move this
//! to its own crate, while we decide on the logistic of where to put it, 
//! it will stay here to go on with the developement of the library.
//! 
//! **The convention is to read bits from the MSB to the LSB of each byte.**
//! 
//! #### Example: 
//! The following stream of bits, to be read from left to right, from top to 
//! bottom:
//! ```text
//! 76543210 bit order
//! --------
//! 01110110 01100000 11110001 11001101 10011111 10110101 01000011 00000000 
//! 10000110 10011011 01110011 11111001 11100110 01100011 00101000 01110000 
//! ```
//! is equivalent to the following stream of bytes:
//! ```text
//! 76 60 f1 cd 9f b5 43 00
//! 86 9b 73 f9 e6 63 28 70
//! ```
//! In code:
//! ```
//! use webgraph::codes::*;
//! // file data
//! let data: [u8; 16] = [
//!     0x76, 0x60, 0xf1, 0xcd, 0x9f, 0xb5, 0x43, 0x00,
//!     0x86, 0x9b, 0x73, 0xf9, 0xe6, 0x63, 0x28, 0x70,
//! ];
//! // Read data as native endianess [`u64`]s, we can't just do a 
//! // transmute because we have no guarantees on the alignement of data
//! let words = data.chunks(8)
//!     .map(|data| u64::from_ne_bytes(data.try_into().unwrap()))
//!     .collect::<Vec<_>>();
//! 
//! // create the bitstream
//! let word_reader = MemWordReader::new(&words);
//! let mut bitstream = BufferedBitStreamReader::new(word_reader);
//! 
//! assert_eq!(bitstream.read_bits(8).unwrap(), 0b0111_0110);
//! assert_eq!(bitstream.read_bits(4).unwrap(), 0b0110);
//! assert_eq!(bitstream.read_bits(4).unwrap(), 0b0000);
//! assert_eq!(bitstream.read_bits(10).unwrap(), 0b1111_0001_11);
//! assert_eq!(bitstream.read_bits(8).unwrap(), 0b00_1101_10);
//! assert_eq!(bitstream.read_bits(38).unwrap(), 0b01_1111_1011_0101_0100_0011_0000_0000_1000_0110);
//! 
//! bitstream.seek_bit(0); // rewind the stream
//! assert_eq!(bitstream.read_bits(8).unwrap(), 0b0111_0110);
//! 
//! bitstream.seek_bit(0); // rewind the stream
//! 
//! assert_eq!(bitstream.read_unary().unwrap(), 1);
//! assert_eq!(bitstream.read_unary().unwrap(), 0);
//! assert_eq!(bitstream.read_unary().unwrap(), 0);
//! assert_eq!(bitstream.read_unary().unwrap(), 1);
//! assert_eq!(bitstream.read_unary().unwrap(), 0);
//! assert_eq!(bitstream.read_unary().unwrap(), 2);
//! assert_eq!(bitstream.read_unary().unwrap(), 0);
//! assert_eq!(bitstream.read_unary().unwrap(), 5);
//! ```

mod bit_stream;
pub use bit_stream::*;

mod word_reader;
pub use word_reader::*;