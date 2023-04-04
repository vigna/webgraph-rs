//! This modules contains all the logic to read and write codes. While it's used
//! by webgraph it's not a part of webgraph. In the future we should move this
//! to its own crate, while we decide on the logistic of where to put it, 
//! it will stay here to go on with the developement of the library.
//! 
//! ### Endianess
//! **The bytes should be read in a little-endian byte order and 
//! MSB to LSB bit order**, i.e., MSB is bit number 0 and LSB is bit number 7.
//! 
//! The choiche of little-endianess is due to the fact that the majority of 
//! computers today are little-endian.
//! The bits order was chosen to be able to use instructions like [`LZCNT`](https://en.wikipedia.org/wiki/X86_Bit_manipulation_instruction_set) to compute
//! the number of trailing bits to speed up the reading of unary codes.
//! 
//! #### Example: 
//! The following stream of bits, to be read from left to right, from top to bottom:
//! ```text
//! 01110110 01100000 11110001 11001101 10011111 10110101 01000011 00000000 
//! 10000110 10011011 01110011 11111001 11100110 01100011 00101000 01110000 
//! ```
//! is equivalent to the following stream of bytes:
//! ```text
//! 76 60 f1 cd 9f b5 43 00
//! 86 9b 73 f9 e6 63 28 70
//! ```
//! that is equivalent to the following stream of [`u64`] words:
//! ```text
//! 0043b59fcdf16076
//! 702863e6f9739b86
//! ```
//! In code:
//! ```
//! use webgraph::codes::*;
//! // file data
//! let data: [u8; 16] = [
//!     0x76, 0x60, 0xf1, 0xcd, 0x9f, 0xb5, 0x43, 0x00,
//!     0x86, 0x9b, 0x73, 0xf9, 0xe6, 0x63, 0x28, 0x70,
//! ];
//! // data converted to little-endian u64 words
//! let words: [u64; 2] = [
//!     0x0043b59fcdf16076,
//!     0x702863e6f9739b86,
//! ];
//! 
//! # // check that the converted data are actually right :)
//! # let data_to_words = [
//! #    u64::from_le_bytes(data[0..8].try_into().unwrap()),
//! #    u64::from_le_bytes(data[8..16].try_into().unwrap()),
//! # ];
//! # assert_eq!(words, data_to_words, "Converted data is wrong!");
//! 
//! // create the bitstream
//! let word_reader = MemWordReader::new(&words);
//! let mut bitstream = BufferedBitStreamReader::new(word_reader);
//! 
//! assert_eq!(bitstream.read_bits(8).unwrap(), 0b0111_0110);
//! assert_eq!(bitstream.read_bits(4).unwrap(), 0b0000);
//! assert_eq!(bitstream.read_bits(4).unwrap(), 0b0110);
//! assert_eq!(bitstream.read_bits(10).unwrap(),0b01_1111_0001);
//! assert_eq!(bitstream.read_bits(8).unwrap(),0b1111_0011);
//! ```

mod bit_stream;
pub use bit_stream::BufferedBitStreamReader;

mod word_reader;
pub use word_reader::*;