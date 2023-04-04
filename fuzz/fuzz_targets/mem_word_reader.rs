#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use webgraph::codes::*;

#[derive(Arbitrary, Debug)]
struct FuzzCase {
    init: Vec<u64>,
    commands: Vec<RandomCommand>
}

#[derive(Arbitrary, Debug)]
enum RandomCommand {
    Len,
    GetPosition,
    SetPosition(usize),
    ReadNextWord,
}

fuzz_target!(|data: FuzzCase| {
    let mut reader = MemWordReader::new(&data.init);
    for command in data.commands {
        match command {
            RandomCommand::Len => {reader.len();},
            RandomCommand::GetPosition => {reader.get_position();},
            RandomCommand::SetPosition(word_index) => {reader.set_position(word_index);},
            RandomCommand::ReadNextWord => {reader.read_next_word();},
        };
    }
});
