#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use webgraph::prelude::*;

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
    let mut idx = 0;
    let mut reader = MemWordRead::new(&data.init);
    for command in data.commands {
        match command {
            RandomCommand::Len => {
                assert_eq!(reader.len(), data.init.len());
            },
            RandomCommand::GetPosition => {
                assert_eq!(reader.get_position(), idx);
            },
            RandomCommand::SetPosition(word_index) => {
                let _ = reader.set_position(word_index);
                if data.init.get(word_index).is_some() {
                    idx = word_index;
                }
            },
            RandomCommand::ReadNextWord => {
                assert_eq!(reader.read_next_word().ok(), data.init.get(idx).copied());
                if data.init.get(idx).is_some() {
                    idx += 1;
                }
            },
        };
    }
});
