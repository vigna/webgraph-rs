use std::path::PathBuf;
use std::env;

const GENERATOR: &str = "code_tables_generator.py";

fn main() {
    let out_dir = env::var_os("OUT_DIR").unwrap();
    // rebuild if the generator has changed
    println!("cargo:rerun-if-changed={GENERATOR}");
    std::process::Command::new("python3")
        .args([GENERATOR, out_dir.to_str().unwrap()])
        .spawn()
        .unwrap();
}
