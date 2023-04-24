use std::path::PathBuf;

const GENERATOR: &str = "code_tables_generator.py";

fn main() {
    let paths: Vec<PathBuf> = vec![
        ["src", "codes", "unary_tables.rs"].iter().collect(),
        ["src", "codes", "gamma_tables.rs"].iter().collect(),
        ["src", "codes", "delta_tables.rs"].iter().collect(),
        ["src", "codes", "zeta_tables.rs"].iter().collect(),
    ];

    // rebuild if the generator has changed
    println!("cargo:rerun-if-changed={GENERATOR}");
    // rebuild if the target files have changed (in fact: vanished)
    paths
        .iter()
        .for_each(|path| println!("cargo:rerun-if-changed={}", path.display()));

    std::process::Command::new("python3")
        .arg(GENERATOR)
        .spawn()
        .unwrap();
}
