use std::path::PathBuf;

fn main() {
    let paths: Vec<PathBuf> = vec![
        ["src", "codes", "unary_tables.rs"].iter().collect(),
        ["src", "codes", "gamma_tables.rs"].iter().collect(),
        ["src", "codes", "delta_tables.rs"].iter().collect(),
        ["src", "codes", "zeta_tables.rs"].iter().collect(),
    ];
    // generate the tables if needed
    if paths.iter().any(|path| !path.exists()) {
        std::process::Command::new("python")
            .arg("./code_tables_generator.py")
            .spawn()
            .unwrap();
    }
}