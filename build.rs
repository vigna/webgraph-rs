use std::path::PathBuf;

fn main() {
    let path: PathBuf = ["src", "codes", "unary_tables.rs"].iter().collect();
    // generate the tables if needed
    if !path.exists() {
        std::process::Command::new("python")
            .arg("./code_tables_generator.py")
            .spawn()
            .unwrap();
    }
}