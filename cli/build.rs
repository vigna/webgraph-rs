use std::fs::OpenOptions;
use std::io::Write;

fn main() {
    built::write_built_file().expect("Failed to acquire build-time information");
    let dst =
        std::path::Path::new(&std::env::var("OUT_DIR").expect("OUT_DIR not set")).join("built.rs");

    let mut file = OpenOptions::new()
        .append(true)
        .open(dst)
        .expect("Could not open file written by built");

    // write the date in DST
    writeln!(
        file,
        "pub const BUILD_DATE: &str = \"{}\";\n",
        chrono::Utc::now().format("%Y-%m-%d")
    )
    .expect("Failed to write build date");
}
