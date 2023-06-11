extern crate bindgen;

use cc;
use std::env;
use std::path::PathBuf;

// Handles compilation and bindings generation

fn main() {
	// Compile into a library
	cc::Build::new()
        .opt_level(3)
        .file("c/mph.c")
        .file("c/spooky.c")
        .compile("libmph.a");

	/* 
	cc::Build::new()
        .cpp(true)
        .flag("-std=c++11")
        .flag_if_supported("-march=native")
        .include("/opt/local/include")
        .opt_level(3)
        .file("block.cpp")
        .compile("libblock.a");
	*/

    // Tell cargo to look for shared libraries in the root of the project
    println!("cargo:rustc-link-search=native=.");

    // Tell cargo to tell rustc to link libcdflib.a
    println!("cargo:rustc-link-lib=static=mph");

    // Tell cargo to invalidate the built crate whenever header changes
    println!("cargo:rerun-if-changed=c/mph.h");

    // Tell cargo to invalidate the built crate whenever the code changes
    println!("cargo:rerun-if-changed=c/mph.c");

    // The bindgen::Builder is the main entry point
    // to bindgen, and lets you build up options for
    // the resulting bindings.
    let bindings = bindgen::Builder::default()
        // The input header we would like to generate
        // bindings for.
        .header("c/mph.h")
        // Tell cargo to invalidate the built crate whenever any of the
        // included header files changed.
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
        // Finish the builder and generate the bindings.
        .generate()
        // Unwrap the Result and panic on failure.
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
