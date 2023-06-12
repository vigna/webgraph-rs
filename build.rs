extern crate bindgen;

use cc;
use std::env;
use std::path::PathBuf;

// Handles compilation and bindings generation

fn main() {
    let libdir_path = PathBuf::from("c")
        // Canonicalize the path as `rustc-link-search` requires an absolute
        // path.
        .canonicalize()
        .expect("cannot canonicalize path");

    let headers_path = libdir_path.join("mph.h");
    let headers_path_str = headers_path.to_str().expect("Path is not a valid string");
    let code_path = libdir_path.join("mph.c");
    let code_path_str = code_path.to_str().expect("Path is not a valid string");
    let spooky_path = libdir_path.join("spooky.c");
    let spooky_path_str = spooky_path.to_str().expect("Path is not a valid string");

    // Compile into a library
    cc::Build::new()
        .opt_level(3)
        .file(code_path_str)
        .file(spooky_path_str)
        .compile("libmph.a");

    // Tell cargo to look for shared libraries in the root of the project
    println!("cargo:rustc-link-search=native=.");

    // Tell cargo to tell rustc to link libmph.a
    println!("cargo:rustc-link-lib=static=mph");

    // Tell cargo to invalidate the built crate whenever header changes
    println!("cargo:rerun-if-changed={}", headers_path_str);

    // Tell cargo to invalidate the built crate whenever the code changes
    println!("cargo:rerun-if-changed={}", code_path_str);

    // The bindgen::Builder is the main entry point
    // to bindgen, and lets you build up options for
    // the resulting bindings.
    let bindings = bindgen::Builder::default()
        // The input header we would like to generate
        // bindings for.
        .header(headers_path_str)
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
