//! Architectural boundaries, enforced by reading the source.
//!
//! The module tree is the crate's future crate boundary: if `core` quietly
//! starts depending on the engine, splitting it out later becomes a rewrite.
//! A boundary nothing checks is a comment, so these tests check them.

use std::fs;
use std::path::{Path, PathBuf};

fn rust_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            files.extend(rust_files(&path));
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            files.push(path);
        }
    }
    files
}

/// Non-test source lines of every file under `src/<module>`, with their location.
fn lines_of(module: &str) -> Vec<(String, String)> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join(module);
    rust_files(&dir)
        .into_iter()
        .flat_map(|path| {
            let text = fs::read_to_string(&path).unwrap();
            // Unit tests may reach across layers; the production code may not.
            let production = text.split("#[cfg(test)]").next().unwrap().to_owned();
            let name = path.display().to_string();
            production
                .lines()
                .map(move |line| (name.clone(), line.to_owned()))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn assert_no_imports(module: &str, forbidden: &[&str]) {
    for (file, line) in lines_of(module) {
        let code = line.trim_start();
        if code.starts_with("//") || !(code.starts_with("use ") || code.contains("crate::")) {
            continue;
        }
        for layer in forbidden {
            assert!(
                !code.contains(&format!("crate::{layer}"))
                    && !code.contains(&format!("super::super::{layer}")),
                "{file}: `{module}` must not depend on `{layer}`: {code}"
            );
        }
    }
}

#[test]
fn the_core_layer_depends_on_no_other_layer() {
    assert_no_imports(
        "core",
        &["protocol", "transport", "storage", "adapter", "engine"],
    );
}

#[test]
fn the_protocol_layer_knows_nothing_of_transports_storage_adapters_or_the_engine() {
    assert_no_imports("protocol", &["transport", "storage", "adapter", "engine"]);
}

#[test]
fn the_transport_layer_knows_nothing_of_the_protocol_storage_adapters_or_the_engine() {
    // A transport moves opaque bytes: that is what keeps any mechanism pluggable.
    assert_no_imports("transport", &["protocol", "storage", "adapter", "engine"]);
}

#[test]
fn storage_and_adapters_do_not_depend_on_the_engine_or_each_other() {
    assert_no_imports("storage", &["engine", "adapter", "protocol", "transport"]);
    assert_no_imports("adapter", &["engine", "storage", "protocol", "transport"]);
}

#[test]
fn surrealdb_appears_nowhere_outside_its_own_adapter_module() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    for path in rust_files(&src) {
        if path.ends_with("adapter/surrealdb.rs") {
            continue;
        }
        let text = fs::read_to_string(&path).unwrap();
        for line in text.lines().filter(|l| l.trim_start().starts_with("use ")) {
            assert!(!line.contains("surrealdb"), "{}: {line}", path.display());
        }
    }
}

#[test]
fn the_session_state_machine_performs_no_io_and_reads_no_clock() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/engine/session.rs");
    let text = fs::read_to_string(path).unwrap();
    let production = text.split("#[cfg(test)]").next().unwrap();
    for banned in [
        "async ",
        ".await",
        "std::fs",
        "std::net",
        "std::time",
        "Instant",
        "SystemTime",
        "tokio",
    ] {
        assert!(
            !production.contains(banned),
            "session.rs must stay sans-IO but mentions `{banned}`"
        );
    }
}
