//! Generates the wire types from `proto/`.
//!
//! `protox` compiles the `.proto` in this process rather than shelling out to
//! `protoc`, so a checkout builds with nothing installed but a Rust
//! toolchain. `buf` still owns whether the contract is *valid* — see
//! `buf.yaml` and `cargo xtask lint` — but it is not in the build path, so a
//! contributor who has not installed it can still compile.

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // `env!` bakes in the dir this file was compiled in, which Bazel's
    // per-action sandbox makes stale by run time. `var` asks at run time.
    let root = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?).join("../../..");
    let proto_root = root.join("proto");
    let files = [
        proto_root.join("enroute/common/v1alpha1/common.proto"),
        proto_root.join("enroute/api/v1alpha1/repository.proto"),
        proto_root.join("enroute/api/v1alpha1/ref.proto"),
        proto_root.join("enroute/api/v1alpha1/object.proto"),
        proto_root.join("enroute/api/v1alpha1/sync.proto"),
        proto_root.join("enroute/hook/v1alpha1/hook.proto"),
    ];

    // Cargo reruns a build script on any change under a watched directory, so
    // watching the root covers files this crate does not yet name.
    println!("cargo:rerun-if-changed={}", proto_root.display());

    let descriptors = protox::compile(&files, [&proto_root])?;

    // The same descriptors, encoded, for the server to serve over gRPC
    // reflection — so what it answers with and what the handlers were
    // generated from cannot be two different things. Encoded by hand, because
    // `file_descriptor_set_path` only names what `protoc` writes.
    let out = PathBuf::from(std::env::var("OUT_DIR")?);
    std::fs::write(
        out.join("enroute_descriptor.bin"),
        protox::prost::Message::encode_to_vec(&descriptors),
    )?;

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        // Pack chunks are the bulk of everything this contract carries, and
        // the default `Vec<u8>` would copy each one on the way in and out.
        // `Bytes` is refcounted, so a chunk read from storage reaches the
        // socket without being copied at all.
        .bytes(".")
        .compile_fds(descriptors)?;

    Ok(())
}
