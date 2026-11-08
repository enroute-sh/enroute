//! Generate the ledger's own protocol types (its log records and the `Peer`
//! service between ledger nodes) from `proto/`, against enroute-shared's
//! protocol: its types are enroute-shared's, not generated again here.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto");
    println!("cargo:rerun-if-changed=../shared/proto");
    let fds = protox::compile(["peer.proto", "log.proto"], ["proto", "../shared/proto"])?;
    tonic_prost_build::configure()
        .bytes(".")
        // The contract's types, which the log records carry.
        .extern_path(".enroute.internal.v1", "::enroute_shared::proto")
        .compile_fds(fds)?;
    Ok(())
}
