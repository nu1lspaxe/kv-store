fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &["proto/kvstore.proto", "proto/raft_service.proto"],
            &["proto"],
        )?;
    Ok(())
}