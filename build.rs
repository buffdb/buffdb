fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .compile(
            &[
                "src/schema/blob.proto",
                "src/schema/common.proto",
                "src/schema/kv.proto",
            ],
            &["src/schema"],
        )
        .unwrap_or_else(|e| panic!("Failed to compile protos {e:?}"));
    Ok(())
}
