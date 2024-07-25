fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .compile(
            &["src/kv/kv_schema.proto", "src/blob/blob_schema.proto"],
            &["src/kv", "src/blob"],
        )
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));
    Ok(())
}
