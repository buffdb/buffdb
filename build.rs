fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .compile(
            &[
                "src/store/kv/kv_schema.proto",
                "src/store/stream/stream_schema.proto",
                "src/store/blob/blob_schema.proto",
            ],
            &["src/store/kv", "src/store/stream", "src/store/blob"],
        )
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));
    Ok(())
}
