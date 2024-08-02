//! Generate Rust code from the protobuf files.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional") // hyperium/tonic#1331
        .build_server(true)
        // .compile_well_known_types(true)
        .compile(
            &[
                "src/schema/blob.proto",
                "src/schema/kv.proto",
                "src/schema/google/protobuf/any.proto",
                "src/schema/google/protobuf/wrappers.proto",
            ],
            &["src/schema"],
        )?;
    Ok(())
}
