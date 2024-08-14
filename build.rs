//! Generate Rust code from the protobuf files.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional") // hyperium/tonic#1331
        .build_server(true)
        .compile(
            &[
                "proto/blob.proto",
                "proto/kv.proto",
                "proto/query.proto",
                "proto/google/protobuf/any.proto",
                "proto/google/protobuf/wrappers.proto",
            ],
            &["proto"],
        )?;
    println!("cargo::rustc-check-cfg=cfg(feature, values(\"rocksdb\"))");
    if cfg!(feature = "vendored-rocksdb") {
        println!("cargo::rustc-cfg=feature=\"rocksdb\"");
    }
    Ok(())
}
