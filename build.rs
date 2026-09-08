fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os("PROTOC").is_none() {
        let protoc = protoc_bin_vendored::protoc_bin_path()?;
        std::env::set_var("PROTOC", protoc);
    }

    println!("cargo:rerun-if-changed=proto");
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(
            &[
                "proto/google/firestore/v1/firestore.proto",
                "proto/google/firestore/emulator/v1/firestore_emulator.proto",
                "proto/google/pubsub/v1/pubsub.proto",
                "proto/google/rpc/status.proto",
                "proto/google/type/latlng.proto",
            ],
            &["proto"],
        )?;
    Ok(())
}
