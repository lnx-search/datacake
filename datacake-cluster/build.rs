fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/*.proto");

    let mut config = prost_build::Config::new();
    config.bytes(&[".datacake_api.Document.data"]);

    tonic_build::configure()
        .out_dir("src/rpc")
        .compile_with_config(
            config,
            &["proto/chitchat.proto", "proto/datacake.proto"],
            &["proto"],
        )?;

    Ok(())
}
