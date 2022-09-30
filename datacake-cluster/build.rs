fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/cluster_rpc.proto");

    let mut config = prost_build::Config::new();
    config.bytes(&["."]);

    tonic_build::configure()
        .out_dir("src/rpc")
        .compile_with_config(config, &["proto/cluster_rpc.proto"], &["proto"])?;

    Ok(())
}
