fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/*.proto");

    tonic_build::configure()
        .out_dir("src/rpc")
        .compile(&["proto/datacake.proto"], &["proto"])?;

    Ok(())
}
