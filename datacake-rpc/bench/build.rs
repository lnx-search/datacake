fn main() -> anyhow::Result<()> {
    println!("cargo:rerun-if-changed=proto/*.proto");

    tonic_build::configure().compile(&["proto/echo.proto"], &["proto"])?;

    Ok(())
}
