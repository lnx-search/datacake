static KEYSPACE_1: &str = "my-first-keyspace";
static KEYSPACE_2: &str = "my-first-keyspace";
static KEYSPACE_3: &str = "my-first-keyspace";

#[tokio::test]
async fn test_multiple_keyspace_single_node() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    // TODO: ADD

    Ok(())
}

#[tokio::test]
async fn test_multiple_keyspace_multi_node() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    // TODO: ADD

    Ok(())
}
