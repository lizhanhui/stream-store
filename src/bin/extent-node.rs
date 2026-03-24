use common::config::ExtentNodeConfig;
use extent_node::ExtentNode;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config = ExtentNodeConfig::default();
    let node = ExtentNode::start(config).await;

    // Wait for Ctrl+C, then gracefully shut down.
    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for ctrl_c");

    node.stop().await;
}
