use common::config::{StreamManagerConfig, load_config_from_file};
use stream_manager::StreamManager;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config = match parse_config_path() {
        Some(path) => load_config_from_file::<StreamManagerConfig>(&path).unwrap_or_else(|e| {
            eprintln!("{e}");
            std::process::exit(1);
        }),
        None => StreamManagerConfig::default(),
    };

    tracing::info!("StreamManager starting with config: {:?}", config);

    let mgr = StreamManager::start(config).await;

    // Wait for Ctrl+C, then gracefully shut down.
    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for ctrl_c");

    mgr.stop().await;
}

fn parse_config_path() -> Option<String> {
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        if args[i] == "--config" || args[i] == "-c" {
            if i + 1 < args.len() {
                return Some(args[i + 1].clone());
            } else {
                eprintln!("--config requires a file path argument");
                std::process::exit(1);
            }
        }
        i += 1;
    }
    None
}
