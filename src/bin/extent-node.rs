use common::config::{ExtentNodeConfig, load_config_from_file};
use extent_node::ExtentNode;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config = match parse_config_path() {
        Some(path) => load_config_from_file::<ExtentNodeConfig>(&path).unwrap_or_else(|e| {
            eprintln!("{e}");
            std::process::exit(1);
        }),
        None => ExtentNodeConfig::default(),
    };

    tracing::info!("ExtentNode starting with config: {:?}", config);

    // Build runtime with core-pinned worker threads (if configured).
    let rt = extent_node::build_runtime(&config);

    rt.block_on(async {
        let node = ExtentNode::start(config).await;

        // Wait for Ctrl+C, then gracefully shut down.
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for ctrl_c");

        node.stop().await;
    });
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
