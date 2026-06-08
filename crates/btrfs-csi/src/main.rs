use anyhow::Result;
use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;

use btrfs_csi::driver::BtrfsCsiDriver;
use btrfs_csi::server::CsiServer;
use btrfs_exchange::config::ExchangeConfig;

/// Btrfs CSI Driver with replication support
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Configuration file path
    #[arg(short, long, default_value = "/etc/btrfs-csi/config.toml")]
    config: String,

    /// Listen address for CSI socket
    #[arg(short, long, default_value = "unix:///csi/csi.sock")]
    endpoint: String,

    /// Node ID (overrides config)
    #[arg(long)]
    node_id: Option<String>,

    /// Zone (overrides config)
    #[arg(long)]
    zone: Option<String>,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            EnvFilter::new(&format!("btrfs_csi={}", args.log_level))
        }))
        .init();

    info!("Starting Btrfs CSI Driver");

    // Load configuration
    let mut config = ExchangeConfig::load(&args.config).unwrap_or_else(|e| {
        info!("Using default configuration: {}", e);
        ExchangeConfig::default()
    });

    // Override with command line arguments
    if let Some(node_id) = args.node_id {
        config.node_id = node_id;
    }
    if let Some(zone) = args.zone {
        config.zone = zone;
    }

    // Create and start driver
    let driver = BtrfsCsiDriver::new(config).await?;
    driver.start().await?;

    // Create and start CSI server
    let server = CsiServer::new(args.endpoint.clone(), driver);
    server.serve().await?;

    Ok(())
}
