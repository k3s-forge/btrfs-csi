use anyhow::Result;
use clap::Parser;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::EnvFilter;

use btrfs_csi::csi_server::CsiGrpcServer;
use btrfs_exchange::config::ExchangeConfig;
use btrfs_exchange::gossip::GossipService;
use btrfs_exchange::replicator::Replicator;

/// Btrfs CSI Driver with replication support
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Configuration file path
    #[arg(short, long, default_value = "/etc/btrfs-csi/config.toml")]
    config: String,

    /// Listen address for CSI gRPC socket
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

    info!("Starting Btrfs CSI Driver v{}", env!("CARGO_PKG_VERSION"));

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

    // Ensure data directories exist
    std::fs::create_dir_all(&config.replication.data_dir)?;
    std::fs::create_dir_all(&config.replication.snapshot_dir)?;
    info!(
        "Data dir: {}, Snapshot dir: {}",
        config.replication.data_dir, config.replication.snapshot_dir
    );

    // Create gossip service
    let gossip = Arc::new(GossipService::new(config.clone()));

    // Start gossip
    gossip.start().await?;

    // Join cluster if seed nodes configured
    if !config.seed_nodes.is_empty() {
        gossip.join_cluster().await?;
    }

    // Create replicator
    let replicator = Arc::new(Replicator::new(config.clone(), gossip.clone()));

    // Start replicator
    replicator.start().await?;

    // Create and start gRPC server
    let server = CsiGrpcServer::new(
        args.endpoint,
        config.node_id.clone(),
        config.zone.clone(),
        config.replication.data_dir.clone(),
        gossip,
        replicator,
    );

    info!(
        "CSI driver ready on node {} (zone={})",
        config.node_id, config.zone
    );

    server.serve().await?;

    Ok(())
}
