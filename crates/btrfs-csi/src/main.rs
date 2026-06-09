use anyhow::Result;
use clap::Parser;
use std::sync::Arc;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use btrfs_csi::csi_server::CsiGrpcServer;
use btrfs_exchange::config::ExchangeConfig;
use btrfs_exchange::gossip::GossipService;
use btrfs_exchange::receiver::ReplicationReceiver;
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

    /// Auth key (overrides config)
    #[arg(long)]
    auth_key: Option<String>,
}

/// Scan for stale CSI mounts in a directory and attempt cleanup
async fn cleanup_stale_mounts(dir: &str) {
    let mount_dir = std::path::Path::new(dir);
    if !mount_dir.exists() {
        return;
    }

    let entries = match tokio::fs::read_dir(mount_dir).await {
        Ok(mut entries) => {
            let mut all = Vec::new();
            while let Ok(Some(entry)) = entries.next_entry().await {
                all.push(entry);
            }
            all
        }
        Err(_) => return,
    };

    for entry in entries {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }

        let path_str = path.to_string_lossy().to_string();
        // Check if this is a leftover mount point by trying to list it
        if let Ok(output) = tokio::process::Command::new("mountpoint")
            .args(["-q", &path_str])
            .output()
            .await
        {
            if output.status.success() {
                warn!("Found stale mount at {}, attempting cleanup", path_str);
                let _ = tokio::process::Command::new("umount")
                    .arg(&path_str)
                    .output()
                    .await;
            }
        }
    }
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
    if let Some(auth_key) = args.auth_key {
        config.auth_key = auth_key;
    }

    // Security: refuse to start with empty auth key
    if config.auth_key.is_empty() {
        eprintln!("FATAL: auth_key is empty. Inter-node HMAC authentication is disabled.");
        eprintln!("Set auth_key in config.toml or via --auth-key flag.");
        eprintln!("Generate a key: openssl rand -hex 32");
        std::process::exit(1);
    }

    // Ensure data directories exist
    std::fs::create_dir_all(&config.replication.data_dir)?;
    std::fs::create_dir_all(&config.replication.snapshot_dir)?;
    info!(
        "Data dir: {}, Snapshot dir: {}",
        config.replication.data_dir, config.replication.snapshot_dir
    );

    // Crash recovery: clean stale mounts before starting
    cleanup_stale_mounts(&config.replication.data_dir).await;

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

    // Start replication receiver (listens for incoming btrfs send streams)
    let receiver = ReplicationReceiver::new(config.clone());
    tokio::spawn(async move {
        if let Err(e) = receiver.start().await {
            tracing::error!("Replication receiver error: {}", e);
        }
    });

    // Create and start gRPC server
    let server = CsiGrpcServer::new(
        args.endpoint,
        config.node_id.clone(),
        config.zone.clone(),
        config.replication.data_dir.clone(),
        gossip,
        replicator,
        config.replication.volume_profiles.clone(),
    );

    // Load persisted volumes and snapshots from disk
    server.controller().load_from_disk().await;

    info!(
        "CSI driver ready on node {} (zone={})",
        config.node_id, config.zone
    );

    // Run gRPC server and wait for shutdown signal concurrently
    let serve_handle = tokio::spawn({
        let server = server;
        async move {
            if let Err(e) = server.serve().await {
                tracing::error!("gRPC server error: {}", e);
            }
        }
    });

    // Wait for SIGTERM (Linux) or Ctrl+C (cross-platform)
    let shutdown_signal = async {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = signal(SignalKind::terminate())
                .expect("Failed to register SIGTERM handler");
            tokio::select! {
                _ = sigterm.recv() => info!("Received SIGTERM"),
                _ = tokio::signal::ctrl_c() => info!("Received SIGINT"),
            }
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c().await.ok();
            info!("Received Ctrl+C");
        }
    };

    tokio::select! {
        _ = serve_handle => {}
        _ = shutdown_signal => {
            info!("Shutting down gracefully...");
            // Give in-flight RPCs time to complete
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            info!("Shutdown complete");
        }
    }

    Ok(())
}
