use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;

use btrfs_exchange::config::{ExchangeConfig, VolumeProfile};
use btrfs_exchange::gossip::GossipService;
use btrfs_exchange::replicator::Replicator;

use crate::csi::controller_server::ControllerServer;
use crate::csi::identity_server::IdentityServer;
use crate::csi::node_server::NodeServer;
use crate::csi_controller::CsiController;
use crate::csi_identity::CsiIdentity;
use crate::csi_node::CsiNode;

pub struct CsiGrpcServer {
    endpoint: String,
    identity: CsiIdentity,
    controller: CsiController,
    node: CsiNode,
}

impl CsiGrpcServer {
    pub fn new(
        endpoint: String,
        node_id: String,
        zone: String,
        data_dir: String,
        config: ExchangeConfig,
        gossip: Arc<GossipService>,
        replicator: Arc<Replicator>,
        volume_profiles: HashMap<String, VolumeProfile>,
    ) -> Self {
        Self {
            endpoint,
            identity: CsiIdentity::new(node_id.clone()),
            controller: CsiController::new(node_id.clone(), zone.clone(), data_dir.clone(), config, gossip, replicator, volume_profiles),
            node: CsiNode::new(node_id, zone, data_dir),
        }
    }

    pub fn controller(&self) -> &CsiController {
        &self.controller
    }

    pub fn node(&self) -> &CsiNode {
        &self.node
    }

    pub async fn serve(&self) -> Result<()> {
        let endpoint = self.endpoint.clone();

        if endpoint.starts_with("unix://") {
            let socket_path = endpoint.trim_start_matches("unix://");
            tracing::info!("CSI gRPC server starting on unix://{}", socket_path);

            let _ = tokio::fs::remove_file(socket_path).await;
            if let Some(parent) = std::path::Path::new(socket_path).parent() {
                tokio::fs::create_dir_all(parent).await?;
            }

            let uds = UnixListener::bind(socket_path)?;
            let stream = UnixListenerStream::new(uds);

            Server::builder()
                .add_service(IdentityServer::new(self.identity.clone()))
                .add_service(ControllerServer::new(self.controller.clone()))
                .add_service(NodeServer::new(self.node.clone()))
                .serve_with_incoming(stream)
                .await?;
        } else {
            let addr: std::net::SocketAddr = endpoint.parse()?;
            tracing::info!("CSI gRPC server starting on tcp://{}", addr);

            Server::builder()
                .add_service(IdentityServer::new(self.identity.clone()))
                .add_service(ControllerServer::new(self.controller.clone()))
                .add_service(NodeServer::new(self.node.clone()))
                .serve(addr)
                .await?;
        }

        Ok(())
    }
}
