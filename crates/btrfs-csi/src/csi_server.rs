use anyhow::Result;
use std::sync::Arc;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;

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
        gossip: Arc<GossipService>,
        replicator: Arc<Replicator>,
    ) -> Self {
        Self {
            endpoint,
            identity: CsiIdentity::new(node_id.clone()),
            controller: CsiController::new(
                node_id.clone(),
                zone.clone(),
                data_dir.clone(),
                gossip,
                replicator,
            ),
            node: CsiNode::new(node_id, zone, data_dir),
        }
    }

    pub async fn serve(&self) -> Result<()> {
        // Parse endpoint
        let endpoint = self.endpoint.clone();

        if endpoint.starts_with("unix://") {
            // Unix socket
            let socket_path = endpoint.trim_start_matches("unix://");
            tracing::info!("CSI gRPC server starting on unix://{}", socket_path);

            // Remove old socket file if exists
            let _ = tokio::fs::remove_file(socket_path).await;

            // Create parent directory
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
            // TCP fallback
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
