use tonic::{Request, Response, Status};

use crate::csi::identity_server::Identity;
use crate::csi::{
    GetPluginCapabilitiesRequest, GetPluginCapabilitiesResponse, GetPluginInfoRequest,
    GetPluginInfoResponse, ProbeRequest, ProbeResponse,
};

pub struct CsiIdentity {
    node_id: String,
}

impl CsiIdentity {
    pub fn new(node_id: String) -> Self {
        Self { node_id }
    }
}

#[tonic::async_trait]
impl Identity for CsiIdentity {
    async fn get_plugin_info(
        &self,
        _request: Request<GetPluginInfoRequest>,
    ) -> Result<Response<GetPluginInfoResponse>, Status> {
        tracing::info!("CSI GetPluginInfo called");

        let response = GetPluginInfoResponse {
            name: "btrfs-csi".to_string(),
            vendor_version: env!("CARGO_PKG_VERSION").to_string(),
            manifest: [
                ("node_id".to_string(), self.node_id.clone()),
                ("driver".to_string(), "btrfs".to_string()),
            ]
            .into_iter()
            .collect(),
        };

        Ok(Response::new(response))
    }

    async fn get_plugin_capabilities(
        &self,
        _request: Request<GetPluginCapabilitiesRequest>,
    ) -> Result<Response<GetPluginCapabilitiesResponse>, Status> {
        tracing::info!("CSI GetPluginCapabilities called");

        use crate::csi::get_plugin_capabilities_response::capability;

        let response = GetPluginCapabilitiesResponse {
            capabilities: vec![
                capability::Capability {
                    r#type: Some(capability::Type::Service(capability::Service {
                        r#type: capability::service::Type::ControllerService as i32,
                    })),
                },
                capability::Capability {
                    r#type: Some(capability::Type::Service(capability::Service {
                        r#type: capability::service::Type::VolumeAccessibilityConstraints as i32,
                    })),
                },
            ],
        };

        Ok(Response::new(response))
    }

    async fn probe(
        &self,
        _request: Request<ProbeRequest>,
    ) -> Result<Response<ProbeResponse>, Status> {
        tracing::debug!("CSI Probe called");

        let ready = std::process::Command::new("btrfs")
            .arg("version")
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false);

        Ok(Response::new(ProbeResponse { ready }))
    }
}
