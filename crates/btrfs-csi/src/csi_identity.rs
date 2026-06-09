use tonic::{Request, Response, Status};

use crate::csi::identity_server::Identity;
use crate::csi::get_plugin_capabilities_response as gpcr;
use crate::csi::{
    GetPluginCapabilitiesRequest, GetPluginCapabilitiesResponse, GetPluginInfoRequest,
    GetPluginInfoResponse, ProbeRequest, ProbeResponse,
};

#[derive(Clone)]
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

        let mut manifest = std::collections::HashMap::new();
        manifest.insert("node_id".to_string(), self.node_id.clone());
        manifest.insert("driver".to_string(), "btrfs".to_string());

        let response = GetPluginInfoResponse {
            name: "btrfs-csi".to_string(),
            vendor_version: env!("CARGO_PKG_VERSION").to_string(),
            manifest,
        };

        Ok(Response::new(response))
    }

    async fn get_plugin_capabilities(
        &self,
        _request: Request<GetPluginCapabilitiesRequest>,
    ) -> Result<Response<GetPluginCapabilitiesResponse>, Status> {
        tracing::info!("CSI GetPluginCapabilities called");

        let response = GetPluginCapabilitiesResponse {
            capabilities: vec![
                gpcr::Capability {
                    r#type: Some(gpcr::capability::Type::Service(gpcr::Service {
                        r#type: gpcr::service::Type::ControllerService.into(),
                    })),
                },
                gpcr::Capability {
                    r#type: Some(gpcr::capability::Type::Service(gpcr::Service {
                        r#type: gpcr::service::Type::VolumeAccessibilityConstraints.into(),
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
