pub mod csi_controller;
pub mod csi_identity;
pub mod csi_node;
pub mod csi_server;

pub mod csi {
    tonic::include_proto!("csi");
}
