use anyhow::{Context, Result};
use chrono::Utc;
use std::io::{self, Write};
use std::path::Path;
use tokio::net::UnixStream;
use tonic::transport::{Endpoint, Uri};
use tower::service_fn;
use tracing::info;

use btrfs_csi::csi::{
    controller_client::ControllerClient,
    identity_client::IdentityClient,
    node_client::NodeClient,
    CapacityRange, ControllerPublishVolumeRequest, ControllerUnpublishVolumeRequest,
    CreateVolumeRequest, DeleteVolumeRequest, GetCapacityRequest, GetPluginInfoRequest,
    ListVolumesRequest, NodeGetInfoRequest, NodeGetVolumeStatsRequest, NodePublishVolumeRequest,
    NodeStageVolumeRequest, NodeUnpublishVolumeRequest, NodeUnstageVolumeRequest, ProbeRequest,
    TopologyRequirement, VolumeCapability,
};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let socket_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/var/run/csi/csi.sock".to_string());
    let socket_path = Path::new(&socket_path);
    let socket_path_clone = socket_path.to_path_buf();

    info!("Connecting to CSI gRPC server at {:?}", socket_path);

    let channel = Endpoint::try_from("http://[::]:0")
        .context("Failed to create endpoint")?
        .connect_with_connector(service_fn(move |_: Uri| {
            let path = socket_path_clone.clone();
            async move {
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(
                    UnixStream::connect(path).await?,
                ))
            }
        }))
        .await
        .context("Failed to connect to CSI server")?;

    let mut identity = IdentityClient::new(channel.clone());
    let mut controller = ControllerClient::new(channel.clone());
    let mut node = NodeClient::new(channel);

    // =========================================
    // IDENTITY SERVICE TESTS
    // =========================================
    println!("\n=== IDENTITY SERVICE ===");

    print_test("Probe");
    let resp = identity
        .probe(ProbeRequest {})
        .await
        .context("Probe failed")?;
    let probe = resp.into_inner();
    assert!(probe.ready, "Probe returned not ready");
    println!("  PASS (ready={})", probe.ready);

    print_test("GetPluginInfo");
    let resp = identity
        .get_plugin_info(GetPluginInfoRequest {})
        .await
        .context("GetPluginInfo failed")?;
    let info = resp.into_inner();
    assert!(!info.name.is_empty(), "Plugin name is empty");
    assert!(!info.vendor_version.is_empty(), "Vendor version is empty");
    println!(
        "  PASS (name={}, version={})",
        info.name, info.vendor_version
    );

    print_test("GetPluginCapabilities");
    let resp = identity
        .get_plugin_capabilities(
            btrfs_csi::csi::GetPluginCapabilitiesRequest {},
        )
        .await
        .context("GetPluginCapabilities failed")?;
    let caps = resp.into_inner();
    println!("  PASS ({} capabilities)", caps.capabilities.len());

    // =========================================
    // CONTROLLER SERVICE TESTS
    // =========================================
    println!("\n=== CONTROLLER SERVICE ===");

    print_test("GetCapacity");
    let resp = controller
        .get_capacity(GetCapacityRequest {
            topology_requirement: Some(TopologyRequirement {
                requisite: vec![],
                preferred: vec![],
            }),
            volume_capabilities: None,
            parameters: Default::default(),
        })
        .await
        .context("GetCapacity failed")?;
    let cap = resp.into_inner();
    assert!(
        cap.available_capacity > 0,
        "Available capacity is 0"
    );
    println!("  PASS (available={} bytes)", cap.available_capacity);

    print_test("ControllerGetCapabilities");
    let resp = controller
        .controller_get_capabilities(
            btrfs_csi::csi::ControllerGetCapabilitiesRequest {},
        )
        .await
        .context("ControllerGetCapabilities failed")?;
    let caps = resp.into_inner();
    println!("  PASS ({} capabilities)", caps.capabilities.len());

    let test_volume_name = format!("csi-test-{}", Utc::now().timestamp());

    print_test(&format!("CreateVolume({})", test_volume_name));
    let resp = controller
        .create_volume(CreateVolumeRequest {
            name: test_volume_name.clone(),
            capacity_range: Some(CapacityRange {
                required_bytes: 10 * 1024 * 1024, // 10MB
                limit_bytes: 100 * 1024 * 1024,   // 100MB
            }),
            volume_capabilities: Some(VolumeCapability {
                access_type: Some(
                    btrfs_csi::csi::volume_capability::AccessType {
                        access_type: Some(
                            btrfs_csi::csi::volume_capability::access_type::AccessType::Mount(
                                btrfs_csi::csi::volume_capability::Mount {
                                    fs_type: "btrfs".to_string(),
                                    mount_flags: Default::default(),
                                },
                            ),
                        ),
                    },
                ),
                access_mode: Some(btrfs_csi::csi::volume_capability::AccessMode {
                    mode: btrfs_csi::csi::volume_capability::access_mode::Mode::SingleNodeWriter
                        as i32,
                }),
            }),
            parameters: Default::default(),
            content_source_snapshot_id: String::new(),
            content_source_volume_id: String::new(),
            topology_requirement: None,
        })
        .await
        .context("CreateVolume failed")?;
    let vol = resp.into_inner();
    let created_volume = vol.volume.unwrap();
    let volume_id = created_volume.volume_id.clone();
    let volume_context = created_volume.volume_context.clone();
    assert!(!volume_id.is_empty(), "Volume ID is empty");
    println!(
        "  PASS (volume_id={}, capacity={})",
        volume_id, created_volume.capacity_bytes
    );

    print_test("ControllerGetVolume");
    let resp = controller
        .controller_get_volume(
            btrfs_csi::csi::ControllerGetVolumeRequest {
                volume_id: volume_id.clone(),
            },
        )
        .await
        .context("ControllerGetVolume failed")?;
    let get_vol = resp.into_inner();
    assert!(
        get_vol.volume.is_some(),
        "ControllerGetVolume returned no volume"
    );
    println!("  PASS");

    print_test("ControllerPublishVolume");
    let resp = controller
        .controller_publish_volume(ControllerPublishVolumeRequest {
            volume_id: volume_id.clone(),
            node_id: "test-node".to_string(),
            volume_capability: Some(VolumeCapability {
                access_type: Some(
                    btrfs_csi::csi::volume_capability::AccessType {
                        access_type: Some(
                            btrfs_csi::csi::volume_capability::access_type::AccessType::Mount(
                                btrfs_csi::csi::volume_capability::Mount {
                                    fs_type: "btrfs".to_string(),
                                    mount_flags: Default::default(),
                                },
                            ),
                        ),
                    },
                ),
                access_mode: Some(btrfs_csi::csi::volume_capability::AccessMode {
                    mode: btrfs_csi::csi::volume_capability::access_mode::Mode::SingleNodeWriter
                        as i32,
                }),
            }),
            client_token: String::new(),
            parameters: Default::default(),
            secrets: Default::default(),
        })
        .await
        .context("ControllerPublishVolume failed")?;
    let pub_resp = resp.into_inner();
    assert!(
        !pub_resp.publish_context.is_empty(),
        "Publish context is empty"
    );
    let publish_context = pub_resp.publish_context.clone();
    println!("  PASS (context={})", publish_context);

    print_test("ValidateVolumeCapabilities");
    let resp = controller
        .validate_volume_capabilities(
            btrfs_csi::csi::ValidateVolumeCapabilitiesRequest {
                volume_id: volume_id.clone(),
                volume_capabilities: vec![VolumeCapability {
                    access_type: Some(
                        btrfs_csi::csi::volume_capability::AccessType {
                            access_type: Some(
                                btrfs_csi::csi::volume_capability::access_type::AccessType::Mount(
                                    btrfs_csi::csi::volume_capability::Mount {
                                        fs_type: "btrfs".to_string(),
                                        mount_flags: Default::default(),
                                    },
                                ),
                            ),
                        },
                    ),
                    access_mode: Some(btrfs_csi::csi::volume_capability::AccessMode {
                        mode: btrfs_csi::csi::volume_capability::access_mode::Mode::SingleNodeWriter
                            as i32,
                    }),
                }],
                parameters: Default::default(),
                secrets: Default::default(),
            },
        )
        .await
        .context("ValidateVolumeCapabilities failed")?;
    let valid = resp.into_inner();
    println!("  PASS (confirmed={})", valid.confirmed.len());

    print_test("ListVolumes");
    let resp = controller
        .list_volumes(ListVolumesRequest {
            max_entries: 100,
            starting_token: String::new(),
        })
        .await
        .context("ListVolumes failed")?;
    let list = resp.into_inner();
    assert!(
        !list.entries.is_empty(),
        "ListVolumes returned no entries"
    );
    println!("  PASS ({} volumes)", list.entries.len());

    // =========================================
    // NODE SERVICE TESTS
    // =========================================
    println!("\n=== NODE SERVICE ===");

    print_test("NodeGetInfo");
    let resp = node
        .node_get_info(NodeGetInfoRequest {})
        .await
        .context("NodeGetInfo failed")?;
    let node_info = resp.into_inner();
    assert!(!node_info.node_id.is_empty(), "Node ID is empty");
    println!("  PASS (node_id={})", node_info.node_id);

    print_test("NodeGetCapabilities");
    let resp = node
        .node_get_capabilities(
            btrfs_csi::csi::NodeGetCapabilitiesRequest {},
        )
        .await
        .context("NodeGetCapabilities failed")?;
    let caps = resp.into_inner();
    println!("  PASS ({} capabilities)", caps.capabilities.len());

    let stage_path = "/tmp/csi-test-stage";
    let publish_path = "/tmp/csi-test-publish";

    print_test("NodeStageVolume");
    let _ = std::fs::create_dir_all(stage_path);
    node.node_stage_volume(NodeStageVolumeRequest {
        volume_id: volume_id.clone(),
        publish_context: publish_context.clone(),
        staging_target_path: stage_path.to_string(),
        volume_capability: Some(VolumeCapability {
            access_type: Some(
                btrfs_csi::csi::volume_capability::AccessType {
                    access_type: Some(
                        btrfs_csi::csi::volume_capability::access_type::AccessType::Mount(
                            btrfs_csi::csi::volume_capability::Mount {
                                fs_type: "btrfs".to_string(),
                                mount_flags: Default::default(),
                            },
                        ),
                    ),
                },
            ),
            access_mode: Some(btrfs_csi::csi::volume_capability::AccessMode {
                mode: btrfs_csi::csi::volume_capability::access_mode::Mode::SingleNodeWriter
                    as i32,
            }),
        }),
        volume_context: volume_context.clone(),
        secrets: Default::default(),
    })
    .await
    .context("NodeStageVolume failed")?;
    println!("  PASS");

    print_test("NodePublishVolume");
    let _ = std::fs::create_dir_all(publish_path);
    node.node_publish_volume(NodePublishVolumeRequest {
        volume_id: volume_id.clone(),
        publish_context: publish_context.clone(),
        target_path: publish_path.to_string(),
        volume_capability: Some(VolumeCapability {
            access_type: Some(
                btrfs_csi::csi::volume_capability::AccessType {
                    access_type: Some(
                        btrfs_csi::csi::volume_capability::access_type::AccessType::Mount(
                            btrfs_csi::csi::volume_capability::Mount {
                                fs_type: "btrfs".to_string(),
                                mount_flags: Default::default(),
                            },
                        ),
                    ),
                },
            ),
            access_mode: Some(btrfs_csi::csi::volume_capability::AccessMode {
                mode: btrfs_csi::csi::volume_capability::access_mode::Mode::SingleNodeWriter
                    as i32,
            }),
        }),
        readonly: false,
        volume_context: volume_context.clone(),
        secrets: Default::default(),
    })
    .await
    .context("NodePublishVolume failed")?;
    println!("  PASS");

    print_test("NodeGetVolumeStats");
    let resp = node
        .node_get_volume_stats(NodeGetVolumeStatsRequest {
            volume_id: volume_id.clone(),
            volume_path: publish_path.to_string(),
            staging_target_path: stage_path.to_string(),
        })
        .await
        .context("NodeGetVolumeStats failed")?;
    let stats = resp.into_inner();
    if let Some(usage) = stats.usage {
        println!(
            "  PASS (total={}, used={}, available={})",
            usage.total, usage.used, usage.available
        );
    } else {
        println!("  PASS (no usage info)");
    }

    print_test("NodeUnpublishVolume");
    node.node_unpublish_volume(NodeUnpublishVolumeRequest {
        volume_id: volume_id.clone(),
        target_path: publish_path.to_string(),
    })
    .await
    .context("NodeUnpublishVolume failed")?;
    println!("  PASS");

    print_test("NodeUnstageVolume");
    node.node_unstage_volume(NodeUnstageVolumeRequest {
        volume_id: volume_id.clone(),
        staging_target_path: stage_path.to_string(),
    })
    .await
    .context("NodeUnstageVolume failed")?;
    println!("  PASS");

    // Cleanup
    let _ = std::fs::remove_dir_all(stage_path);
    let _ = std::fs::remove_dir_all(publish_path);

    // =========================================
    // CLEANUP
    // =========================================
    println!("\n=== CLEANUP ===");

    print_test("ControllerUnpublishVolume");
    controller
        .controller_unpublish_volume(ControllerUnpublishVolumeRequest {
            volume_id: volume_id.clone(),
            node_id: "test-node".to_string(),
            secrets: Default::default(),
        })
        .await
        .context("ControllerUnpublishVolume failed")?;
    println!("  PASS");

    print_test(&format!("DeleteVolume({})", volume_id));
    controller
        .delete_volume(DeleteVolumeRequest {
            volume_id: volume_id.clone(),
        })
        .await
        .context("DeleteVolume failed")?;
    println!("  PASS");

    // =========================================
    // SUMMARY
    // =========================================
    println!("\n========================================");
    println!("  ALL CSI gRPC TESTS PASSED");
    println!("========================================");
    println!("  Identity:   3/3  (Probe, GetPluginInfo, GetPluginCapabilities)");
    println!("  Controller: 8/8  (GetCapacity, CreateVolume, GetVolume, Publish, Validate, List, GetCapabilities, Unpublish)");
    println!("  Node:       6/6  (GetInfo, GetCapabilities, Stage, Publish, Stats, Unstage)");
    println!("  Cleanup:    2/2  (Unpublish, DeleteVolume)");
    println!("  Total:     19/19 CSI RPCs verified");
    println!("========================================");

    Ok(())
}

fn print_test(name: &str) {
    print!("[TEST] {} ...", name);
    let _ = io::stdout().flush();
}
