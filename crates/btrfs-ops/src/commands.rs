use anyhow::{Context, Result};
use std::process::Stdio;
use tokio::process::Command;
use tracing::{debug, error, warn};

/// Wrapper for executing btrfs commands
pub struct BtrfsCommand;

impl BtrfsCommand {
    /// Execute a btrfs command and return stdout
    pub async fn run(cmd: &str, args: &[&str]) -> Result<String> {
        debug!("Running: btrfs {} {}", cmd, args.join(" "));

        let output = Command::new("btrfs")
            .arg(cmd)
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await
            .context("Failed to execute btrfs command")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            error!("btrfs {} failed: {}", cmd, stderr);
            anyhow::bail!("btrfs {} failed: {}", cmd, stderr);
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    /// Execute a btrfs command with stdin
    pub async fn run_with_stdin(cmd: &str, args: &[&str], stdin: &[u8]) -> Result<String> {
        debug!("Running: btrfs {} {} (with stdin)", cmd, args.join(" "));

        let mut child = Command::new("btrfs")
            .arg(cmd)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("Failed to spawn btrfs command")?;

        // Write stdin
        if let Some(mut child_stdin) = child.stdin.take() {
            child_stdin
                .write_all(stdin)
                .await
                .context("Failed to write stdin")?;
        }

        let output = child
            .wait_with_output()
            .await
            .context("Failed to wait for btrfs command")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            error!("btrfs {} failed: {}", cmd, stderr);
            anyhow::bail!("btrfs {} failed: {}", cmd, stderr);
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    /// Execute a btrfs send command and pipe to a writer
    pub async fn send_volume(
        vol_path: &str,
        target: &str,
        port: u16,
        incremental_parent: Option<&str>,
    ) -> Result<()> {
        let mut args = vec!["send", vol_path];

        if let Some(parent) = incremental_parent {
            args.push("-p");
            args.push(parent);
        }

        // Use btrfs send piped to netcat or custom receiver
        // For now, we'll use a simple approach
        debug!("Sending volume {} to {}:{}", vol_path, target, port);

        // TODO: Implement proper TCP send
        // For now, just log the operation
        warn!("btrfs send not yet fully implemented");
        Ok(())
    }

    /// Execute btrfs receive
    pub async fn receive_volume(receive_path: &str) -> Result<()> {
        debug!("Receiving volume to {}", receive_path);

        // TODO: Implement proper TCP receive
        warn!("btrfs receive not yet fully implemented");
        Ok(())
    }
}

use tokio::io::AsyncWriteExt;
