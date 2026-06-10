use std::path::Path;

/// Set I/O priority of the current process to IDLE (background-only I/O).
/// On Linux uses ioprio_set syscall. On other platforms this is a no-op.
pub fn set_io_priority_idle() -> std::result::Result<(), String> {
    #[cfg(target_os = "linux")]
    {
        const IOPRIO_CLASS_IDLE: u64 = 3;
        const IOPRIO_WHO_PROCESS: u64 = 1;

        unsafe {
            let ioprio: u64 = IOPRIO_CLASS_IDLE << 13;
            let ret = libc::syscall(libc::SYS_ioprio_set, IOPRIO_WHO_PROCESS, 0i32, ioprio);
            if ret < 0 {
                let err = std::io::Error::last_os_error();
                return Err(format!("ioprio_set IDLE failed: {}", err));
            }
        }
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        Ok(())
    }
}

/// Check if /proc/loadavg indicates the system is busy.
/// Returns true if the 1-minute load average exceeds the given threshold.
pub fn is_system_busy(threshold: f64) -> bool {
    let content = match std::fs::read_to_string("/proc/loadavg") {
        Ok(c) => c,
        Err(_) => return false, // Cannot check, assume not busy
    };
    if let Some(load_str) = content.split_whitespace().next() {
        if let Ok(load) = load_str.parse::<f64>() {
            return load > threshold;
        }
    }
    false
}

/// Check disk I/O utilization. Returns true if disk is busy above threshold.
pub fn is_disk_busy(mount_point: &str, busy_threshold: f64) -> bool {
    // Read /sys/block/*/stat for I/O utilization
    // Try to find the block device for the given mount point
    let mounts = match std::fs::read_to_string("/proc/mounts") {
        Ok(m) => m,
        Err(_) => return false,
    };

    let device_name = mounts.lines()
        .find(|line| line.contains(mount_point))
        .and_then(|line| line.split_whitespace().next())
        .and_then(|dev| {
            let path = Path::new(dev);
            path.file_name()
        })
        .map(|n| n.to_string_lossy().to_string());

    let dev = match device_name {
        Some(d) => d,
        None => return false,
    };

    let stat_path = format!("/sys/block/{}/stat", dev.trim_start_matches("dm-"));
    let stat_content = match std::fs::read_to_string(&stat_path) {
        Ok(c) => c,
        Err(_) => {
            // Handle device mapper: check if it's a dm device
            let real_path = format!("/sys/block/dm-{}/stat", dev.trim_start_matches("dm-"));
            match std::fs::read_to_string(&real_path) {
                Ok(c) => c,
                Err(_) => return false,
            }
        }
    };

    // /sys/block/*/stat format: field 10 is I/O time (milliseconds) spent doing work
    if let Some(field_10) = stat_content.split_whitespace().nth(9) {
        if let Ok(io_time_ms) = field_10.parse::<u64>() {
            // Consider busy if I/O time > threshold (in ms) from last check
            // Simple heuristic: if io_time_ms > 0 and we've been running > 1 second, check ratio
            return io_time_ms as f64 > busy_threshold;
        }
    }
    false
}

/// Return the current CPU load average (1-minute).
pub fn get_load_average() -> f64 {
    let content = match std::fs::read_to_string("/proc/loadavg") {
        Ok(c) => c,
        Err(_) => return 0.0,
    };
    content.split_whitespace()
        .next()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0)
}