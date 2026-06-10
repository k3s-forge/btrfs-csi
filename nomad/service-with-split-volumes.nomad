# Nomad job: 单服务 3 个 PVC (data/logs/cache) 不同副本策略
# 适用场景：核心数据 3 副本，日志/缓存 0 副本 + NOCOW
# 要求：CSI driver v0.2.0+, Nomad 1.8+

job "my-service" {
  datacenters = ["dc1"]
  type        = "service"

  # --- 1. 核心数据 PVC (3 副本，压缩，同步写) ---
  volume "svc-data" {
    type            = "csi"
    plugin_id       = "btrfs-csi"
    capacity_min    = "20GiB"
    capacity_max    = "100GiB"
    capability {
      access_mode     = "single-node-writer"
      attachment_mode = "file-system"
    }
    parameters {
      replica_count      = "3"
      replica_zone       = "dc1,dc2"
      sync_interval      = "300"
      profile            = "database"
      require_quorum     = "true"
      readonly_on_minority = "true"
    }
  }

  # --- 2. 日志 PVC (0 副本，NOCOW，不压缩) ---
  volume "svc-logs" {
    type            = "csi"
    plugin_id       = "btrfs-csi"
    capacity_min    = "5GiB"
    capacity_max    = "20GiB"
    capability {
      access_mode     = "single-node-writer"
      attachment_mode = "file-system"
    }
    parameters {
      replica_count      = "0"
      profile            = "log"
      # NOCOW 已由 profile=log 自动启用
    }
  }

  # --- 3. 缓存 PVC (0 副本，NOCOW，可选 tmpfs 覆盖) ---
  volume "svc-cache" {
    type            = "csi"
    plugin_id       = "btrfs-csi"
    capacity_min    = "2GiB"
    capacity_max    = "10GiB"
    capability {
      access_mode     = "single-node-writer"
      attachment_mode = "file-system"
    }
    parameters {
      replica_count = "0"
      profile       = "log"
    }
  }

  group "app" {
    count = 1

    # --- 挂载 3 个 volume 到同一目录树 ---
    volume_mount {
      volume      = "svc-data"
      destination = "/data"
      read_only   = false
    }
    volume_mount {
      volume      = "svc-logs"
      destination = "/data/logs"
      read_only   = false
    }
    volume_mount {
      volume      = "svc-cache"
      destination = "/data/cache"
      read_only   = false
    }

    task "server" {
      driver = "docker"
      config {
        image = "my-app:latest"
        # 应用内部写入：
        # /data/           -> 核心数据 (3 副本)
        # /data/logs/      -> 日志 (0 副本, NOCOW)
        # /data/cache/     -> 缓存 (0 副本, NOCOW)
        mounts = [
          { type = "bind", source = "/data",      target = "/data" },
          { type = "bind", source = "/data/logs", target = "/data/logs" },
          { type = "bind", source = "/data/cache",target = "/data/cache" },
        ]
      }
      resources {
        cpu    = 500
        memory = 1024
      }
    }
  }
}

# --- 部署前检查清单 ---
# 1. 确保 3 个 PVC 对应的 StorageClass 已存在（或用 CSI 直接 provision）
# 2. btrfs 后端池有足够空间：(20+5+2)GiB * replica_count
# 3. 日志/缓存目录在容器启动前自动创建（Docker mount 会自动建目录）
# 4. 如需日志轮转，配置 logrotate 写 /data/logs/，或用 sidecar 收集