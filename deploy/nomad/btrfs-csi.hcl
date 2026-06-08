job "btrfs-csi" {
  datacenters = ["dc1"]
  type = "system"

  group "csi" {
    network {
      port "transport" {
        static = 9200
      }
      port "csi" {
        static = 9201
      }
    }

    # Volume for configuration
    volume "config" {
      type = "host"
      source = "btrfs-csi-config"
      read_only = true
    }

    # Volume for data
    volume "data" {
      type = "host"
      source = "btrfs-data"
      read_only = false
    }

    # Volume for snapshots
    volume "snapshots" {
      type = "host"
      source = "btrfs-snapshots"
      read_only = false
    }

    # CSI socket volume
    volume "csi-socket" {
      type = "host"
      source = "csi-socket"
      read_only = false
    }

    task "btrfs-csi" {
      driver = "exec"

      config {
        command = "/usr/local/bin/btrfs-csi"
        args = [
          "--config", "/etc/btrfs-csi/config.toml",
          "--endpoint", "0.0.0.0:9201",
        ]
      }

      volume_mount {
        volume = "config"
        destination = "/etc/btrfs-csi"
        read_only = true
      }

      volume_mount {
        volume = "data"
        destination = "/mnt/data"
        read_only = false
      }

      volume_mount {
        volume = "snapshots"
        destination = "/mnt/snapshots"
        read_only = false
      }

      volume_mount {
        volume = "csi-socket"
        destination = "/csi"
        read_only = false
      }

      service {
        name = "btrfs-csi"
        port = "csi"

        check {
          type = "tcp"
          port = "csi"
          interval = "10s"
          timeout = "2s"
        }
      }

      resources {
        cpu    = 100
        memory = 128
      }
    }
  }
}
