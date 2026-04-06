# Prometheus and Grafana monitoring infrastructure
resource "google_compute_instance" "prometheus" {
  name         = "${var.project_name}-prometheus"
  machine_type = "e2-standard-2"
  zone         = var.zone

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2004-lts"
      size  = 20
    }
  }

  network_interface {
    network = google_compute_network.vpc.name
    subnetwork = google_compute_subnetwork.subnet.name
    
    access_config {
      nat_ip = google_compute_address.prometheus_ip.address
    }
  }

  metadata_startup_script = templatefile("${path.module}/scripts/setup-prometheus.sh", {
    redis_ip = google_redis_instance.cache.host
    postgres_ip = google_sql_database_instance.main.private_ip_address
    feature_store_ip = google_compute_instance.feature_store.network_interface[0].network_ip
    kafka_ip = google_compute_instance.kafka.network_interface[0].network_ip
  })

  service_account {
    email  = google_service_account.monitoring.email
    scopes = ["monitoring", "logging-write"]
  }

  tags = ["prometheus", "monitoring"]

  depends_on = [
    google_compute_instance.feature_store,
    google_compute_instance.kafka
  ]
}

resource "google_compute_address" "prometheus_ip" {
  name   = "${var.project_name}-prometheus-ip"
  region = var.region
}

# Grafana instance
resource "google_compute_instance" "grafana" {
  name         = "${var.project_name}-grafana"
  machine_type = "e2-standard-2"
  zone         = var.zone

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2004-lts"
      size  = 20
    }
  }

  network_interface {
    network = google_compute_network.vpc.name
    subnetwork = google_compute_subnetwork.subnet.name
    
    access_config {
      nat_ip = google_compute_address.grafana_ip.address
    }
  }

  metadata_startup_script = templatefile("${path.module}/scripts/setup-grafana.sh", {
    prometheus_ip = google_compute_instance.prometheus.network_interface[0].network_ip
  })

  service_account {
    email  = google_service_account.monitoring.email
    scopes = ["monitoring", "logging-write"]
  }

  tags = ["grafana", "monitoring"]

  depends_on = [google_compute_instance.prometheus]
}

resource "google_compute_address" "grafana_ip" {
  name   = "${var.project_name}-grafana-ip"
  region = var.region
}

# Service account for monitoring
resource "google_service_account" "monitoring" {
  account_id   = "${var.project_name}-monitoring"
  display_name = "Feature Store Monitoring Service Account"
  description  = "Service account for Prometheus and Grafana monitoring"
}

resource "google_project_iam_member" "monitoring_metrics" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.monitoring.email}"
}

resource "google_project_iam_member" "monitoring_logs" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.monitoring.email}"
}

# Alertmanager configuration
resource "google_storage_bucket_object" "alertmanager_config" {
  name   = "alertmanager.yml"
  bucket = google_storage_bucket.config.name
  content = yamlencode({
    global = {
      smtp_smarthost = var.smtp_host
      smtp_from      = var.alert_from_email
    }
    route = {
      group_by        = ["alertname"]
      group_wait      = "10s"
      group_interval  = "10s"
      repeat_interval = "1h"
      receiver        = "web.hook"
    }
    receivers = [
      {
        name = "web.hook"
        email_configs = [
          {
            to      = var.alert_to_email
            subject = "Feature Store Alert: {{ .GroupLabels.alertname }}"
            body    = "{{ range .Alerts }}{{ .Annotations.description }}{{ end }}"
          }
        ]
      }
    ]
  })
}

# Prometheus configuration
resource "google_storage_bucket_object" "prometheus_config" {
  name   = "prometheus.yml"
  bucket = google_storage_bucket.config.name
  content = yamlencode({
    global = {
      scrape_interval     = "15s"
      evaluation_interval = "15s"
    }
    rule_files = [
      "/etc/prometheus/rules/*.yml"
    ]
    alerting = {
      alertmanagers = [
        {
          static_configs = [
            {
              targets = ["localhost:9093"]
            }
          ]
        }
      ]
    }
    scrape_configs = [
      {
        job_name = "prometheus"
        static_configs = [
          {
            targets = ["localhost:9090"]
          }
        ]
      },
      {
        job_name = "feature-store"
        static_configs = [
          {
            targets = ["${google_compute_instance.feature_store.network_interface[0].network_ip}:8000"]
          }
        ]
        metrics_path = "/metrics"
        scrape_interval = "10s"
      },
      {
        job_name = "redis"
        static_configs = [
          {
            targets = ["${google_redis_instance.cache.host}:6379"]
          }
        ]
      },
      {
        job_name = "postgres"
        static_configs = [
          {
            targets = ["${google_sql_database_instance.main.private_ip_address}:5432"]
          }
        ]
      },
      {
        job_name = "kafka"
        static_configs = [
          {
            targets = ["${google_compute_instance.kafka.network_interface[0].network_ip}:9092"]
          }
        ]
      },
      {
        job_name = "node-exporter"
        static_configs = [
          {
            targets = [
              "${google_compute_instance.feature_store.network_interface[0].network_ip}:9100",
              "${google_compute_instance.kafka.network_interface[0].network_ip}:9100"
            ]
          }
        ]
      }
    ]
  })
}

# Alert rules
resource "google_storage_bucket_object" "alert_rules" {
  name   = "alert_rules.yml"
  bucket = google_storage_bucket.config.name
  content = yamlencode({
    groups = [
      {
        name = "feature_store_alerts"
        rules = [
          {
            alert = "FeatureStoreDown"
            expr  = "up{job=\"feature-store\"} == 0"
            for   = "1m"
            labels = {
              severity = "critical"
            }
            annotations = {
              summary     = "Feature Store is down"
              description = "Feature Store has been down for more than 1 minute"
            }
          },
          {
            alert = "HighAPILatency"
            expr  = "histogram_quantile(0.95, rate(http_request_duration_seconds_bucket{job=\"feature-store\"}[5m])) > 0.5"
            for   = "2m"
            labels = {
              severity = "warning"
            }
            annotations = {
              summary     = "High API latency"
              description = "95th percentile latency is above 500ms for 2 minutes"
            }
          },
          {
            alert = "FeatureStoreCacheHitRateLow"
            expr  = "rate(feature_store_cache_hits_total[5m]) / (rate(feature_store_cache_hits_total[5m]) + rate(feature_store_cache_misses_total[5m])) < 0.8"
            for   = "5m"
            labels = {
              severity = "warning"
            }
            annotations = {
              summary     = "Low cache hit rate"
              description = "Cache hit rate is below 80% for 5 minutes"
            }
          },
          {
            alert = "RedisDown"
            expr  = "up{job=\"redis\"} == 0"
            for   = "30s"
            labels = {
              severity = "critical"
            }
            annotations = {
              summary     = "Redis is down"
              description = "Redis instance has been down for more than 30 seconds"
            }
          },
          {
            alert = "PostgreSQLDown"
            expr  = "up{job=\"postgres\"} == 0"
            for   = "30s"
            labels = {
              severity = "critical"
            }
            annotations = {
              summary     = "PostgreSQL is down"
              description = "PostgreSQL instance has been down for more than 30 seconds"
            }
          },
          {
            alert = "KafkaDown"
            expr  = "up{job=\"kafka\"} == 0"
            for   = "1m"
            labels = {
              severity = "critical"
            }
            annotations = {
              summary     = "Kafka is down"
              description = "Kafka instance has been down for more than 1 minute"
            }
          },
          {
            alert = "FeatureSyncLag"
            expr  = "kafka_consumer_lag_sum{topic=\"feature_updates\"} > 1000"
            for   = "2m"
            labels = {
              severity = "warning"
            }
            annotations = {
              summary     = "High feature sync lag"
              description = "Kafka consumer lag is above 1000 messages for 2 minutes"
            }
          },
          {
            alert = "HighMemoryUsage"
            expr  = "(1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)) > 0.85"
            for   = "5m"
            labels = {
              severity = "warning"
            }
            annotations = {
              summary     = "High memory usage"
              description = "Memory usage is above 85% for 5 minutes on {{ $labels.instance }}"
            }
          },
          {
            alert = "HighCPUUsage"
            expr  = "100 - (avg by(instance) (irate(node_cpu_seconds_total{mode=\"idle\"}[5m])) * 100) > 80"
            for   = "10m"
            labels = {
              severity = "warning"
            }
            annotations = {
              summary     = "High CPU usage"
              description = "CPU usage is above 80% for 10 minutes on {{ $labels.instance }}"
            }
          },
          {
            alert = "DiskSpaceRunningOut"
            expr  = "(node_filesystem_avail_bytes{mountpoint=\"/\"} / node_filesystem_size_bytes{mountpoint=\"/\"}) * 100 < 20"
            for   = "5m"
            labels = {
              severity = "critical"
            }
            annotations = {
              summary     = "Disk space running out"
              description = "Disk space is below 20% on {{ $labels.instance }}"
            }
          }
        ]
      }
    ]
  })
}

# Firewall rules for monitoring
resource "google_compute_firewall" "prometheus" {
  name    = "${var.project_name}-prometheus"
  network = google_compute_network.vpc.name

  allow {
    protocol = "tcp"
    ports    = ["9090", "9093"]
  }

  source_ranges = [var.allowed_cidr]
  target_tags   = ["prometheus"]
}

resource "google_compute_firewall" "grafana" {
  name    = "${var.project_name}-grafana"
  network = google_compute_network.vpc.name

  allow {
    protocol = "tcp"
    ports    = ["3000"]
  }

  source_ranges = [var.allowed_cidr]
  target_tags   = ["grafana"]
}

resource "google_compute_firewall" "node_exporter" {
  name    = "${var.project_name}-node-exporter"
  network = google_compute_network.vpc.name

  allow {
    protocol = "tcp"
    ports    = ["9100"]
  }

  source_tags = ["prometheus"]
  target_tags = ["feature-store", "kafka"]
}

# Variables for monitoring configuration
variable "smtp_host" {
  description = "SMTP host for sending alerts"
  type        = string
  default     = "smtp.gmail.com:587"
}

variable "alert_from_email" {
  description = "Email address to send alerts from"
  type        = string
}

variable "alert_to_email" {
  description = "Email address to send alerts to"
  type        = string
}

variable "allowed_cidr" {
  description = "CIDR block allowed to access monitoring dashboards"
  type        = string
  default     = "0.0.0.0/0"
}

# Outputs
output "prometheus_ip" {
  description = "External IP address of Prometheus server"
  value       = google_compute_address.prometheus_ip.address
}

output "grafana_ip" {
  description = "External IP address of Grafana server"
  value       = google_compute_address.grafana_ip.address
}

output "prometheus_url" {
  description = "URL to access Prometheus"
  value       = "http://${google_compute_address.prometheus_ip.address}:9090"
}

output "grafana_url" {
  description = "URL to access Grafana"
  value       = "http://${google_compute_address.grafana_ip.address}:3000"
}