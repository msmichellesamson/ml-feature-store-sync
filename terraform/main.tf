variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for resources"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "GCP zone for resources"
  type        = string
  default     = "us-central1-a"
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "redis_memory_size_gb" {
  description = "Redis memory size in GB"
  type        = number
  default     = 4
}

variable "postgres_tier" {
  description = "CloudSQL instance tier"
  type        = string
  default     = "db-custom-2-8192"
}

variable "kafka_machine_type" {
  description = "Machine type for Kafka instances"
  type        = string
  default     = "e2-standard-4"
}

locals {
  name_prefix = "${var.environment}-ml-feature-store"
  labels = {
    environment = var.environment
    project     = "ml-feature-store-sync"
    managed_by  = "terraform"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

resource "random_password" "postgres_password" {
  length  = 32
  special = true
}

resource "google_project_service" "required_apis" {
  for_each = toset([
    "redis.googleapis.com",
    "sqladmin.googleapis.com",
    "compute.googleapis.com",
    "servicenetworking.googleapis.com",
    "vpcaccess.googleapis.com"
  ])
  
  project = var.project_id
  service = each.value
  
  disable_on_destroy = false
}

resource "google_compute_network" "feature_store_vpc" {
  name                    = "${local.name_prefix}-vpc"
  auto_create_subnetworks = false
  
  depends_on = [google_project_service.required_apis]
}

resource "google_compute_subnetwork" "feature_store_subnet" {
  name          = "${local.name_prefix}-subnet"
  ip_cidr_range = "10.0.0.0/24"
  network       = google_compute_network.feature_store_vpc.id
  region        = var.region
  
  private_ip_google_access = true
}

resource "google_compute_global_address" "private_ip_address" {
  name          = "${local.name_prefix}-private-ip"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 16
  network       = google_compute_network.feature_store_vpc.id
  
  depends_on = [google_project_service.required_apis]
}

resource "google_service_networking_connection" "private_vpc_connection" {
  network                 = google_compute_network.feature_store_vpc.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.private_ip_address.name]
}

resource "google_redis_instance" "feature_cache" {
  name           = "${local.name_prefix}-cache"
  tier           = "STANDARD_HA"
  memory_size_gb = var.redis_memory_size_gb
  region         = var.region
  
  authorized_network = google_compute_network.feature_store_vpc.id
  connect_mode       = "PRIVATE_SERVICE_ACCESS"
  
  redis_version     = "REDIS_6_X"
  display_name      = "Feature Store Cache"
  
  maintenance_policy {
    weekly_maintenance_window {
      day = "SUNDAY"
      start_time {
        hours   = 2
        minutes = 0
        seconds = 0
        nanos   = 0
      }
    }
  }
  
  labels = local.labels
  
  depends_on = [
    google_project_service.required_apis,
    google_service_networking_connection.private_vpc_connection
  ]
}

resource "google_sql_database_instance" "feature_store_db" {
  name             = "${local.name_prefix}-db"
  database_version = "POSTGRES_14"
  region           = var.region
  deletion_protection = false
  
  settings {
    tier                        = var.postgres_tier
    availability_type          = "REGIONAL"
    disk_type                  = "PD_SSD"
    disk_size                  = 100
    disk_autoresize           = true
    disk_autoresize_limit     = 0
    
    backup_configuration {
      enabled                        = true
      start_time                     = "02:00"
      location                       = var.region
      point_in_time_recovery_enabled = true
      transaction_log_retention_days = 7
      
      backup_retention_settings {
        retained_backups = 30
        retention_unit   = "COUNT"
      }
    }
    
    maintenance_window {
      day          = 7
      hour         = 3
      update_track = "stable"
    }
    
    database_flags {
      name  = "log_checkpoints"
      value = "on"
    }
    
    database_flags {
      name  = "log_connections"
      value = "on"
    }
    
    database_flags {
      name  = "log_disconnections"
      value = "on"
    }
    
    database_flags {
      name  = "log_lock_waits"
      value = "on"
    }
    
    database_flags {
      name  = "log_min_duration_statement"
      value = "1000"
    }
    
    ip_configuration {
      ipv4_enabled                                  = false
      private_network                               = google_compute_network.feature_store_vpc.id
      enable_private_path_for_google_cloud_services = true
    }
    
    insights_config {
      query_insights_enabled  = true
      query_plans_per_minute  = 5
      query_string_length     = 1024
      record_application_tags = true
      record_client_address   = true
    }
    
    user_labels = local.labels
  }
  
  depends_on = [
    google_project_service.required_apis,
    google_service_networking_connection.private_vpc_connection
  ]
}

resource "google_sql_database" "feature_store" {
  name     = "feature_store"
  instance = google_sql_database_instance.feature_store_db.name
}

resource "google_sql_database" "feature_metadata" {
  name     = "feature_metadata"
  instance = google_sql_database_instance.feature_store_db.name
}

resource "google_sql_user" "feature_store_user" {
  name     = "feature_store_user"
  instance = google_sql_database_instance.feature_store_db.name
  password = random_password.postgres_password.result
}

resource "google_compute_firewall" "kafka_internal" {
  name    = "${local.name_prefix}-kafka-internal"
  network = google_compute_network.feature_store_vpc.name
  
  allow {
    protocol = "tcp"
    ports    = ["9092", "9093", "2181", "2888", "3888"]
  }
  
  source_ranges = ["10.0.0.0/24"]
  target_tags   = ["kafka-cluster"]
}

resource "google_compute_firewall" "app_internal" {
  name    = "${local.name_prefix}-app-internal"
  network = google_compute_network.feature_store_vpc.name
  
  allow {
    protocol = "tcp"
    ports    = ["8000", "8080"]
  }
  
  source_ranges = ["10.0.0.0/24"]
  target_tags   = ["feature-store-app"]
}

resource "google_compute_instance_template" "kafka_template" {
  name_prefix = "${local.name_prefix}-kafka-"
  
  machine_type = var.kafka_machine_type
  
  disk {
    source_image = "ubuntu-os-cloud/ubuntu-2204-lts"
    auto_delete  = true
    boot         = true
    disk_size_gb = 50
    disk_type    = "pd-ssd"
  }
  
  disk {
    auto_delete  = false
    boot         = false
    disk_size_gb = 100
    disk_type    = "pd-ssd"
    device_name  = "kafka-data"
  }
  
  network_interface {
    network    = google_compute_network.feature_store_vpc.id
    subnetwork = google_compute_subnetwork.feature_store_subnet.id
  }
  
  metadata = {
    startup-script = templatefile("${path.module}/kafka-startup.sh", {
      project_id = var.project_id
    })
  }
  
  service_account {
    scopes = [
      "https://www.googleapis.com/auth/cloud-platform",
      "https://www.googleapis.com/auth/logging.write",
      "https://www.googleapis.com/auth/monitoring.write"
    ]
  }
  
  tags = ["kafka-cluster"]
  
  labels = local.labels
  
  lifecycle {
    create_before_destroy = true
  }
}

resource "google_compute_region_instance_group_manager" "kafka_cluster" {
  name   = "${local.name_prefix}-kafka-cluster"
  region = var.region
  
  base_instance_name = "${local.name_prefix}-kafka"
  target_size        = 3
  
  version {
    instance_template = google_compute_instance_template.kafka_template.id
  }
  
  named_port {
    name = "kafka"
    port = 9092
  }
  
  auto_healing_policies {
    health_check      = google_compute_health_check.kafka_health.id
    initial_delay_sec = 300
  }
}

resource "google_compute_health_check" "kafka_health" {
  name               = "${local.name_prefix}-kafka-health"
  check_interval_sec = 30
  timeout_sec        = 10
  
  tcp_health_check {
    port = "9092"
  }
}

resource "google_secret_manager_secret" "postgres_password" {
  secret_id = "${local.name_prefix}-postgres-password"
  
  labels = local.labels
  
  replication {
    user_managed {
      replicas {
        location = var.region
      }
    }
  }
  
  depends_on = [google_project_service.required_apis]
}

resource "google_secret_manager_secret_version" "postgres_password" {
  secret      = google_secret_manager_secret.postgres_password.id
  secret_data = random_password.postgres_password.result
}

resource "google_secret_manager_secret" "redis_auth" {
  secret_id = "${local.name_prefix}-redis-auth"
  
  labels = local.labels
  
  replication {
    user_managed {
      replicas {
        location = var.region
      }
    }
  }
  
  depends_on = [google_project_service.required_apis]
}

resource "google_secret_manager_secret_version" "redis_auth" {
  secret      = google_secret_manager_secret.redis_auth.id
  secret_data = google_redis_instance.feature_cache.auth_string
}

resource "google_monitoring_notification_channel" "email" {
  display_name = "Feature Store Email Alerts"
  type         = "email"
  
  labels = {
    email_address = "michelle.samson@example.com"
  }
}

resource "google_monitoring_alert_policy" "redis_memory_usage" {
  display_name = "Redis Memory Usage High"
  combiner     = "OR"
  
  conditions {
    display_name = "Redis memory usage > 80%"
    
    condition_threshold {
      filter          = "resource.type=\"redis_instance\" AND resource.label.instance_id=\"${google_redis_instance.feature_cache.name}\""
      duration        = "300s"
      comparison      = "COMPARISON_GREATER_THAN"
      threshold_value = 0.8
      
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MEAN"
      }
    }
  }
  
  notification_channels = [google_monitoring_notification_channel.email.name]
  
  alert_strategy {
    auto_close = "1800s"
  }
}

resource "google_monitoring_alert_policy" "postgres_connection_count" {
  display_name = "PostgreSQL Connection Count High"
  combiner     = "OR"
  
  conditions {
    display_name = "Connection count > 80% of max"
    
    condition_threshold {
      filter          = "resource.type=\"cloudsql_database\" AND resource.label.database_id=\"${var.project_id}:${google_sql_database_instance.feature_store_db.name}\""
      duration        = "300s"
      comparison      = "COMPARISON_GREATER_THAN"
      threshold_value = 80
      
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MEAN"
      }
    }
  }
  
  notification_channels = [google_monitoring_notification_channel.email.name]
  
  alert_strategy {
    auto_close = "1800s"
  }
}

output "redis_host" {
  description = "Redis instance IP address"
  value       = google_redis_instance.feature_cache.host
  sensitive   = true
}

output "redis_port" {
  description = "Redis instance port"
  value       = google_redis_instance.feature_cache.port
}

output "postgres_connection_name" {
  description = "PostgreSQL connection name"
  value       = google_sql_database_instance.feature_store_db.connection_name
  sensitive   = true
}

output "postgres_private_ip" {
  description = "PostgreSQL private IP address"
  value       = google_sql_database_instance.feature_store_db.private_ip_address
  sensitive   = true
}

output "vpc_network" {
  description = "VPC network name"
  value       = google_compute_network.feature_store_vpc.name
}

output "subnet_name" {
  description = "Subnet name"
  value       = google_compute_subnetwork.feature_store_subnet.name
}

output "kafka_instance_group" {
  description = "Kafka instance group manager name"
  value       = google_compute_region_instance_group_manager.kafka_cluster.name
}