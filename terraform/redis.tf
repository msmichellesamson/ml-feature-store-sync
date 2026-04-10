# Redis Cache for Feature Store
resource "google_redis_instance" "feature_cache" {
  name               = "feature-store-cache"
  memory_size_gb     = 1
  region             = var.region
  redis_version      = "REDIS_6_X"
  display_name       = "Feature Store Cache"
  tier               = "BASIC"
  
  auth_enabled = true
  
  labels = {
    environment = var.environment
    service     = "feature-store"
  }
}

# Kafka for streaming features
resource "google_pubsub_topic" "feature_events" {
  name = "feature-events"
  
  labels = {
    environment = var.environment
    service     = "feature-store"
  }
}

resource "google_pubsub_subscription" "feature_sync" {
  name  = "feature-sync-sub"
  topic = google_pubsub_topic.feature_events.name
  
  ack_deadline_seconds = 600
  
  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
  
  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.feature_events_dlq.id
    max_delivery_attempts = 5
  }
}

resource "google_pubsub_topic" "feature_events_dlq" {
  name = "feature-events-dlq"
}

# Output connection strings
output "redis_host" {
  value = google_redis_instance.feature_cache.host
  sensitive = true
}

output "pubsub_topic" {
  value = google_pubsub_topic.feature_events.name
}