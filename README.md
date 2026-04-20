# ML Feature Store Sync

Production-grade feature store with Redis caching, PostgreSQL persistence, and real-time ML feature pipeline synchronization.

## Architecture

```
Kafka → Transformer → Feature Store (Redis + PostgreSQL)
  ↓
API Layer → ML Models
  ↓
Monitoring & Drift Detection
```

## Key Features

- **Real-time synchronization** between Redis cache and PostgreSQL storage
- **Feature versioning** with automatic schema evolution
- **Drift detection** with configurable thresholds
- **Circuit breaker pattern** for resilient external service calls
- **Comprehensive monitoring** with Prometheus metrics
- **Batch and streaming** feature ingestion
- **Feature lineage tracking** for ML governance

## Infrastructure

- **Kubernetes** deployment with HPA
- **Terraform** for AWS infrastructure
- **Redis Cluster** for high-availability caching
- **PostgreSQL** with connection pooling
- **Kafka** for real-time streaming
- **Prometheus + Grafana** for observability

## API Endpoints

- `GET /health` - Health checks with circuit breaker status
- `POST /features` - Store feature vectors
- `GET /features/{key}` - Retrieve features with fallback
- `POST /batch` - Bulk feature operations
- `GET /metadata` - Feature schema and lineage

## Quick Start

```bash
# Development
docker-compose up -d
python -m src.main

# Production deployment
terraform apply
kubectl apply -k k8s/
```

## Monitoring

- Circuit breaker metrics for Redis/PostgreSQL
- Feature drift detection alerts
- API latency and error rates
- Cache hit ratios and sync delays

## Tech Stack

- **Python 3.11** with asyncio
- **FastAPI** for REST API
- **Redis** for caching layer
- **PostgreSQL** for persistent storage
- **Kafka** for streaming ingestion
- **Prometheus** for metrics
- **Kubernetes** for orchestration