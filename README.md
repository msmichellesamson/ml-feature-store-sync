# ML Feature Store Sync

Production-grade feature store with Redis caching, PostgreSQL persistence, and real-time ML feature pipeline synchronization.

## Features

- **Real-time Sync**: Kafka-based streaming pipeline for feature updates
- **Multi-storage**: Redis for low-latency access, PostgreSQL for persistence
- **Schema Management**: Versioned feature schemas with validation
- **Drift Detection**: Automatic feature drift monitoring and alerting
- **Feature Validation**: REST API for validating features against schemas
- **Production Ready**: Full observability, health checks, and auto-scaling

## Architecture

```
Kafka → Transformer → Feature Store (Redis + PostgreSQL)
                           ↓
                    ML Models (via API)
```

## API Endpoints

### Features
- `GET /features/{feature_group}` - Get feature values
- `POST /features/batch` - Batch feature retrieval
- `PUT /features/{feature_group}` - Update features

### Validation
- `POST /validation/validate` - Validate features against schema
- `GET /validation/schema/{feature_group}` - Get validation schema

### Monitoring
- `GET /health` - Health check
- `GET /metadata/lineage/{feature_group}` - Feature lineage

## Quick Start

```bash
# Start infrastructure
docker-compose up -d

# Install dependencies
pip install -r requirements.txt

# Run feature store
python -m src.main
```

## Configuration

- Redis: Caching layer for sub-millisecond access
- PostgreSQL: Persistent storage with versioning
- Kafka: Streaming feature updates
- Prometheus: Metrics and drift detection

## Infrastructure

Deploy to AWS using Terraform:

```bash
cd terraform/
terraform plan
terraform apply
```

## Monitoring

- Feature drift detection with alerting
- API latency and error rate tracking
- Cache hit/miss ratios
- Schema validation metrics

## Tech Stack

- **Backend**: Python, FastAPI, asyncio
- **Storage**: Redis, PostgreSQL
- **Streaming**: Apache Kafka
- **Infrastructure**: Terraform, Kubernetes, Docker
- **Monitoring**: Prometheus, Grafana
- **CI/CD**: GitHub Actions