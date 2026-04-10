# ML Feature Store Sync

Production feature store with Redis caching, PostgreSQL persistence, and real-time ML feature pipeline synchronization.

## Architecture

```
ML Models → Kafka/PubSub → Feature Store API → Redis Cache + PostgreSQL
                                ↓
                         Drift Detection + Lineage Tracking
```

## Features

- **Real-time sync**: Kafka consumer for streaming feature updates
- **Dual storage**: Redis for low-latency reads, PostgreSQL for persistence
- **ML monitoring**: Feature drift detection and alerting
- **API lineage**: Track feature usage across models
- **Production-ready**: Full observability, health checks, auto-scaling

## Quick Start

```bash
# Deploy infrastructure
cd terraform && terraform apply

# Deploy to Kubernetes
kubectl apply -f k8s/

# Or run locally
docker-compose up
```

## API Endpoints

- `GET /health` - Health check
- `GET /features/{key}` - Retrieve feature
- `POST /features` - Store feature
- `GET /lineage/{model_id}` - Feature lineage

## Infrastructure

- **GCP**: Redis, Cloud SQL, Pub/Sub, GKE
- **Monitoring**: Prometheus, Grafana, alerting rules
- **Auto-scaling**: HPA based on CPU and memory
- **Security**: TLS, authentication, network policies

## Tech Stack

- **Backend**: FastAPI, asyncio, gRPC
- **Storage**: Redis, PostgreSQL
- **Streaming**: Kafka/Pub Sub
- **ML**: scikit-learn, feature drift detection
- **Infra**: Terraform, Kubernetes, Docker