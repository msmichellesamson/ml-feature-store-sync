# ML Feature Store Sync

Production-grade feature store with Redis caching, PostgreSQL persistence, and real-time ML feature pipeline synchronization.

## Features

- **Real-time sync**: Kafka-based feature pipeline synchronization
- **Dual storage**: Redis for low-latency access, PostgreSQL for persistence  
- **ML monitoring**: Data drift detection and feature quality metrics
- **Production ready**: Kubernetes deployment, auto-scaling, monitoring

## Architecture

```
Kafka → Sync Engine → Redis (cache) + PostgreSQL (persistence)
                  ↓
              REST API → ML Models
                  ↓
           Drift Detection → Alerts
```

## Quick Start

```bash
# Local development
docker-compose up -d

# Kubernetes deployment
kubectl apply -f k8s/

# Test the API
curl http://localhost:8000/features/user_123
```

## API Endpoints

- `GET /features/{entity_id}` - Get features for entity
- `POST /features/batch` - Batch feature retrieval
- `GET /health` - Health check
- `GET /metrics` - Prometheus metrics

## Configuration

All configuration is managed via Kubernetes ConfigMaps and environment variables:

- Redis/PostgreSQL connection settings
- Kafka consumer configuration
- Drift detection thresholds
- API and metrics ports
- Feature TTL and batch sizes

## Infrastructure

- **Kubernetes**: Auto-scaling deployment with HPA
- **Terraform**: GCP infrastructure provisioning
- **Monitoring**: Prometheus metrics, Grafana dashboards
- **CI/CD**: GitHub Actions with automated testing

## Skills Demonstrated

- **ML/AI**: Feature store architecture, data drift detection
- **Infrastructure**: Terraform, Kubernetes, cloud deployment
- **Backend**: REST APIs, distributed caching, microservices
- **Database**: PostgreSQL persistence, Redis caching
- **DevOps**: Container orchestration, monitoring, CI/CD
- **Data**: Kafka streaming, ETL pipelines, data quality
- **SRE**: Health checks, metrics, observability