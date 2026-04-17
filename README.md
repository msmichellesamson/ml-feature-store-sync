# ML Feature Store Sync

Production feature store with Redis caching, PostgreSQL persistence, and real-time ML feature pipeline synchronization.

## Architecture

```
Kafka → Transformer → Feature Store → Redis Cache
                           ↓
                    PostgreSQL (persistence)
```

## Skills Demonstrated

- **ML/Data**: Real-time feature pipelines, drift detection, feature versioning
- **Infrastructure**: Terraform (GCP), Kubernetes, Docker
- **Backend**: FastAPI, PostgreSQL, Redis
- **SRE**: Prometheus monitoring, health checks, HPA
- **DevOps**: GitHub Actions CI/CD, GitOps
- **Database**: Query optimization, data modeling

## Quick Start

```bash
# Development
docker-compose up -d
python -m src.main

# Production
terraform -chdir=terraform apply
kubectl apply -k k8s/
```

## API Endpoints

- `GET /features/{name}` - Retrieve feature values
- `POST /features/{name}/batch` - Batch computation
- `GET /lineage/{name}` - Feature lineage
- `GET /health` - Health check

Full API documentation: [docs/api-spec.yaml](docs/api-spec.yaml)

## Monitoring

- Prometheus metrics on `/metrics`
- Grafana dashboards for feature drift
- Health checks with circuit breakers

## Infrastructure

- **Compute**: GKE cluster with HPA
- **Storage**: Cloud SQL (PostgreSQL), Memorystore (Redis)
- **Messaging**: Kafka for streaming
- **Monitoring**: Prometheus + Grafana

## Testing

```bash
pytest tests/ -v
```

Tests cover sync engine, feature store operations, and API endpoints.