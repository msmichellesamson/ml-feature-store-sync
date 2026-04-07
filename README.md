# ML Feature Store Sync

Production-grade feature store with Redis caching, PostgreSQL persistence, and real-time ML feature pipeline synchronization.

## Architecture

```
Kafka Stream → Sync Engine → Feature Store
                    ↓
              PostgreSQL (persistent)
                    ↓
                Redis (cache)
```

## Tech Stack

- **Backend**: Python FastAPI
- **Storage**: PostgreSQL + Redis
- **Streaming**: Kafka
- **Monitoring**: Prometheus + Grafana
- **Infrastructure**: Terraform + Kubernetes
- **Deployment**: Docker + CI/CD

## Skills Demonstrated

- **ML/Data**: Feature engineering, drift detection, real-time pipelines
- **Backend**: FastAPI, async processing, data validation
- **Database**: PostgreSQL optimization, Redis caching strategies
- **Infrastructure**: Terraform (GCP), Kubernetes manifests
- **SRE**: Health checks, monitoring, observability
- **DevOps**: CI/CD, containerization, GitOps

## Quick Start

### Local Development
```bash
docker-compose up -d
python -m src.main
```

### Kubernetes Deployment
```bash
kubectl apply -f k8s/
```

### Terraform Infrastructure
```bash
cd terraform
terraform init
terraform apply
```

## API Endpoints

- `GET /features/{feature_id}` - Retrieve feature
- `POST /features/batch` - Batch feature retrieval
- `POST /features/sync` - Force sync from Kafka
- `GET /health` - Health check
- `GET /metrics` - Prometheus metrics

## Monitoring

- **Metrics**: Feature freshness, cache hit rates, sync latency
- **Alerts**: Feature drift detection, sync failures
- **Dashboards**: Grafana dashboard for feature store health

## Feature Schema

```python
class FeatureRecord:
    feature_id: str
    feature_value: float
    timestamp: datetime
    metadata: Dict[str, Any]
```

## Current Status

✅ Core feature store implementation  
✅ Redis caching layer  
✅ PostgreSQL persistence  
✅ Kafka streaming consumer  
✅ Feature drift detection  
✅ Docker containerization  
✅ Terraform infrastructure  
✅ Prometheus monitoring  
✅ Kubernetes deployment manifests  
🔄 CI/CD pipeline  
📋 Load testing  
📋 Chaos engineering

## License

MIT