# ML Feature Store Sync

Production feature store with Redis caching, PostgreSQL persistence, and real-time ML feature pipeline synchronization.

## Architecture

- **Storage**: PostgreSQL (persistence) + Redis (caching)
- **Streaming**: Kafka consumer for real-time features
- **Monitoring**: Prometheus metrics, drift detection
- **API**: FastAPI with health checks and feature lineage
- **Infrastructure**: Kubernetes deployment with HPA, Terraform for GCP

## Skills Demonstrated

- **ML/Data**: Feature store, drift detection, real-time pipelines
- **Infrastructure**: Terraform, Kubernetes, monitoring setup
- **Backend**: FastAPI, PostgreSQL, Redis integration
- **DevOps**: CI/CD, containerization, observability
- **SRE**: Metrics collection, health checks, reliability

## Quick Start

```bash
# Local development
docker-compose up -d
pip install -r requirements.txt
python src/main.py

# Kubernetes deployment
kubectl apply -f k8s/

# Infrastructure
cd terraform && terraform apply
```

## API Endpoints

- `GET /health` - Health check
- `GET /features/{feature_id}` - Retrieve feature
- `POST /features` - Store feature
- `GET /lineage/{feature_id}` - Feature lineage
- `GET /metrics` - Prometheus metrics

## Monitoring

- Prometheus ServiceMonitor for metrics scraping
- Feature drift detection with configurable thresholds
- Redis/PostgreSQL connection monitoring
- Kafka lag monitoring

## Testing

```bash
pytest tests/ -v
```