# ML Feature Store with Real-time Sync Engine

![Build Status](https://img.shields.io/github/workflow/status/michellesamson/ml-feature-store/CI?style=flat-square)
![Python](https://img.shields.io/badge/python-3.11+-blue.svg?style=flat-square)
![License](https://img.shields.io/badge/license-MIT-green.svg?style=flat-square)

Production-grade feature store with sub-millisecond feature serving, real-time Redis/PostgreSQL synchronization, and Kafka-based streaming pipeline. Built for ML systems requiring high-throughput feature retrieval with ACID persistence guarantees.

## Skills Demonstrated

- **ML Engineering**: Feature versioning, online/offline serving, and ML pipeline integration
- **Backend**: FastAPI async architecture with connection pooling and circuit breakers
- **Database**: PostgreSQL with optimized feature queries, Redis caching with TTL management
- **Infrastructure**: Terraform GCP deployment with auto-scaling and monitoring
- **Data Engineering**: Kafka streaming pipeline with exactly-once processing semantics
- **SRE**: Prometheus metrics, alerting, and distributed tracing

## Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   ML Model  │    │ Kafka Topic │    │ Data Source │
│  (Serving)  │    │  (Updates)  │    │   Systems   │
└──────┬──────┘    └──────┬──────┘    └──────┬──────┘
       │                  │                  │
       │ GET /features    │ consume          │ produce
       ▼                  ▼                  ▼
┌─────────────────────────────────────────────────────┐
│                FastAPI Server                       │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐ │
│  │ Feature API │  │ Sync Engine │  │Kafka Consumer│ │
│  └─────────────┘  └─────────────┘  └─────────────┘ │
└─────────┬───────────────┬───────────────┬─────────────┘
          │               │               │
          ▼               ▼               ▼
    ┌──────────┐    ┌──────────┐    ┌──────────┐
    │  Redis   │◄──►│PostgreSQL│    │ Metrics  │
    │ (Cache)  │    │(Persist) │    │(Prom/GCP)│
    └──────────┘    └──────────┘    └──────────┘
         ▲                               ▲
         │ <1ms reads                    │ monitoring
         ▼                               ▼
   ┌──────────┐                   ┌──────────┐
   │ML Models │                   │ Alerts   │
   │ Serving  │                   │ Dashboard│
   └──────────┘                   └──────────┘
```

## Quick Start

```bash
# Start local environment
docker-compose up -d

# Install dependencies
pip install -r requirements.txt

# Run migrations
python -m alembic upgrade head

# Start the server
python src/main.py

# Test feature serving
curl -X POST "http://localhost:8000/features/batch" \
  -H "Content-Type: application/json" \
  -d '{"entity_ids": ["user_123", "user_456"], "feature_names": ["avg_purchase", "login_count"]}'
```

## Configuration

```bash
# Environment Variables
REDIS_URL=redis://localhost:6379
POSTGRES_URL=postgresql://user:pass@localhost:5432/features
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_FEATURES=ml_features
FEATURE_CACHE_TTL=3600
BATCH_SIZE=1000
SYNC_INTERVAL_MS=100
PROMETHEUS_PORT=9090
```

Configuration via `config/settings.yaml`:
```yaml
feature_store:
  cache:
    ttl_seconds: 3600
    max_connections: 100
  sync:
    batch_size: 1000
    interval_ms: 100
  monitoring:
    enable_tracing: true
    metrics_port: 9090
```

## Infrastructure

Deploy to GCP with Terraform:

```bash
cd terraform
terraform init
terraform apply

# Outputs GCP resources:
# - Cloud SQL PostgreSQL (HA)
# - MemoryStore Redis (cluster)
# - GKE cluster with auto-scaling
# - Kafka on Compute Engine
# - Monitoring stack (Prometheus/Grafana)
```

Infrastructure includes:
- **Auto-scaling**: GKE pods scale 1-50 based on CPU/memory
- **High Availability**: Multi-zone deployment with failover
- **Monitoring**: Custom Prometheus metrics with SLO alerting
- **Security**: VPC, IAM roles, encrypted connections

## API Usage

### Batch Feature Serving
```python
import httpx

async with httpx.AsyncClient() as client:
    response = await client.post(
        "http://localhost:8000/features/batch",
        json={
            "entity_ids": ["user_123", "product_456"],
            "feature_names": ["user_ltv", "product_popularity"],
            "version": "v1.2.0"  # Optional feature version
        }
    )
    features = response.json()
    # {"user_123": {"user_ltv": 156.78}, "product_456": {"product_popularity": 0.85}}
```

### Real-time Feature Updates
```python
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Update features via Kafka
producer.send('ml_features', {
    "entity_id": "user_123",
    "features": {"login_count": 45, "avg_purchase": 89.32},
    "timestamp": "2024-01-15T10:30:00Z",
    "version": "v1.2.0"
})
```

### Feature Schema Registration
```python
import httpx

schema = {
    "name": "user_engagement_v2",
    "features": {
        "session_duration": {"type": "float", "range": [0, 86400]},
        "page_views": {"type": "int", "range": [0, 1000]},
        "conversion_score": {"type": "float", "range": [0.0, 1.0]}
    },
    "version": "2.1.0"
}

response = httpx.post("http://localhost:8000/schemas", json=schema)
```

## Development

```bash
# Run tests with coverage
pytest tests/ --cov=src --cov-report=html

# Performance testing
python tests/load_test.py  # Simulates 10k QPS

# Type checking
mypy src/

# Code quality
black src/ tests/
ruff check src/ tests/

# Database migrations
alembic revision --autogenerate -m "Add feature versioning"
alembic upgrade head
```

### Testing Strategy
- **Unit tests**: Core feature store logic with mocks
- **Integration tests**: Redis/PostgreSQL with testcontainers
- **Load tests**: 10k QPS feature serving validation
- **Contract tests**: API schema validation with Pydantic

## License

MIT © Michelle Samson