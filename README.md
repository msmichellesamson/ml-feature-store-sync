# ML Feature Store Sync

Production feature store with Redis caching, PostgreSQL persistence, and real-time ML feature pipeline synchronization.

## 🏗️ Architecture

```
Kafka Stream → Feature Store → Redis Cache → ML Models
                    ↓
              PostgreSQL (persistence)
                    ↓
           Drift Detection & Monitoring
```

## 🚀 Features

- **Real-time sync**: Kafka-based streaming pipeline
- **Dual storage**: Redis for speed, PostgreSQL for persistence  
- **Feature versioning**: Schema evolution and rollback
- **Drift detection**: Automated data quality monitoring
- **Production APIs**: RESTful endpoints with caching
- **Infrastructure**: Terraform + Kubernetes deployment
- **Observability**: Prometheus metrics + structured logging

## 📡 API Endpoints

### Features
- `GET /api/v1/features/{name}` - Get feature values
- `POST /api/v1/features/batch` - Batch feature retrieval
- `GET /api/v1/features/{name}/lineage` - Feature lineage

### Metadata  
- `GET /api/v1/metadata/features/{name}` - Feature metadata with caching
- `GET /api/v1/metadata/features` - List all features

### Health
- `GET /health` - Service health check
- `GET /health/detailed` - Component health status

## 🛠️ Tech Stack

- **Backend**: Python, FastAPI, asyncio
- **Storage**: Redis, PostgreSQL
- **Streaming**: Apache Kafka
- **Infrastructure**: Terraform, Kubernetes, GCP
- **Monitoring**: Prometheus, Grafana
- **CI/CD**: GitHub Actions

## 🏃 Quick Start

```bash
# Start services
docker-compose up -d

# Run tests
pytest tests/ -v

# Deploy to GCP
cd terraform && terraform apply
kubectl apply -k k8s/
```

## 🔧 Configuration

```python
# Environment variables
REDIS_URL=redis://localhost:6379
POSTGRES_URL=postgresql://user:pass@localhost/features
KAFKA_BROKERS=localhost:9092
PROMETHEUS_PORT=8080
```

## 📊 Monitoring

- Feature freshness alerts
- Cache hit rate metrics  
- Data drift detection
- API latency tracking
- Database query performance

## 🏗️ Infrastructure

- **Terraform**: VPC, RDS, ElastiCache, ALB
- **Kubernetes**: HPA, service mesh, monitoring
- **Observability**: Prometheus ServiceMonitor

## 🧪 Testing

- Unit tests for all components
- Integration tests with real Redis/PostgreSQL
- Load testing for API endpoints
- Chaos engineering for reliability

Built for production ML workloads requiring sub-100ms feature serving with enterprise-grade reliability.