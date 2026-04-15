# ML Feature Store Sync

Production-grade feature store with Redis caching, PostgreSQL persistence, and real-time ML feature pipeline synchronization.

## Overview

This system provides a high-performance feature store that synchronizes ML features across multiple storage backends with built-in monitoring, drift detection, and feature versioning.

## Architecture

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐
│   Kafka         │───▶│ Sync Engine  │───▶│   PostgreSQL    │
│   Streams       │    │              │    │   (Persistence) │
└─────────────────┘    └──────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐
                       │     Redis       │
                       │    (Cache)      │
                       └─────────────────┘
```

## Key Features

### 🚀 **Multi-Backend Storage**
- **Redis**: Sub-millisecond feature retrieval with automatic TTL
- **PostgreSQL**: Durable persistence with optimized queries
- **Feature Versioning**: Track schema changes and feature evolution

### 📊 **Real-time Processing**
- **Kafka Integration**: Stream processing for real-time feature updates
- **Batch Processing**: Efficient bulk feature ingestion API
- **Sync Engine**: Intelligent synchronization between storage layers

### 🔍 **ML Operations**
- **Drift Detection**: Statistical drift monitoring with configurable thresholds
- **Feature Lineage**: Track feature dependencies and transformations  
- **Health Monitoring**: Comprehensive health checks and metrics

### ☁️ **Production Infrastructure**
- **Kubernetes**: Auto-scaling deployment with HPA
- **Terraform**: Complete GCP infrastructure as code
- **Monitoring**: Prometheus metrics with Grafana dashboards
- **Load Balancing**: Application Load Balancer with health checks

## Quick Start

### Local Development
```bash
# Start all services
docker-compose up -d

# Install dependencies
pip install -r requirements.txt

# Run feature store
python src/main.py
```

### Deploy to GCP
```bash
# Infrastructure
cd terraform
terraform init && terraform apply

# Kubernetes
kubectl apply -f k8s/
```

## API Usage

### Store Features
```python
POST /features/batch
{
  "features": {
    "user_123": {
      "age": 25,
      "score": 0.85,
      "category": "premium"
    }
  }
}
```

### Retrieve Features
```python
GET /features/user_123
# Returns: {"age": 25, "score": 0.85, "category": "premium"}
```

### Feature Versioning
```python
# Create new version
POST /features/version
{
  "feature_name": "user_profile",
  "schema": {"age": "int", "score": "float"},
  "metadata": {"model_version": "v1.2.0"}
}

# List versions
GET /features/user_profile/versions
```

### Monitor Drift
```python
GET /monitoring/drift/user_score
# Returns drift statistics and alerts
```

## Configuration

### Environment Variables
```bash
REDIS_URL=redis://localhost:6379
DATABASE_URL=postgresql://user:pass@localhost/features
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
PROMETHEUS_PORT=8001
```

## Monitoring

- **Metrics**: http://localhost:8001/metrics
- **Health**: http://localhost:8000/health
- **Feature Lineage**: http://localhost:8000/lineage

## Technology Stack

- **Backend**: Python 3.11+ with FastAPI
- **Storage**: Redis 7.0+ & PostgreSQL 15+
- **Streaming**: Apache Kafka
- **Infrastructure**: Terraform, Kubernetes, GCP
- **Monitoring**: Prometheus, Grafana
- **Testing**: Pytest with fixtures

## Skills Demonstrated

✅ **ML/AI**: Feature engineering, drift detection, model serving infrastructure  
✅ **Backend**: FastAPI APIs, async processing, data validation  
✅ **Database**: Redis optimization, PostgreSQL schemas, query performance  
✅ **Infrastructure**: Terraform IaC, GCP services, auto-scaling  
✅ **DevOps**: Docker, Kubernetes, CI/CD pipelines  
✅ **Data**: Kafka streaming, ETL pipelines, data quality monitoring  
✅ **SRE**: Prometheus monitoring, health checks, reliability patterns

---
*Production ML infrastructure for real-time feature serving*