import asyncio
import json
import pytest
import pytest_asyncio
from datetime import datetime, timezone
from typing import Dict, Any, List
from unittest.mock import AsyncMock, MagicMock, patch
import uuid

import redis.asyncio as redis
import asyncpg
from kafka import KafkaProducer
from fastapi.testclient import TestClient

from src.main import app
from src.core.feature_store import FeatureStore
from src.core.sync_engine import SyncEngine
from src.storage.redis_client import RedisClient
from src.storage.postgres_client import PostgresClient
from src.streaming.kafka_consumer import KafkaConsumer
from src.models.feature_schema import Feature, FeatureType, FeatureGroup


@pytest_asyncio.fixture
async def redis_client():
    """Redis client for testing."""
    client = redis.Redis(
        host="localhost",
        port=6379,
        db=1,  # Use different DB for tests
        decode_responses=True
    )
    yield client
    await client.flushdb()
    await client.close()


@pytest_asyncio.fixture
async def postgres_client():
    """PostgreSQL client for testing."""
    conn = await asyncpg.connect(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        database="test_feature_store"
    )
    
    # Setup test schema
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS feature_groups (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name VARCHAR(255) UNIQUE NOT NULL,
            description TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS features (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            group_id UUID REFERENCES feature_groups(id),
            name VARCHAR(255) NOT NULL,
            feature_type VARCHAR(50) NOT NULL,
            default_value JSONB,
            metadata JSONB DEFAULT '{}',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(group_id, name)
        )
    """)
    
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS feature_values (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            feature_id UUID REFERENCES features(id),
            entity_id VARCHAR(255) NOT NULL,
            value JSONB NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            INDEX (feature_id, entity_id, timestamp DESC)
        )
    """)
    
    yield conn
    
    # Cleanup
    await conn.execute("TRUNCATE TABLE feature_values CASCADE")
    await conn.execute("TRUNCATE TABLE features CASCADE")
    await conn.execute("TRUNCATE TABLE feature_groups CASCADE")
    await conn.close()


@pytest_asyncio.fixture
async def feature_store(redis_client, postgres_client):
    """Feature store instance for testing."""
    redis_storage = RedisClient(redis_client)
    postgres_storage = PostgresClient(postgres_client)
    
    store = FeatureStore(
        redis_client=redis_storage,
        postgres_client=postgres_storage
    )
    
    return store


@pytest_asyncio.fixture
async def sync_engine(feature_store):
    """Sync engine for testing."""
    return SyncEngine(feature_store=feature_store)


@pytest.fixture
def test_client():
    """FastAPI test client."""
    return TestClient(app)


@pytest.fixture
def sample_feature_group() -> FeatureGroup:
    """Sample feature group for testing."""
    return FeatureGroup(
        name="user_profile",
        description="User profile features"
    )


@pytest.fixture
def sample_feature() -> Feature:
    """Sample feature for testing."""
    return Feature(
        name="age",
        feature_type=FeatureType.NUMERIC,
        default_value=0,
        metadata={"unit": "years", "min": 0, "max": 150}
    )


class TestFeatureStore:
    """Integration tests for FeatureStore."""
    
    @pytest.mark.asyncio
    async def test_create_feature_group(self, feature_store, sample_feature_group):
        """Test creating a feature group."""
        group_id = await feature_store.create_feature_group(sample_feature_group)
        
        assert group_id is not None
        assert isinstance(group_id, str)
        
        # Verify group exists in PostgreSQL
        retrieved_group = await feature_store.get_feature_group(group_id)
        assert retrieved_group.name == sample_feature_group.name
        assert retrieved_group.description == sample_feature_group.description
    
    @pytest.mark.asyncio
    async def test_create_feature(self, feature_store, sample_feature_group, sample_feature):
        """Test creating a feature within a group."""
        group_id = await feature_store.create_feature_group(sample_feature_group)
        
        feature_id = await feature_store.create_feature(group_id, sample_feature)
        
        assert feature_id is not None
        assert isinstance(feature_id, str)
        
        # Verify feature exists
        retrieved_feature = await feature_store.get_feature(feature_id)
        assert retrieved_feature.name == sample_feature.name
        assert retrieved_feature.feature_type == sample_feature.feature_type
        assert retrieved_feature.metadata == sample_feature.metadata
    
    @pytest.mark.asyncio
    async def test_store_and_get_feature_value(self, feature_store, sample_feature_group, sample_feature):
        """Test storing and retrieving feature values."""
        group_id = await feature_store.create_feature_group(sample_feature_group)
        feature_id = await feature_store.create_feature(group_id, sample_feature)
        
        entity_id = "user_123"
        value = 25
        timestamp = datetime.now(timezone.utc)
        
        # Store feature value
        await feature_store.store_feature_value(
            feature_id=feature_id,
            entity_id=entity_id,
            value=value,
            timestamp=timestamp
        )
        
        # Retrieve from Redis (cache)
        cached_value = await feature_store.get_feature_value(feature_id, entity_id)
        assert cached_value["value"] == value
        assert cached_value["timestamp"] == timestamp.isoformat()
        
        # Retrieve historical values from PostgreSQL
        historical = await feature_store.get_historical_values(
            feature_id=feature_id,
            entity_id=entity_id,
            limit=10
        )
        
        assert len(historical) == 1
        assert historical[0]["value"] == value
    
    @pytest.mark.asyncio
    async def test_batch_store_features(self, feature_store, sample_feature_group, sample_feature):
        """Test batch storing of feature values."""
        group_id = await feature_store.create_feature_group(sample_feature_group)
        feature_id = await feature_store.create_feature(group_id, sample_feature)
        
        batch_data = [
            {
                "feature_id": feature_id,
                "entity_id": f"user_{i}",
                "value": i * 10,
                "timestamp": datetime.now(timezone.utc)
            }
            for i in range(1, 6)
        ]
        
        await feature_store.batch_store_features(batch_data)
        
        # Verify all values were stored
        for data in batch_data:
            cached = await feature_store.get_feature_value(
                data["feature_id"], 
                data["entity_id"]
            )
            assert cached["value"] == data["value"]
    
    @pytest.mark.asyncio
    async def test_feature_value_expiration(self, feature_store, sample_feature_group, sample_feature):
        """Test Redis cache expiration."""
        group_id = await feature_store.create_feature_group(sample_feature_group)
        feature_id = await feature_store.create_feature(group_id, sample_feature)
        
        entity_id = "user_expire_test"
        value = 100
        ttl = 2  # 2 seconds
        
        await feature_store.store_feature_value(
            feature_id=feature_id,
            entity_id=entity_id,
            value=value,
            timestamp=datetime.now(timezone.utc),
            ttl=ttl
        )
        
        # Should exist initially
        cached = await feature_store.get_feature_value(feature_id, entity_id)
        assert cached["value"] == value
        
        # Wait for expiration
        await asyncio.sleep(ttl + 1)
        
        # Should be expired from cache but still in PostgreSQL
        cached = await feature_store.get_feature_value(feature_id, entity_id)
        assert cached is None
        
        historical = await feature_store.get_historical_values(
            feature_id=feature_id,
            entity_id=entity_id,
            limit=1
        )
        assert len(historical) == 1
        assert historical[0]["value"] == value
    
    @pytest.mark.asyncio
    async def test_feature_group_not_found(self, feature_store):
        """Test handling of non-existent feature group."""
        non_existent_id = str(uuid.uuid4())
        
        group = await feature_store.get_feature_group(non_existent_id)
        assert group is None
    
    @pytest.mark.asyncio
    async def test_concurrent_feature_updates(self, feature_store, sample_feature_group, sample_feature):
        """Test concurrent updates to the same feature."""
        group_id = await feature_store.create_feature_group(sample_feature_group)
        feature_id = await feature_store.create_feature(group_id, sample_feature)
        
        entity_id = "user_concurrent"
        
        async def update_feature(value: int):
            await feature_store.store_feature_value(
                feature_id=feature_id,
                entity_id=entity_id,
                value=value,
                timestamp=datetime.now(timezone.utc)
            )
        
        # Run concurrent updates
        tasks = [update_feature(i) for i in range(10)]
        await asyncio.gather(*tasks)
        
        # Should have the most recent value
        cached = await feature_store.get_feature_value(feature_id, entity_id)
        assert cached is not None
        assert isinstance(cached["value"], int)
        assert 0 <= cached["value"] <= 9


class TestSyncEngine:
    """Integration tests for SyncEngine."""
    
    @pytest.mark.asyncio
    async def test_redis_to_postgres_sync(self, sync_engine, feature_store, sample_feature_group, sample_feature):
        """Test synchronizing data from Redis to PostgreSQL."""
        group_id = await feature_store.create_feature_group(sample_feature_group)
        feature_id = await feature_store.create_feature(group_id, sample_feature)
        
        # Store data only in Redis
        entity_id = "user_sync_test"
        value = 42
        timestamp = datetime.now(timezone.utc)
        
        redis_key = f"feature:{feature_id}:{entity_id}"
        await feature_store.redis_client.client.hset(
            redis_key,
            mapping={
                "value": json.dumps(value),
                "timestamp": timestamp.isoformat(),
                "feature_id": feature_id,
                "entity_id": entity_id
            }
        )
        
        # Sync to PostgreSQL
        await sync_engine.sync_redis_to_postgres()
        
        # Verify data exists in PostgreSQL
        historical = await feature_store.get_historical_values(
            feature_id=feature_id,
            entity_id=entity_id,
            limit=1
        )
        
        assert len(historical) == 1
        assert historical[0]["value"] == value
    
    @pytest.mark.asyncio
    async def test_postgres_to_redis_sync(self, sync_engine, feature_store, sample_feature_group, sample_feature):
        """Test synchronizing data from PostgreSQL to Redis."""
        group_id = await feature_store.create_feature_group(sample_feature_group)
        feature_id = await feature_store.create_feature(group_id, sample_feature)
        
        # Store data only in PostgreSQL
        entity_id = "user_pg_sync"
        value = 99
        timestamp = datetime.now(timezone.utc)
        
        await feature_store.postgres_client.connection.execute("""
            INSERT INTO feature_values (feature_id, entity_id, value, timestamp)
            VALUES ($1, $2, $3, $4)
        """, uuid.UUID(feature_id), entity_id, json.dumps(value), timestamp)
        
        # Sync to Redis
        await sync_engine.sync_postgres_to_redis([feature_id])
        
        # Verify data exists in Redis
        cached = await feature_store.get_feature_value(feature_id, entity_id)
        assert cached is not None
        assert cached["value"] == value
    
    @pytest.mark.asyncio
    async def test_incremental_sync(self, sync_engine, feature_store, sample_feature_group, sample_feature):
        """Test incremental synchronization."""
        group_id = await feature_store.create_feature_group(sample_feature_group)
        feature_id = await feature_store.create_feature(group_id, sample_feature)
        
        # Initial sync
        await sync_engine.sync_redis_to_postgres()
        last_sync = datetime.now(timezone.utc)
        
        # Add new data after sync
        await asyncio.sleep(1)
        entity_id = "user_incremental"
        value = 77
        
        await feature_store.store_feature_value(
            feature_id=feature_id,
            entity_id=entity_id,
            value=value,
            timestamp=datetime.now(timezone.utc)
        )
        
        # Incremental sync
        await sync_engine.sync_redis_to_postgres(since=last_sync)
        
        # Verify new data was synced
        historical = await feature_store.get_historical_values(
            feature_id=feature_id,
            entity_id=entity_id,
            limit=1
        )
        
        assert len(historical) == 1
        assert historical[0]["value"] == value


class TestKafkaIntegration:
    """Integration tests for Kafka streaming."""
    
    @pytest.fixture
    def kafka_producer(self):
        """Kafka producer for testing."""
        return KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
    
    @pytest.mark.asyncio
    async def test_kafka_message_processing(self, feature_store, sample_feature_group, sample_feature, kafka_producer):
        """Test processing Kafka messages."""
        group_id = await feature_store.create_feature_group(sample_feature_group)
        feature_id = await feature_store.create_feature(group_id, sample_feature)
        
        # Create Kafka consumer
        consumer = KafkaConsumer(feature_store=feature_store)
        
        # Send test message
        message = {
            "feature_id": feature_id,
            "entity_id": "user_kafka_test",
            "value": 123,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        kafka_producer.send('feature-updates', value=message, key=feature_id)
        kafka_producer.flush()
        
        # Process message (mock the consumption)
        await consumer._process_message(message)
        
        # Verify feature was stored
        cached = await feature_store.get_feature_value(feature_id, "user_kafka_test")
        assert cached is not None
        assert cached["value"] == 123
    
    @pytest.mark.asyncio
    async def test_kafka_batch_processing(self, feature_store, sample_feature_group, sample_feature, kafka_producer):
        """Test processing batch of Kafka messages."""
        group_id = await feature_store.create_feature_group(sample_feature_group)
        feature_id = await feature_store.create_feature(group_id, sample_feature)
        
        consumer = KafkaConsumer(feature_store=feature_store)
        
        # Send multiple messages
        messages = []
        for i in range(5):
            message = {
                "feature_id": feature_id,
                "entity_id": f"user_batch_{i}",
                "value": i * 20,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            messages.append(message)
            kafka_producer.send('feature-updates', value=message, key=feature_id)
        
        kafka_producer.flush()
        
        # Process messages in batch
        await consumer._process_batch(messages)
        
        # Verify all features were stored
        for i, msg in enumerate(messages):
            cached = await feature_store.get_feature_value(
                feature_id, 
                f"user_batch_{i}"
            )
            assert cached is not None
            assert cached["value"] == i * 20


class TestAPIIntegration:
    """Integration tests for FastAPI endpoints."""
    
    def test_health_check(self, test_client):
        """Test health check endpoint."""
        response = test_client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"
    
    def test_create_feature_group_api(self, test_client):
        """Test creating feature group via API."""
        payload = {
            "name": "api_test_group",
            "description": "Test group via API"
        }
        
        response = test_client.post("/feature-groups", json=payload)
        assert response.status_code == 201
        
        data = response.json()
        assert "id" in data
        assert data["name"] == payload["name"]
        assert data["description"] == payload["description"]
    
    def test_store_feature_value_api(self, test_client):
        """Test storing feature value via API."""
        # First create a feature group and feature
        group_payload = {
            "name": "api_value_group",
            "description": "Test group for values"
        }
        group_response = test_client.post("/feature-groups", json=group_payload)
        group_id = group_response.json()["id"]
        
        feature_payload = {
            "name": "test_feature",
            "feature_type": "numeric",
            "default_value": 0
        }
        feature_response = test_client.post(
            f"/feature-groups/{group_id}/features", 
            json=feature_payload
        )
        feature_id = feature_response.json()["id"]
        
        # Store feature value
        value_payload = {
            "entity_id": "api_user_123",
            "value": 456,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        response = test_client.post(
            f"/features/{feature_id}/values",
            json=value_payload
        )
        assert response.status_code == 201
    
    def test_get_feature_value_api(self, test_client):
        """Test retrieving feature value via API."""
        # Setup (create group, feature, and value)
        group_payload = {"name": "get_test_group", "description": "Test"}
        group_response = test_client.post("/feature-groups", json=group_payload)
        group_id = group_response.json()["id"]
        
        feature_payload = {
            "name": "get_test_feature",
            "feature_type": "numeric",
            "default_value": 0
        }
        feature_response = test_client.post(
            f"/feature-groups/{group_id}/features",
            json=feature_payload
        )
        feature_id = feature_response.json()["id"]
        
        value_payload = {
            "entity_id": "get_user_123",
            "value": 789,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        test_client.post(f"/features/{feature_id}/values", json=value_payload)
        
        # Retrieve value
        response = test_client.get(f"/features/{feature_id}/values/get_user_123")
        assert response.status_code == 200
        
        data = response.json()
        assert data["value"] == 789
        assert data["entity_id"] == "get_user_123"
    
    def test_batch_store_api(self, test_client):
        """Test batch storing feature values via API."""
        # Setup
        group_payload = {"name": "batch_group", "description": "Batch test"}
        group_response = test_client.post("/feature-groups", json=group_payload)
        group_id = group_response.json()["id"]
        
        feature_payload = {
            "name": "batch_feature",
            "feature_type": "numeric",
            "default_value": 0
        }
        feature_response = test_client.post(
            f"/feature-groups/{group_id}/features",
            json=feature_payload
        )
        feature_id = feature_response.json()["id"]
        
        # Batch store
        batch_payload = {
            "values": [
                {
                    "feature_id": feature_id,
                    "entity_id": f"batch_user_{i}",
                    "value": i * 100,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
                for i in range(3)
            ]
        }
        
        response = test_client.post("/features/batch", json=batch_payload)
        assert response.status_code == 201
        
        # Verify values were stored
        for i in range(3):
            get_response = test_client.get(
                f"/features/{feature_id}/values/batch_user_{i}"
            )
            assert get_response.status_code == 200
            assert get_response.json()["value"] == i * 100


class TestErrorHandling:
    """Integration tests for error handling."""
    
    @pytest.mark.asyncio
    async def test_invalid_feature_type(self, feature_store, sample_feature_group):
        """Test handling invalid feature types."""
        group_id = await feature_store.create_feature_group(sample_feature_group)
        
        invalid_feature = Feature(
            name="invalid_feature",
            feature_type="invalid_type",  # Invalid type
            default_value=None
        )
        
        with pytest.raises(ValueError):
            await feature_store.create_feature(group_id, invalid_feature)
    
    @pytest.mark.asyncio
    async def test_redis_connection_failure(self, postgres_client):
        """Test handling Redis connection failures."""
        # Create feature store with invalid Redis connection
        invalid_redis = MagicMock()
        invalid_redis.get = AsyncMock(side_effect=redis.ConnectionError())
        
        redis_client = RedisClient(invalid_redis)
        postgres_client_wrapper = PostgresClient(postgres_client)
        
        store = FeatureStore(
            redis_client=redis_client,
            postgres_client=postgres_client_wrapper
        )
        
        # Should fallback to PostgreSQL
        feature_value = await store.get_feature_value("test_feature", "test_entity")
        assert feature_value is None  # No data exists, but no exception raised
    
    def test_api_validation_errors(self, test_client):
        """Test API validation error handling."""
        # Invalid feature group payload
        invalid_payload = {"name": ""}  # Empty name should fail validation
        
        response = test_client.post("/feature-groups", json=invalid_payload)
        assert response.status_code == 422  # Validation error
        
        # Missing required fields
        incomplete_payload = {"description": "Missing name"}
        response = test_client.post("/feature-groups", json=incomplete_payload)
        assert response.status_code == 422


class TestPerformance:
    """Performance and load tests."""
    
    @pytest.mark.asyncio
    async def test_bulk_feature_storage(self, feature_store, sample_feature_group, sample_feature):
        """Test performance of bulk feature storage."""
        import time
        
        group_id = await feature_store.create_feature_group(sample_feature_group)
        feature_id = await feature_store.create_feature(group_id, sample_feature)
        
        # Generate large batch of data
        batch_size = 1000
        batch_data = [
            {
                "feature_id": feature_id,
                "entity_id": f"perf_user_{i}",
                "value": i,
                "timestamp": datetime.now(timezone.utc)
            }
            for i in range(batch_size)
        ]
        
        start_time = time.time()
        await feature_store.batch_store_features(batch_data)
        end_time = time.time()
        
        duration = end_time - start_time
        throughput = batch_size / duration
        
        # Should handle at least 100 features per second
        assert throughput > 100, f"Throughput too low: {throughput:.2f} features/sec"
    
    @pytest.mark.asyncio
    async def test_concurrent_reads(self, feature_store, sample_feature_group, sample_feature):
        """Test concurrent read performance."""
        group_id = await feature_store.create_feature_group(sample_feature_group)
        feature_id = await feature_store.create_feature(group_id, sample_feature)
        
        # Store some test data
        entity_id = "concurrent_read_user"
        await feature_store.store_feature_value(
            feature_id=feature_id,
            entity_id=entity_id,
            value=42,
            timestamp=datetime.now(timezone.utc)
        )
        
        async def read_feature():
            return await feature_store.get_feature_value(feature_id, entity_id)
        
        # Run 50 concurrent reads
        tasks = [read_feature() for _ in range(50)]
        results = await asyncio.gather(*tasks)
        
        # All reads should succeed
        assert all(result is not None for result in results)
        assert all(result["value"] == 42 for result in results)