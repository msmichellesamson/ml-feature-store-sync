import asyncio
import json
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union, Tuple
from dataclasses import dataclass
from enum import Enum

import asyncpg
import redis.asyncio as redis
import structlog
from pydantic import BaseModel, validator
import numpy as np

logger = structlog.get_logger(__name__)


class FeatureStoreError(Exception):
    """Base exception for feature store operations"""
    pass


class FeatureNotFoundError(FeatureStoreError):
    """Feature does not exist"""
    pass


class FeatureValidationError(FeatureStoreError):
    """Feature validation failed"""
    pass


class DataConsistencyError(FeatureStoreError):
    """Data consistency check failed"""
    pass


class FeatureType(str, Enum):
    CATEGORICAL = "categorical"
    NUMERICAL = "numerical"
    EMBEDDING = "embedding"
    BOOLEAN = "boolean"
    TEXT = "text"


class CacheStrategy(str, Enum):
    WRITE_THROUGH = "write_through"
    WRITE_BACK = "write_back"
    READ_THROUGH = "read_through"


@dataclass
class FeatureMetadata:
    """Feature metadata configuration"""
    name: str
    feature_type: FeatureType
    description: str
    tags: List[str]
    ttl_seconds: Optional[int] = None
    version: int = 1
    created_at: datetime = None
    updated_at: datetime = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow()
        if self.updated_at is None:
            self.updated_at = datetime.utcnow()


class FeatureValue(BaseModel):
    """Feature value with metadata"""
    entity_id: str
    feature_name: str
    value: Union[str, int, float, bool, List[float]]
    feature_type: FeatureType
    timestamp: datetime
    version: int = 1

    @validator('value')
    def validate_value_type(cls, v, values):
        feature_type = values.get('feature_type')
        if feature_type == FeatureType.NUMERICAL and not isinstance(v, (int, float)):
            raise ValueError(f"Numerical feature must be int or float, got {type(v)}")
        if feature_type == FeatureType.BOOLEAN and not isinstance(v, bool):
            raise ValueError(f"Boolean feature must be bool, got {type(v)}")
        if feature_type == FeatureType.EMBEDDING and not isinstance(v, list):
            raise ValueError(f"Embedding feature must be list, got {type(v)}")
        return v

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class FeatureStoreCore:
    """Production-grade feature store with Redis caching and PostgreSQL persistence"""
    
    def __init__(
        self,
        postgres_url: str,
        redis_url: str,
        cache_strategy: CacheStrategy = CacheStrategy.WRITE_THROUGH,
        consistency_check_interval: int = 300,
        max_retries: int = 3
    ):
        self.postgres_url = postgres_url
        self.redis_url = redis_url
        self.cache_strategy = cache_strategy
        self.consistency_check_interval = consistency_check_interval
        self.max_retries = max_retries
        
        self._pg_pool: Optional[asyncpg.Pool] = None
        self._redis: Optional[redis.Redis] = None
        self._feature_metadata: Dict[str, FeatureMetadata] = {}
        self._consistency_task: Optional[asyncio.Task] = None
        
        self.logger = logger.bind(component="feature_store_core")

    async def initialize(self) -> None:
        """Initialize database connections and create tables"""
        try:
            # Initialize PostgreSQL connection pool
            self._pg_pool = await asyncpg.create_pool(
                self.postgres_url,
                min_size=5,
                max_size=20,
                command_timeout=30
            )
            
            # Initialize Redis connection
            self._redis = redis.from_url(self.redis_url, decode_responses=True)
            
            # Test connections
            async with self._pg_pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            
            await self._redis.ping()
            
            # Create database schema
            await self._create_schema()
            
            # Load feature metadata
            await self._load_feature_metadata()
            
            # Start consistency checker
            self._consistency_task = asyncio.create_task(self._consistency_checker())
            
            self.logger.info("Feature store initialized successfully")
            
        except Exception as e:
            self.logger.error("Failed to initialize feature store", error=str(e))
            raise FeatureStoreError(f"Initialization failed: {e}")

    async def close(self) -> None:
        """Clean up resources"""
        if self._consistency_task:
            self._consistency_task.cancel()
            try:
                await self._consistency_task
            except asyncio.CancelledError:
                pass
        
        if self._redis:
            await self._redis.close()
        
        if self._pg_pool:
            await self._pg_pool.close()
            
        self.logger.info("Feature store closed")

    async def _create_schema(self) -> None:
        """Create database tables if they don't exist"""
        async with self._pg_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS feature_metadata (
                    name VARCHAR(255) PRIMARY KEY,
                    feature_type VARCHAR(50) NOT NULL,
                    description TEXT,
                    tags JSONB DEFAULT '[]',
                    ttl_seconds INTEGER,
                    version INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                );
                
                CREATE INDEX IF NOT EXISTS idx_feature_metadata_type 
                ON feature_metadata(feature_type);
                
                CREATE INDEX IF NOT EXISTS idx_feature_metadata_tags 
                ON feature_metadata USING GIN(tags);
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS feature_values (
                    id BIGSERIAL PRIMARY KEY,
                    entity_id VARCHAR(255) NOT NULL,
                    feature_name VARCHAR(255) NOT NULL,
                    value JSONB NOT NULL,
                    feature_type VARCHAR(50) NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    version INTEGER DEFAULT 1,
                    FOREIGN KEY (feature_name) REFERENCES feature_metadata(name)
                );
                
                CREATE INDEX IF NOT EXISTS idx_feature_values_entity_feature 
                ON feature_values(entity_id, feature_name);
                
                CREATE INDEX IF NOT EXISTS idx_feature_values_timestamp 
                ON feature_values(timestamp DESC);
                
                CREATE INDEX IF NOT EXISTS idx_feature_values_feature_name 
                ON feature_values(feature_name);
            """)

    async def _load_feature_metadata(self) -> None:
        """Load feature metadata from database"""
        async with self._pg_pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM feature_metadata")
            for row in rows:
                metadata = FeatureMetadata(
                    name=row['name'],
                    feature_type=FeatureType(row['feature_type']),
                    description=row['description'],
                    tags=row['tags'] or [],
                    ttl_seconds=row['ttl_seconds'],
                    version=row['version'],
                    created_at=row['created_at'],
                    updated_at=row['updated_at']
                )
                self._feature_metadata[row['name']] = metadata

    async def register_feature(self, metadata: FeatureMetadata) -> None:
        """Register a new feature or update existing one"""
        try:
            async with self._pg_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO feature_metadata 
                    (name, feature_type, description, tags, ttl_seconds, version, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (name) DO UPDATE SET
                        feature_type = EXCLUDED.feature_type,
                        description = EXCLUDED.description,
                        tags = EXCLUDED.tags,
                        ttl_seconds = EXCLUDED.ttl_seconds,
                        version = EXCLUDED.version,
                        updated_at = EXCLUDED.updated_at
                """, 
                    metadata.name,
                    metadata.feature_type.value,
                    metadata.description,
                    json.dumps(metadata.tags),
                    metadata.ttl_seconds,
                    metadata.version,
                    datetime.utcnow()
                )
            
            self._feature_metadata[metadata.name] = metadata
            
            # Invalidate cache key pattern for this feature
            await self._invalidate_feature_cache(metadata.name)
            
            self.logger.info("Feature registered", feature=metadata.name)
            
        except Exception as e:
            self.logger.error("Failed to register feature", feature=metadata.name, error=str(e))
            raise FeatureStoreError(f"Feature registration failed: {e}")

    async def write_feature(
        self, 
        entity_id: str, 
        feature_name: str, 
        value: Any,
        timestamp: Optional[datetime] = None
    ) -> None:
        """Write feature value with caching strategy"""
        if feature_name not in self._feature_metadata:
            raise FeatureNotFoundError(f"Feature {feature_name} not registered")
        
        metadata = self._feature_metadata[feature_name]
        timestamp = timestamp or datetime.utcnow()
        
        feature_value = FeatureValue(
            entity_id=entity_id,
            feature_name=feature_name,
            value=value,
            feature_type=metadata.feature_type,
            timestamp=timestamp,
            version=metadata.version
        )
        
        try:
            # Execute write based on cache strategy
            if self.cache_strategy == CacheStrategy.WRITE_THROUGH:
                await self._write_through(feature_value)
            elif self.cache_strategy == CacheStrategy.WRITE_BACK:
                await self._write_back(feature_value)
            else:
                await self._write_database_only(feature_value)
            
            self.logger.debug(
                "Feature written",
                entity_id=entity_id,
                feature=feature_name,
                strategy=self.cache_strategy.value
            )
            
        except Exception as e:
            self.logger.error(
                "Failed to write feature",
                entity_id=entity_id,
                feature=feature_name,
                error=str(e)
            )
            raise FeatureStoreError(f"Feature write failed: {e}")

    async def read_feature(
        self, 
        entity_id: str, 
        feature_name: str,
        timestamp: Optional[datetime] = None
    ) -> Optional[FeatureValue]:
        """Read feature value with caching"""
        if feature_name not in self._feature_metadata:
            raise FeatureNotFoundError(f"Feature {feature_name} not registered")
        
        cache_key = self._get_cache_key(entity_id, feature_name)
        
        try:
            # Try cache first
            cached_value = await self._redis.get(cache_key)
            if cached_value:
                feature_data = json.loads(cached_value)
                feature_value = FeatureValue(**feature_data)
                
                # Check if timestamp filter applies
                if timestamp and feature_value.timestamp > timestamp:
                    feature_value = await self._read_from_database(entity_id, feature_name, timestamp)
                
                return feature_value
            
            # Cache miss - read from database
            feature_value = await self._read_from_database(entity_id, feature_name, timestamp)
            
            if feature_value and self.cache_strategy == CacheStrategy.READ_THROUGH:
                await self._cache_feature(feature_value)
            
            return feature_value
            
        except Exception as e:
            self.logger.error(
                "Failed to read feature",
                entity_id=entity_id,
                feature=feature_name,
                error=str(e)
            )
            raise FeatureStoreError(f"Feature read failed: {e}")

    async def read_features(
        self, 
        entity_id: str, 
        feature_names: List[str],
        timestamp: Optional[datetime] = None
    ) -> Dict[str, Optional[FeatureValue]]:
        """Read multiple features efficiently"""
        results = {}
        cache_keys = [self._get_cache_key(entity_id, name) for name in feature_names]
        
        try:
            # Batch read from cache
            cached_values = await self._redis.mget(cache_keys)
            
            cache_misses = []
            for i, (feature_name, cached_value) in enumerate(zip(feature_names, cached_values)):
                if cached_value:
                    feature_data = json.loads(cached_value)
                    feature_value = FeatureValue(**feature_data)
                    
                    # Check timestamp filter
                    if timestamp and feature_value.timestamp > timestamp:
                        cache_misses.append(feature_name)
                    else:
                        results[feature_name] = feature_value
                else:
                    cache_misses.append(feature_name)
            
            # Fetch cache misses from database
            if cache_misses:
                db_results = await self._read_features_from_database(
                    entity_id, cache_misses, timestamp
                )
                results.update(db_results)
                
                # Cache the results
                if self.cache_strategy == CacheStrategy.READ_THROUGH:
                    for feature_name, feature_value in db_results.items():
                        if feature_value:
                            await self._cache_feature(feature_value)
            
            return results
            
        except Exception as e:
            self.logger.error(
                "Failed to read features",
                entity_id=entity_id,
                features=feature_names,
                error=str(e)
            )
            raise FeatureStoreError(f"Features read failed: {e}")

    async def delete_feature_values(
        self, 
        entity_id: str, 
        feature_name: str,
        before_timestamp: Optional[datetime] = None
    ) -> int:
        """Delete feature values for entity"""
        try:
            async with self._pg_pool.acquire() as conn:
                if before_timestamp:
                    result = await conn.execute("""
                        DELETE FROM feature_values 
                        WHERE entity_id = $1 AND feature_name = $2 AND timestamp < $3
                    """, entity_id, feature_name, before_timestamp)
                else:
                    result = await conn.execute("""
                        DELETE FROM feature_values 
                        WHERE entity_id = $1 AND feature_name = $2
                    """, entity_id, feature_name)
            
            # Invalidate cache
            cache_key = self._get_cache_key(entity_id, feature_name)
            await self._redis.delete(cache_key)
            
            deleted_count = int(result.split()[-1])
            self.logger.info(
                "Feature values deleted",
                entity_id=entity_id,
                feature=feature_name,
                count=deleted_count
            )
            
            return deleted_count
            
        except Exception as e:
            self.logger.error(
                "Failed to delete feature values",
                entity_id=entity_id,
                feature=feature_name,
                error=str(e)
            )
            raise FeatureStoreError(f"Feature deletion failed: {e}")

    async def get_feature_metadata(self, feature_name: str) -> Optional[FeatureMetadata]:
        """Get feature metadata"""
        return self._feature_metadata.get(feature_name)

    async def list_features(self, tags: Optional[List[str]] = None) -> List[FeatureMetadata]:
        """List all features, optionally filtered by tags"""
        features = list(self._feature_metadata.values())
        
        if tags:
            filtered_features = []
            for feature in features:
                if any(tag in feature.tags for tag in tags):
                    filtered_features.append(feature)
            return filtered_features
        
        return features

    async def get_feature_statistics(
        self, 
        feature_name: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Get feature usage statistics"""
        try:
            async with self._pg_pool.acquire() as conn:
                base_query = """
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT(DISTINCT entity_id) as unique_entities,
                        MIN(timestamp) as earliest_timestamp,
                        MAX(timestamp) as latest_timestamp
                    FROM feature_values 
                    WHERE feature_name = $1
                """
                params = [feature_name]
                
                if start_time:
                    base_query += " AND timestamp >= $" + str(len(params) + 1)
                    params.append(start_time)
                
                if end_time:
                    base_query += " AND timestamp <= $" + str(len(params) + 1)
                    params.append(end_time)
                
                stats = await conn.fetchrow(base_query, *params)
                
                return {
                    "feature_name": feature_name,
                    "total_records": stats["total_records"],
                    "unique_entities": stats["unique_entities"],
                    "earliest_timestamp": stats["earliest_timestamp"],
                    "latest_timestamp": stats["latest_timestamp"],
                    "query_start_time": start_time,
                    "query_end_time": end_time
                }
                
        except Exception as e:
            self.logger.error(
                "Failed to get feature statistics",
                feature=feature_name,
                error=str(e)
            )
            raise FeatureStoreError(f"Statistics query failed: {e}")

    # Private methods

    def _get_cache_key(self, entity_id: str, feature_name: str) -> str:
        """Generate Redis cache key"""
        return f"feature:{feature_name}:entity:{entity_id}"

    async def _write_through(self, feature_value: FeatureValue) -> None:
        """Write to both cache and database simultaneously"""
        # Write to database first for consistency
        await self._write_to_database(feature_value)
        # Then update cache
        await self._cache_feature(feature_value)

    async def _write_back(self, feature_value: FeatureValue) -> None:
        """Write to cache immediately, database later"""
        await self._cache_feature(feature_value)
        # Mark for later database write (simplified implementation)
        await self._write_to_database(feature_value)

    async def _write_database_only(self, feature_value: FeatureValue) -> None:
        """Write only to database"""
        await self._write_to_database(feature_value)

    async def _write_to_database(self, feature_value: FeatureValue) -> None:
        """Write feature value to PostgreSQL"""
        async with self._pg_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO feature_values 
                (entity_id, feature_name, value, feature_type, timestamp, version)
                VALUES ($1, $2, $3, $4, $5, $6)
            """, 
                feature_value.entity_id,
                feature_value.feature_name,
                json.dumps(feature_value.value),
                feature_value.feature_type.value,
                feature_value.timestamp,
                feature_value.version
            )

    async def _cache_feature(self, feature_value: FeatureValue) -> None:
        """Cache feature value in Redis"""
        cache_key = self._get_cache_key(feature_value.entity_id, feature_value.feature_name)
        cache_value = json.dumps(feature_value.dict())
        
        metadata = self._feature_metadata[feature_value.feature_name]
        if metadata.ttl_seconds:
            await self._redis.setex(cache_key, metadata.ttl_seconds, cache_value)
        else:
            await self._redis.set(cache_key, cache_value)

    async def _read_from_database(
        self, 
        entity_id: str, 
        feature_name: str,
        timestamp: Optional[datetime] = None
    ) -> Optional[FeatureValue]:
        """Read feature value from PostgreSQL"""
        async with self._pg_pool.acquire() as conn:
            if timestamp:
                row = await conn.fetchrow("""
                    SELECT * FROM feature_values 
                    WHERE entity_id = $1 AND feature_name = $2 AND timestamp <= $3
                    ORDER BY timestamp DESC LIMIT 1
                """, entity_id, feature_name, timestamp)
            else:
                row = await conn.fetchrow("""
                    SELECT * FROM feature_values 
                    WHERE entity_id = $1 AND feature_name = $2
                    ORDER BY timestamp DESC LIMIT 1
                """, entity_id, feature_name)
            
            if not row:
                return None
            
            return FeatureValue(
                entity_id=row['entity_id'],
                feature_name=row['feature_name'],
                value=json.loads(row['value']),
                feature_type=FeatureType(row['feature_type']),
                timestamp=row['timestamp'],
                version=row['version']
            )

    async def _read_features_from_database(
        self, 
        entity_id: str, 
        feature_names: List[str],
        timestamp: Optional[datetime] = None
    ) -> Dict[str, Optional[FeatureValue]]:
        """Read multiple features from database efficiently"""
        results = {name: None for name in feature_names}
        
        async with self._pg_pool.acquire() as conn:
            if timestamp:
                rows = await conn.fetch("""
                    SELECT DISTINCT ON (feature_name) *
                    FROM feature_values 
                    WHERE entity_id = $1 AND feature_name = ANY($2) AND timestamp <= $3
                    ORDER BY feature_name, timestamp DESC
                """, entity_id, feature_names, timestamp)
            else:
                rows = await conn.fetch("""
                    SELECT DISTINCT ON (feature_name) *
                    FROM feature_values 
                    WHERE entity_id = $1 AND feature_name = ANY($2)
                    ORDER BY feature_name, timestamp DESC
                """, entity_id, feature_names)
            
            for row in rows:
                feature_value = FeatureValue(
                    entity_id=row['entity_id'],
                    feature_name=row['feature_name'],
                    value=json.loads(row['value']),
                    feature_type=FeatureType(row['feature_type']),
                    timestamp=row['timestamp'],
                    version=row['version']
                )
                results[row['feature_name']] = feature_value
        
        return results

    async def _invalidate_feature_cache(self, feature_name: str) -> None:
        """Invalidate all cache entries for a feature"""
        pattern = f"feature:{feature_name}:entity:*"
        keys = await self._redis.keys(pattern)
        if keys:
            await self._redis.delete(*keys)

    async def _consistency_checker(self) -> None:
        """Background task to check cache-database consistency"""
        while True:
            try:
                await asyncio.sleep(self.consistency_check_interval)
                
                # Sample random cache keys and verify against database
                sample_keys = await self._redis.randomkey()
                if sample_keys:
                    # Parse key to extract entity_id and feature_name
                    if sample_keys.startswith("feature:"):
                        parts = sample_keys.split(":")
                        if len(parts) == 4:
                            feature_name, entity_id = parts[1], parts[3]
                            await self._verify_consistency(entity_id, feature_name)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("Consistency check failed", error=str(e))

    async def _verify_consistency(self, entity_id: str, feature_name: str) -> None:
        """Verify cache-database consistency for a specific feature"""
        try:
            cache_value = await self.read_feature(entity_id, feature_name)
            db_value = await self._read_from_database(entity_id, feature_name)
            
            if cache_value and db_value:
                if (cache_value.value != db_value.value or 
                    cache_value.timestamp != db_value.timestamp):
                    self.logger.warning(
                        "Data inconsistency detected",
                        entity_id=entity_id,
                        feature=feature_name
                    )
                    # Re-cache the correct value
                    await self._cache_feature(db_value)
                    
        except Exception as e:
            self.logger.error(
                "Consistency verification failed",
                entity_id=entity_id,
                feature=feature_name,
                error=str(e)
            )