"""
Redis client for ML feature store caching layer.
Handles feature caching, TTL management, and batch operations.
"""

import asyncio
import json
import time
from typing import Any, Dict, List, Optional, Union, Set
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta

import redis.asyncio as redis
import structlog
from pydantic import BaseModel

from src.core.exceptions import CacheError, FeatureNotFoundError

logger = structlog.get_logger(__name__)


@dataclass
class CacheStats:
    """Cache performance statistics."""
    hits: int = 0
    misses: int = 0
    errors: int = 0
    total_requests: int = 0
    avg_response_time_ms: float = 0.0
    
    @property
    def hit_rate(self) -> float:
        """Calculate cache hit rate."""
        if self.total_requests == 0:
            return 0.0
        return self.hits / self.total_requests


class FeatureCacheEntry(BaseModel):
    """Cache entry for ML features."""
    feature_name: str
    feature_value: Union[float, int, str, List[float]]
    entity_id: str
    timestamp: float
    ttl_seconds: int
    metadata: Optional[Dict[str, Any]] = None
    
    def is_expired(self) -> bool:
        """Check if cache entry is expired."""
        return time.time() > (self.timestamp + self.ttl_seconds)
    
    def to_cache_key(self) -> str:
        """Generate Redis cache key."""
        return f"feature:{self.entity_id}:{self.feature_name}"


class RedisClient:
    """
    Production Redis client for ML feature store caching.
    Handles connection pooling, retries, and performance monitoring.
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        max_connections: int = 20,
        retry_attempts: int = 3,
        timeout_seconds: float = 5.0,
        default_ttl: int = 3600
    ):
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.max_connections = max_connections
        self.retry_attempts = retry_attempts
        self.timeout_seconds = timeout_seconds
        self.default_ttl = default_ttl
        
        self._client: Optional[redis.Redis] = None
        self._stats = CacheStats()
        self._connection_pool: Optional[redis.ConnectionPool] = None
        
        self.logger = logger.bind(
            redis_host=host,
            redis_port=port,
            redis_db=db
        )
    
    async def connect(self) -> None:
        """Initialize Redis connection pool."""
        try:
            self._connection_pool = redis.ConnectionPool(
                host=self.host,
                port=self.port,
                db=self.db,
                password=self.password,
                max_connections=self.max_connections,
                socket_timeout=self.timeout_seconds,
                socket_connect_timeout=self.timeout_seconds,
                retry_on_timeout=True,
                decode_responses=True
            )
            
            self._client = redis.Redis(connection_pool=self._connection_pool)
            
            # Test connection
            await self._client.ping()
            
            self.logger.info("Redis connection established")
            
        except Exception as e:
            self.logger.error("Failed to connect to Redis", error=str(e))
            raise CacheError(f"Redis connection failed: {e}")
    
    async def disconnect(self) -> None:
        """Close Redis connection."""
        if self._client:
            await self._client.aclose()
        if self._connection_pool:
            await self._connection_pool.aclose()
        
        self.logger.info("Redis connection closed")
    
    async def health_check(self) -> bool:
        """Check Redis connection health."""
        try:
            if not self._client:
                return False
            
            start_time = time.time()
            await self._client.ping()
            response_time = (time.time() - start_time) * 1000
            
            self.logger.debug("Redis health check passed", response_time_ms=response_time)
            return True
            
        except Exception as e:
            self.logger.error("Redis health check failed", error=str(e))
            return False
    
    async def get_feature(
        self,
        entity_id: str,
        feature_name: str,
        check_expiry: bool = True
    ) -> Optional[FeatureCacheEntry]:
        """
        Get single feature from cache.
        
        Args:
            entity_id: Entity identifier
            feature_name: Feature name
            check_expiry: Whether to check TTL expiration
            
        Returns:
            Feature cache entry or None if not found/expired
        """
        start_time = time.time()
        cache_key = f"feature:{entity_id}:{feature_name}"
        
        try:
            self._stats.total_requests += 1
            
            cached_data = await self._with_retry(
                self._client.get, cache_key
            )
            
            if not cached_data:
                self._stats.misses += 1
                self.logger.debug("Cache miss", entity_id=entity_id, feature=feature_name)
                return None
            
            entry = FeatureCacheEntry.parse_raw(cached_data)
            
            if check_expiry and entry.is_expired():
                self._stats.misses += 1
                await self._client.delete(cache_key)
                self.logger.debug("Cache entry expired", entity_id=entity_id, feature=feature_name)
                return None
            
            self._stats.hits += 1
            response_time = (time.time() - start_time) * 1000
            self._update_avg_response_time(response_time)
            
            self.logger.debug(
                "Cache hit",
                entity_id=entity_id,
                feature=feature_name,
                response_time_ms=response_time
            )
            
            return entry
            
        except Exception as e:
            self._stats.errors += 1
            self.logger.error(
                "Failed to get feature from cache",
                entity_id=entity_id,
                feature=feature_name,
                error=str(e)
            )
            return None
    
    async def get_features_batch(
        self,
        entity_id: str,
        feature_names: List[str],
        check_expiry: bool = True
    ) -> Dict[str, FeatureCacheEntry]:
        """
        Get multiple features for an entity in batch.
        
        Args:
            entity_id: Entity identifier
            feature_names: List of feature names
            check_expiry: Whether to check TTL expiration
            
        Returns:
            Dictionary of feature_name -> FeatureCacheEntry
        """
        if not feature_names:
            return {}
        
        start_time = time.time()
        cache_keys = [f"feature:{entity_id}:{name}" for name in feature_names]
        result = {}
        
        try:
            self._stats.total_requests += len(feature_names)
            
            # Use pipeline for batch operations
            async with self._client.pipeline(transaction=False) as pipe:
                for key in cache_keys:
                    pipe.get(key)
                cached_values = await pipe.execute()
            
            for i, (feature_name, cached_data) in enumerate(zip(feature_names, cached_values)):
                if not cached_data:
                    self._stats.misses += 1
                    continue
                
                try:
                    entry = FeatureCacheEntry.parse_raw(cached_data)
                    
                    if check_expiry and entry.is_expired():
                        self._stats.misses += 1
                        # Clean up expired entry
                        await self._client.delete(cache_keys[i])
                        continue
                    
                    result[feature_name] = entry
                    self._stats.hits += 1
                    
                except Exception as e:
                    self.logger.error(
                        "Failed to parse cache entry",
                        feature=feature_name,
                        error=str(e)
                    )
                    self._stats.errors += 1
            
            response_time = (time.time() - start_time) * 1000
            self._update_avg_response_time(response_time)
            
            self.logger.debug(
                "Batch cache lookup completed",
                entity_id=entity_id,
                requested_features=len(feature_names),
                found_features=len(result),
                response_time_ms=response_time
            )
            
            return result
            
        except Exception as e:
            self._stats.errors += len(feature_names)
            self.logger.error(
                "Failed batch feature lookup",
                entity_id=entity_id,
                features=feature_names,
                error=str(e)
            )
            return {}
    
    async def set_feature(
        self,
        entry: FeatureCacheEntry,
        ttl_override: Optional[int] = None
    ) -> bool:
        """
        Set single feature in cache.
        
        Args:
            entry: Feature cache entry
            ttl_override: Optional TTL override in seconds
            
        Returns:
            True if successful, False otherwise
        """
        cache_key = entry.to_cache_key()
        ttl = ttl_override or entry.ttl_seconds or self.default_ttl
        
        try:
            # Update timestamp
            entry.timestamp = time.time()
            
            success = await self._with_retry(
                self._client.setex,
                cache_key,
                ttl,
                entry.json()
            )
            
            if success:
                self.logger.debug(
                    "Feature cached",
                    entity_id=entry.entity_id,
                    feature=entry.feature_name,
                    ttl=ttl
                )
            
            return bool(success)
            
        except Exception as e:
            self.logger.error(
                "Failed to cache feature",
                entity_id=entry.entity_id,
                feature=entry.feature_name,
                error=str(e)
            )
            return False
    
    async def set_features_batch(
        self,
        entries: List[FeatureCacheEntry],
        ttl_override: Optional[int] = None
    ) -> int:
        """
        Set multiple features in batch.
        
        Args:
            entries: List of feature cache entries
            ttl_override: Optional TTL override in seconds
            
        Returns:
            Number of successfully cached features
        """
        if not entries:
            return 0
        
        try:
            current_time = time.time()
            
            # Use pipeline for batch operations
            async with self._client.pipeline(transaction=False) as pipe:
                for entry in entries:
                    entry.timestamp = current_time
                    cache_key = entry.to_cache_key()
                    ttl = ttl_override or entry.ttl_seconds or self.default_ttl
                    
                    pipe.setex(cache_key, ttl, entry.json())
                
                results = await pipe.execute()
            
            success_count = sum(1 for result in results if result)
            
            self.logger.info(
                "Batch cache operation completed",
                total_entries=len(entries),
                successful=success_count
            )
            
            return success_count
            
        except Exception as e:
            self.logger.error("Failed batch cache operation", error=str(e))
            return 0
    
    async def delete_feature(self, entity_id: str, feature_name: str) -> bool:
        """Delete single feature from cache."""
        cache_key = f"feature:{entity_id}:{feature_name}"
        
        try:
            deleted = await self._with_retry(self._client.delete, cache_key)
            
            if deleted:
                self.logger.debug(
                    "Feature deleted from cache",
                    entity_id=entity_id,
                    feature=feature_name
                )
            
            return bool(deleted)
            
        except Exception as e:
            self.logger.error(
                "Failed to delete feature",
                entity_id=entity_id,
                feature=feature_name,
                error=str(e)
            )
            return False
    
    async def delete_entity_features(self, entity_id: str) -> int:
        """Delete all features for an entity."""
        try:
            pattern = f"feature:{entity_id}:*"
            keys = []
            
            # Scan for matching keys
            async for key in self._client.scan_iter(match=pattern, count=100):
                keys.append(key)
            
            if not keys:
                return 0
            
            deleted = await self._client.delete(*keys)
            
            self.logger.info(
                "Deleted entity features",
                entity_id=entity_id,
                deleted_count=deleted
            )
            
            return deleted
            
        except Exception as e:
            self.logger.error(
                "Failed to delete entity features",
                entity_id=entity_id,
                error=str(e)
            )
            return 0
    
    async def get_cache_stats(self) -> CacheStats:
        """Get current cache statistics."""
        return self._stats
    
    async def reset_stats(self) -> None:
        """Reset cache statistics."""
        self._stats = CacheStats()
        self.logger.info("Cache statistics reset")
    
    async def get_memory_usage(self) -> Dict[str, Any]:
        """Get Redis memory usage statistics."""
        try:
            info = await self._client.info("memory")
            
            return {
                "used_memory": info.get("used_memory", 0),
                "used_memory_human": info.get("used_memory_human", "0B"),
                "used_memory_rss": info.get("used_memory_rss", 0),
                "used_memory_peak": info.get("used_memory_peak", 0),
                "used_memory_peak_human": info.get("used_memory_peak_human", "0B"),
                "total_system_memory": info.get("total_system_memory", 0),
                "memory_fragmentation_ratio": info.get("mem_fragmentation_ratio", 0.0)
            }
            
        except Exception as e:
            self.logger.error("Failed to get memory usage", error=str(e))
            return {}
    
    async def expire_features_by_pattern(
        self,
        pattern: str,
        max_keys: int = 1000
    ) -> int:
        """
        Set expiration on features matching pattern.
        
        Args:
            pattern: Redis key pattern (e.g., "feature:user123:*")
            max_keys: Maximum keys to process in one operation
            
        Returns:
            Number of keys that had expiration set
        """
        try:
            keys = []
            count = 0
            
            async for key in self._client.scan_iter(match=pattern, count=100):
                keys.append(key)
                count += 1
                
                if count >= max_keys:
                    break
            
            if not keys:
                return 0
            
            # Set short expiration on matched keys
            async with self._client.pipeline(transaction=False) as pipe:
                for key in keys:
                    pipe.expire(key, 60)  # 1 minute expiration
                
                results = await pipe.execute()
            
            expired_count = sum(1 for result in results if result)
            
            self.logger.info(
                "Set expiration on features",
                pattern=pattern,
                total_keys=len(keys),
                expired_count=expired_count
            )
            
            return expired_count
            
        except Exception as e:
            self.logger.error(
                "Failed to expire features by pattern",
                pattern=pattern,
                error=str(e)
            )
            return 0
    
    def _update_avg_response_time(self, response_time_ms: float) -> None:
        """Update average response time with exponential moving average."""
        if self._stats.avg_response_time_ms == 0:
            self._stats.avg_response_time_ms = response_time_ms
        else:
            # EMA with alpha = 0.1
            alpha = 0.1
            self._stats.avg_response_time_ms = (
                alpha * response_time_ms + 
                (1 - alpha) * self._stats.avg_response_time_ms
            )
    
    async def _with_retry(self, func, *args, **kwargs):
        """Execute Redis operation with retry logic."""
        last_exception = None
        
        for attempt in range(self.retry_attempts):
            try:
                return await func(*args, **kwargs)
                
            except (redis.ConnectionError, redis.TimeoutError) as e:
                last_exception = e
                
                if attempt < self.retry_attempts - 1:
                    wait_time = 2 ** attempt  # Exponential backoff
                    self.logger.warning(
                        "Redis operation failed, retrying",
                        attempt=attempt + 1,
                        wait_time=wait_time,
                        error=str(e)
                    )
                    await asyncio.sleep(wait_time)
                else:
                    self.logger.error(
                        "Redis operation failed after retries",
                        attempts=self.retry_attempts,
                        error=str(e)
                    )
            
            except Exception as e:
                # Don't retry on non-connection errors
                self.logger.error("Redis operation failed", error=str(e))
                raise
        
        raise CacheError(f"Redis operation failed after {self.retry_attempts} attempts: {last_exception}")