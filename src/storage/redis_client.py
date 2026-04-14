import asyncio
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from contextlib import asynccontextmanager

import redis.asyncio as redis
from redis.exceptions import RedisError, ConnectionError, TimeoutError

logger = logging.getLogger(__name__)

class CircuitBreaker:
    """Simple circuit breaker for Redis operations"""
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 30):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
    
    def can_execute(self) -> bool:
        if self.state == "CLOSED":
            return True
        
        if self.state == "OPEN":
            if self.last_failure_time and \
               datetime.now() - self.last_failure_time > timedelta(seconds=self.recovery_timeout):
                self.state = "HALF_OPEN"
                return True
            return False
        
        return True  # HALF_OPEN
    
    def record_success(self):
        self.failure_count = 0
        self.state = "CLOSED"
    
    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        
        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"

class RedisClient:
    def __init__(self, redis_url: str, max_connections: int = 20):
        self.redis_url = redis_url
        self.pool: Optional[redis.ConnectionPool] = None
        self.client: Optional[redis.Redis] = None
        self.circuit_breaker = CircuitBreaker()
        self.max_connections = max_connections
    
    async def connect(self):
        """Initialize connection pool"""
        try:
            self.pool = redis.ConnectionPool.from_url(
                self.redis_url,
                max_connections=self.max_connections,
                retry_on_timeout=True,
                socket_keepalive=True,
                socket_keepalive_options={}
            )
            self.client = redis.Redis(connection_pool=self.pool)
            
            # Test connection
            await self.client.ping()
            logger.info("Redis connection pool established")
            
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    @asynccontextmanager
    async def _execute_with_circuit_breaker(self):
        """Execute Redis operation with circuit breaker"""
        if not self.circuit_breaker.can_execute():
            raise ConnectionError("Circuit breaker is OPEN")
        
        try:
            yield
            self.circuit_breaker.record_success()
        except (RedisError, TimeoutError, ConnectionError) as e:
            self.circuit_breaker.record_failure()
            logger.error(f"Redis operation failed: {e}")
            raise
    
    async def get_feature(self, key: str) -> Optional[Dict[str, Any]]:
        """Get feature from Redis with circuit breaker"""
        async with self._execute_with_circuit_breaker():
            data = await self.client.get(key)
            return eval(data.decode()) if data else None
    
    async def set_feature(self, key: str, value: Dict[str, Any], ttl: int = 3600):
        """Set feature in Redis with TTL"""
        async with self._execute_with_circuit_breaker():
            await self.client.setex(key, ttl, str(value))
    
    async def batch_get(self, keys: List[str]) -> Dict[str, Optional[Dict[str, Any]]]:
        """Batch get features"""
        async with self._execute_with_circuit_breaker():
            pipe = self.client.pipeline()
            for key in keys:
                pipe.get(key)
            
            results = await pipe.execute()
            return {
                key: eval(result.decode()) if result else None
                for key, result in zip(keys, results)
            }
    
    async def delete_pattern(self, pattern: str) -> int:
        """Delete keys matching pattern"""
        async with self._execute_with_circuit_breaker():
            keys = await self.client.keys(pattern)
            if keys:
                return await self.client.delete(*keys)
            return 0
    
    async def close(self):
        """Close connection pool"""
        if self.client:
            await self.client.close()
        if self.pool:
            await self.pool.disconnect()
        logger.info("Redis connection pool closed")
