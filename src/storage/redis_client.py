import redis
import logging
import time
from typing import Optional, Any, Dict, List
from dataclasses import dataclass
from contextlib import contextmanager

logger = logging.getLogger(__name__)

@dataclass
class RedisConfig:
    host: str = 'localhost'
    port: int = 6379
    password: Optional[str] = None
    db: int = 0
    max_connections: int = 20
    socket_timeout: int = 5
    socket_connect_timeout: int = 5
    retry_on_timeout: bool = True
    max_retries: int = 3
    retry_delay: float = 0.1

class RedisClient:
    def __init__(self, config: RedisConfig):
        self.config = config
        self.pool = redis.ConnectionPool(
            host=config.host,
            port=config.port,
            password=config.password,
            db=config.db,
            max_connections=config.max_connections,
            socket_timeout=config.socket_timeout,
            socket_connect_timeout=config.socket_connect_timeout,
            retry_on_timeout=config.retry_on_timeout
        )
        self.redis = redis.Redis(connection_pool=self.pool)
        logger.info(f"Redis client initialized with pool size: {config.max_connections}")

    def _retry_operation(self, operation, *args, **kwargs) -> Any:
        """Execute operation with retry logic"""
        last_exception = None
        
        for attempt in range(self.config.max_retries + 1):
            try:
                return operation(*args, **kwargs)
            except (redis.ConnectionError, redis.TimeoutError) as e:
                last_exception = e
                if attempt < self.config.max_retries:
                    wait_time = self.config.retry_delay * (2 ** attempt)
                    logger.warning(f"Redis operation failed (attempt {attempt + 1}), retrying in {wait_time}s: {e}")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Redis operation failed after {self.config.max_retries + 1} attempts: {e}")
                    
        raise last_exception

    def get_features(self, feature_keys: List[str]) -> Dict[str, Optional[str]]:
        """Get multiple features with retry logic"""
        try:
            return self._retry_operation(self.redis.mget, feature_keys)
        except Exception as e:
            logger.error(f"Failed to get features {feature_keys}: {e}")
            return {key: None for key in feature_keys}

    def set_feature(self, key: str, value: str, ttl: Optional[int] = None) -> bool:
        """Set feature with TTL and retry logic"""
        try:
            result = self._retry_operation(self.redis.set, key, value, ex=ttl)
            return bool(result)
        except Exception as e:
            logger.error(f"Failed to set feature {key}: {e}")
            return False

    @contextmanager
    def pipeline(self):
        """Context manager for Redis pipeline with error handling"""
        pipe = None
        try:
            pipe = self.redis.pipeline()
            yield pipe
            self._retry_operation(pipe.execute)
        except Exception as e:
            logger.error(f"Pipeline operation failed: {e}")
            if pipe:
                pipe.reset()
            raise

    def health_check(self) -> bool:
        """Check Redis connection health"""
        try:
            self.redis.ping()
            return True
        except Exception as e:
            logger.error(f"Redis health check failed: {e}")
            return False

    def close(self):
        """Close connection pool"""
        try:
            self.pool.disconnect()
            logger.info("Redis connection pool closed")
        except Exception as e:
            logger.error(f"Error closing Redis pool: {e}")
