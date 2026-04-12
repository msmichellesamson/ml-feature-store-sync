import logging
import time
from typing import Optional, Any, Dict, List
from redis import Redis, ConnectionPool, RedisError
from redis.retry import Retry
from redis.backoff import ExponentialBackoff

logger = logging.getLogger(__name__)

class RedisClient:
    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0, max_connections: int = 20):
        self.pool = ConnectionPool(
            host=host,
            port=port,
            db=db,
            max_connections=max_connections,
            retry_on_timeout=True,
            socket_connect_timeout=5,
            socket_timeout=5
        )
        
        self.client = Redis(
            connection_pool=self.pool,
            retry=Retry(ExponentialBackoff(), retries=3),
            retry_on_error=[ConnectionError, TimeoutError]
        )
        
    def get_feature(self, key: str) -> Optional[Dict[str, Any]]:
        try:
            data = self.client.hgetall(key)
            if not data:
                return None
            return {k.decode(): v.decode() for k, v in data.items()}
        except RedisError as e:
            logger.error(f"Failed to get feature {key}: {e}")
            raise
    
    def set_feature(self, key: str, features: Dict[str, Any], ttl: int = 3600) -> bool:
        try:
            pipe = self.client.pipeline()
            pipe.hset(key, mapping={k: str(v) for k, v in features.items()})
            pipe.expire(key, ttl)
            pipe.execute()
            return True
        except RedisError as e:
            logger.error(f"Failed to set feature {key}: {e}")
            return False
    
    def batch_get(self, keys: List[str]) -> Dict[str, Optional[Dict[str, Any]]]:
        try:
            pipe = self.client.pipeline()
            for key in keys:
                pipe.hgetall(key)
            results = pipe.execute()
            
            return {
                key: {k.decode(): v.decode() for k, v in result.items()} if result else None
                for key, result in zip(keys, results)
            }
        except RedisError as e:
            logger.error(f"Failed batch get for keys {keys}: {e}")
            return {key: None for key in keys}
    
    def health_check(self) -> bool:
        try:
            self.client.ping()
            return True
        except RedisError:
            return False
    
    def get_connection_info(self) -> Dict[str, Any]:
        try:
            info = self.client.info()
            return {
                "connected_clients": info.get("connected_clients", 0),
                "used_memory": info.get("used_memory_human", "0B"),
                "uptime_seconds": info.get("uptime_in_seconds", 0)
            }
        except RedisError as e:
            logger.error(f"Failed to get connection info: {e}")
            return {}