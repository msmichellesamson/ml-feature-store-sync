import redis
import logging
import time
from typing import Optional, Dict, Any, List
from redis.connection import ConnectionPool
from redis.retry import Retry
from redis.backoff import ExponentialBackoff

logger = logging.getLogger(__name__)

class RedisClient:
    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0, 
                 max_connections: int = 20, retry_attempts: int = 3):
        self.pool = ConnectionPool(
            host=host, 
            port=port, 
            db=db,
            max_connections=max_connections,
            retry_on_timeout=True,
            socket_keepalive=True,
            socket_keepalive_options={}
        )
        
        self.retry = Retry(
            ExponentialBackoff(),
            retries=retry_attempts
        )
        
        self.client = redis.Redis(
            connection_pool=self.pool,
            retry=self.retry
        )
    
    def set_features(self, feature_key: str, features: Dict[str, Any], ttl: int = 3600) -> bool:
        try:
            pipe = self.client.pipeline()
            pipe.hmset(feature_key, features)
            pipe.expire(feature_key, ttl)
            pipe.execute()
            logger.info(f"Features cached for key: {feature_key}")
            return True
        except redis.RedisError as e:
            logger.error(f"Failed to cache features for {feature_key}: {e}")
            return False
    
    def get_features(self, feature_key: str) -> Optional[Dict[str, str]]:
        try:
            features = self.client.hgetall(feature_key)
            if features:
                return {k.decode(): v.decode() for k, v in features.items()}
            return None
        except redis.RedisError as e:
            logger.error(f"Failed to retrieve features for {feature_key}: {e}")
            return None
    
    def delete_features(self, feature_key: str) -> bool:
        try:
            result = self.client.delete(feature_key)
            return result > 0
        except redis.RedisError as e:
            logger.error(f"Failed to delete features for {feature_key}: {e}")
            return False
    
    def health_check(self) -> bool:
        try:
            self.client.ping()
            return True
        except redis.RedisError:
            return False
    
    def close(self):
        if hasattr(self, 'pool'):
            self.pool.disconnect()