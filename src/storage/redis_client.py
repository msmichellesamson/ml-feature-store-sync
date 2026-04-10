import redis
import time
import logging
from typing import Optional, Any, Dict
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

@dataclass
class CircuitBreaker:
    failure_threshold: int = 5
    recovery_timeout: int = 60
    half_open_max_calls: int = 3
    
    def __post_init__(self):
        self.failure_count = 0
        self.last_failure_time = 0
        self.state = CircuitState.CLOSED
        self.half_open_calls = 0

    def call_allowed(self) -> bool:
        if self.state == CircuitState.CLOSED:
            return True
        elif self.state == CircuitState.OPEN:
            if time.time() - self.last_failure_time >= self.recovery_timeout:
                self.state = CircuitState.HALF_OPEN
                self.half_open_calls = 0
                return True
            return False
        else:  # HALF_OPEN
            return self.half_open_calls < self.half_open_max_calls

    def record_success(self):
        if self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.half_open_calls = 0

    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.OPEN
        elif self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN

class RedisClient:
    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0):
        self.client = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        self.circuit_breaker = CircuitBreaker()
        logger.info(f"Redis client initialized: {host}:{port}/{db}")

    def _execute_with_circuit_breaker(self, operation, *args, **kwargs) -> Any:
        if not self.circuit_breaker.call_allowed():
            logger.warning(f"Circuit breaker OPEN - rejecting Redis operation")
            raise redis.ConnectionError("Circuit breaker open")

        try:
            if self.circuit_breaker.state == CircuitState.HALF_OPEN:
                self.circuit_breaker.half_open_calls += 1
                
            result = operation(*args, **kwargs)
            self.circuit_breaker.record_success()
            return result
        except (redis.ConnectionError, redis.TimeoutError) as e:
            self.circuit_breaker.record_failure()
            logger.error(f"Redis operation failed: {e}")
            raise

    def get_features(self, feature_key: str) -> Optional[Dict[str, Any]]:
        """Get feature vector from Redis with circuit breaker protection"""
        try:
            result = self._execute_with_circuit_breaker(self.client.hgetall, feature_key)
            if result:
                logger.debug(f"Retrieved features for key: {feature_key}")
                return {k: float(v) if v.replace('.', '').replace('-', '').isdigit() else v 
                       for k, v in result.items()}
            return None
        except Exception as e:
            logger.error(f"Failed to get features for {feature_key}: {e}")
            return None

    def set_features(self, feature_key: str, features: Dict[str, Any], ttl: int = 3600) -> bool:
        """Set feature vector in Redis with TTL and circuit breaker protection"""
        try:
            pipeline = self.client.pipeline()
            pipeline.hset(feature_key, mapping=features)
            pipeline.expire(feature_key, ttl)
            
            self._execute_with_circuit_breaker(pipeline.execute)
            logger.debug(f"Set features for key: {feature_key} (TTL: {ttl}s)")
            return True
        except Exception as e:
            logger.error(f"Failed to set features for {feature_key}: {e}")
            return False

    def health_check(self) -> bool:
        """Check Redis connectivity without circuit breaker"""
        try:
            self.client.ping()
            return True
        except Exception:
            return False