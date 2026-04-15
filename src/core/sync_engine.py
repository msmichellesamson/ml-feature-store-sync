import asyncio
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from enum import Enum

from ..storage.redis_client import RedisClient
from ..storage.postgres_client import PostgresClient
from ..models.feature_schema import FeatureRecord


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.state = CircuitState.CLOSED
    
    def can_execute(self) -> bool:
        if self.state == CircuitState.CLOSED:
            return True
        elif self.state == CircuitState.OPEN:
            if self._should_attempt_reset():
                self.state = CircuitState.HALF_OPEN
                return True
            return False
        else:  # HALF_OPEN
            return True
    
    def record_success(self):
        self.failure_count = 0
        self.state = CircuitState.CLOSED
    
    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = datetime.utcnow()
        
        if self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN
    
    def _should_attempt_reset(self) -> bool:
        return (
            self.last_failure_time and 
            datetime.utcnow() - self.last_failure_time > timedelta(seconds=self.recovery_timeout)
        )


class SyncEngine:
    def __init__(self, redis_client: RedisClient, postgres_client: PostgresClient):
        self.redis = redis_client
        self.postgres = postgres_client
        self.logger = logging.getLogger(__name__)
        self.circuit_breaker = CircuitBreaker()
        
    async def sync_with_retry(self, features: List[FeatureRecord], max_retries: int = 3) -> bool:
        """Sync features with exponential backoff retry and circuit breaker."""
        if not self.circuit_breaker.can_execute():
            self.logger.warning(f"Circuit breaker is {self.circuit_breaker.state.value}, skipping sync")
            return False
            
        for attempt in range(max_retries):
            try:
                await self._sync_features(features)
                self.circuit_breaker.record_success()
                self.logger.info(f"Successfully synced {len(features)} features")
                return True
                
            except Exception as e:
                wait_time = 2 ** attempt  # Exponential backoff
                self.logger.warning(
                    f"Sync attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s"
                )
                
                if attempt < max_retries - 1:
                    await asyncio.sleep(wait_time)
                else:
                    self.circuit_breaker.record_failure()
                    self.logger.error(f"All sync attempts failed. Circuit breaker triggered.")
                    
        return False
    
    async def _sync_features(self, features: List[FeatureRecord]):
        """Internal sync implementation."""
        # Batch write to PostgreSQL first for durability
        await self.postgres.batch_insert_features(features)
        
        # Then update Redis cache
        for feature in features:
            await self.redis.set_feature(feature.key, feature.value, ttl=3600)
