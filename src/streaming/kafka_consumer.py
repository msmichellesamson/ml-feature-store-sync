import asyncio
import json
from typing import Dict, Any, Optional, List, Set
from dataclasses import dataclass
from contextlib import asynccontextmanager

import structlog
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
import redis.asyncio as redis
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from pydantic import BaseModel, ValidationError

from ..core.feature_store import FeatureStore, FeatureNotFoundError
from ..core.sync_engine import SyncEngine, SyncError


logger = structlog.get_logger()


class KafkaConsumerError(Exception):
    """Base exception for Kafka consumer operations."""
    pass


class MessageValidationError(KafkaConsumerError):
    """Raised when message validation fails."""
    pass


class ProcessingError(KafkaConsumerError):
    """Raised when message processing fails."""
    pass


@dataclass
class ConsumerConfig:
    """Configuration for Kafka consumer."""
    bootstrap_servers: List[str]
    topic: str
    group_id: str
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False
    max_poll_records: int = 100
    session_timeout_ms: int = 30000
    heartbeat_interval_ms: int = 10000
    consumer_timeout_ms: int = 1000


class FeatureUpdateMessage(BaseModel):
    """Schema for feature update messages."""
    feature_id: str
    feature_group: str
    entity_id: str
    feature_values: Dict[str, Any]
    timestamp: int
    version: str
    source: str
    
    class Config:
        extra = "forbid"


class BatchUpdateMessage(BaseModel):
    """Schema for batch feature update messages."""
    batch_id: str
    feature_group: str
    updates: List[Dict[str, Any]]
    timestamp: int
    version: str
    source: str
    
    class Config:
        extra = "forbid"


class KafkaFeatureConsumer:
    """High-performance Kafka consumer for feature store updates."""
    
    def __init__(
        self,
        config: ConsumerConfig,
        feature_store: FeatureStore,
        sync_engine: SyncEngine,
        redis_client: redis.Redis,
        db_session_factory: sessionmaker
    ) -> None:
        self.config = config
        self.feature_store = feature_store
        self.sync_engine = sync_engine
        self.redis_client = redis_client
        self.db_session_factory = db_session_factory
        
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._running = False
        self._processed_count = 0
        self._error_count = 0
        self._last_processed_timestamp = 0
        self._duplicate_filter: Set[str] = set()
        
        # Circuit breaker state
        self._circuit_breaker_failures = 0
        self._circuit_breaker_threshold = 5
        self._circuit_breaker_open = False
        self._last_failure_time = 0
        self._circuit_breaker_timeout = 60  # seconds
        
        self.logger = logger.bind(component="kafka_consumer", topic=config.topic)

    async def start(self) -> None:
        """Start the Kafka consumer."""
        if self._running:
            raise KafkaConsumerError("Consumer is already running")
        
        try:
            self._consumer = AIOKafkaConsumer(
                self.config.topic,
                bootstrap_servers=self.config.bootstrap_servers,
                group_id=self.config.group_id,
                auto_offset_reset=self.config.auto_offset_reset,
                enable_auto_commit=self.config.enable_auto_commit,
                max_poll_records=self.config.max_poll_records,
                session_timeout_ms=self.config.session_timeout_ms,
                heartbeat_interval_ms=self.config.heartbeat_interval_ms,
                consumer_timeout_ms=self.config.consumer_timeout_ms,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            
            await self._consumer.start()
            self._running = True
            
            self.logger.info(
                "kafka_consumer_started",
                topic=self.config.topic,
                group_id=self.config.group_id
            )
            
        except Exception as e:
            self.logger.error("failed_to_start_consumer", error=str(e))
            raise KafkaConsumerError(f"Failed to start consumer: {e}")

    async def stop(self) -> None:
        """Stop the Kafka consumer."""
        if not self._running:
            return
        
        self._running = False
        
        if self._consumer:
            try:
                await self._consumer.stop()
                self.logger.info("kafka_consumer_stopped")
            except Exception as e:
                self.logger.error("error_stopping_consumer", error=str(e))

    async def consume_messages(self) -> None:
        """Main message consumption loop."""
        if not self._running or not self._consumer:
            raise KafkaConsumerError("Consumer not started")
        
        self.logger.info("starting_message_consumption")
        
        try:
            async for msg_batch in self._consumer:
                if not self._running:
                    break
                
                if self._circuit_breaker_open:
                    if await self._check_circuit_breaker():
                        continue
                
                try:
                    await self._process_message_batch([msg_batch])
                    await self._commit_offsets()
                    
                except Exception as e:
                    await self._handle_processing_error(e)
                    
        except KafkaError as e:
            self.logger.error("kafka_error_in_consumption", error=str(e))
            raise KafkaConsumerError(f"Kafka error: {e}")

    async def _process_message_batch(self, messages: List[Any]) -> None:
        """Process a batch of messages efficiently."""
        batch_start_time = asyncio.get_event_loop().time()
        
        for msg in messages:
            try:
                await self._process_single_message(msg)
                self._processed_count += 1
                
            except Exception as e:
                self._error_count += 1
                self.logger.error(
                    "message_processing_error",
                    partition=msg.partition,
                    offset=msg.offset,
                    error=str(e)
                )
                # Continue processing other messages in batch
                continue
        
        batch_duration = asyncio.get_event_loop().time() - batch_start_time
        
        self.logger.info(
            "batch_processed",
            batch_size=len(messages),
            duration_ms=batch_duration * 1000,
            processed_count=self._processed_count,
            error_count=self._error_count
        )

    async def _process_single_message(self, msg: Any) -> None:
        """Process a single Kafka message."""
        try:
            # Parse message type and validate
            message_data = msg.value
            message_type = message_data.get("type", "feature_update")
            
            # Duplicate detection
            message_id = self._generate_message_id(msg)
            if message_id in self._duplicate_filter:
                self.logger.debug("duplicate_message_skipped", message_id=message_id)
                return
            
            self._duplicate_filter.add(message_id)
            # Keep filter size bounded
            if len(self._duplicate_filter) > 10000:
                self._duplicate_filter.clear()
            
            if message_type == "feature_update":
                await self._handle_feature_update(message_data)
            elif message_type == "batch_update":
                await self._handle_batch_update(message_data)
            else:
                raise MessageValidationError(f"Unknown message type: {message_type}")
            
            self._last_processed_timestamp = msg.timestamp
            self._reset_circuit_breaker()
            
        except ValidationError as e:
            raise MessageValidationError(f"Message validation failed: {e}")
        except Exception as e:
            raise ProcessingError(f"Failed to process message: {e}")

    async def _handle_feature_update(self, message_data: Dict[str, Any]) -> None:
        """Handle single feature update message."""
        try:
            update_msg = FeatureUpdateMessage(**message_data)
            
            async with self.db_session_factory() as session:
                # Update feature store
                await self.feature_store.upsert_feature(
                    session=session,
                    feature_id=update_msg.feature_id,
                    feature_group=update_msg.feature_group,
                    entity_id=update_msg.entity_id,
                    feature_values=update_msg.feature_values,
                    timestamp=update_msg.timestamp,
                    version=update_msg.version
                )
                
                # Update Redis cache
                cache_key = f"feature:{update_msg.feature_group}:{update_msg.entity_id}"
                await self.redis_client.hset(
                    cache_key,
                    mapping={
                        "values": json.dumps(update_msg.feature_values),
                        "timestamp": str(update_msg.timestamp),
                        "version": update_msg.version
                    }
                )
                await self.redis_client.expire(cache_key, 3600)  # 1 hour TTL
                
                # Trigger sync if needed
                await self.sync_engine.check_sync_triggers(
                    session=session,
                    feature_group=update_msg.feature_group,
                    entity_id=update_msg.entity_id
                )
                
                await session.commit()
            
            self.logger.debug(
                "feature_update_processed",
                feature_id=update_msg.feature_id,
                feature_group=update_msg.feature_group,
                entity_id=update_msg.entity_id
            )
            
        except Exception as e:
            raise ProcessingError(f"Failed to handle feature update: {e}")

    async def _handle_batch_update(self, message_data: Dict[str, Any]) -> None:
        """Handle batch feature update message."""
        try:
            batch_msg = BatchUpdateMessage(**message_data)
            
            async with self.db_session_factory() as session:
                # Process batch updates
                for update_data in batch_msg.updates:
                    await self.feature_store.upsert_feature(
                        session=session,
                        feature_id=update_data["feature_id"],
                        feature_group=batch_msg.feature_group,
                        entity_id=update_data["entity_id"],
                        feature_values=update_data["feature_values"],
                        timestamp=batch_msg.timestamp,
                        version=batch_msg.version
                    )
                    
                    # Update Redis cache
                    cache_key = f"feature:{batch_msg.feature_group}:{update_data['entity_id']}"
                    await self.redis_client.hset(
                        cache_key,
                        mapping={
                            "values": json.dumps(update_data["feature_values"]),
                            "timestamp": str(batch_msg.timestamp),
                            "version": batch_msg.version
                        }
                    )
                    await self.redis_client.expire(cache_key, 3600)
                
                # Trigger batch sync
                await self.sync_engine.trigger_batch_sync(
                    session=session,
                    feature_group=batch_msg.feature_group,
                    batch_id=batch_msg.batch_id
                )
                
                await session.commit()
            
            self.logger.info(
                "batch_update_processed",
                batch_id=batch_msg.batch_id,
                feature_group=batch_msg.feature_group,
                update_count=len(batch_msg.updates)
            )
            
        except Exception as e:
            raise ProcessingError(f"Failed to handle batch update: {e}")

    def _generate_message_id(self, msg: Any) -> str:
        """Generate unique message ID for duplicate detection."""
        return f"{msg.partition}:{msg.offset}"

    async def _commit_offsets(self) -> None:
        """Manually commit Kafka offsets."""
        if self._consumer and not self.config.enable_auto_commit:
            try:
                await self._consumer.commit()
            except KafkaError as e:
                self.logger.error("offset_commit_failed", error=str(e))
                raise

    async def _handle_processing_error(self, error: Exception) -> None:
        """Handle processing errors with circuit breaker logic."""
        self._circuit_breaker_failures += 1
        self._last_failure_time = asyncio.get_event_loop().time()
        
        if self._circuit_breaker_failures >= self._circuit_breaker_threshold:
            self._circuit_breaker_open = True
            self.logger.error(
                "circuit_breaker_opened",
                failures=self._circuit_breaker_failures,
                error=str(error)
            )
        
        # Don't re-raise to continue processing other messages
        self.logger.error("processing_error_handled", error=str(error))

    async def _check_circuit_breaker(self) -> bool:
        """Check if circuit breaker should be closed."""
        current_time = asyncio.get_event_loop().time()
        
        if current_time - self._last_failure_time > self._circuit_breaker_timeout:
            self._circuit_breaker_open = False
            self._circuit_breaker_failures = 0
            self.logger.info("circuit_breaker_closed")
            return False
        
        return True

    def _reset_circuit_breaker(self) -> None:
        """Reset circuit breaker on successful processing."""
        if self._circuit_breaker_failures > 0:
            self._circuit_breaker_failures = 0

    async def get_consumer_metrics(self) -> Dict[str, Any]:
        """Get consumer performance metrics."""
        return {
            "processed_count": self._processed_count,
            "error_count": self._error_count,
            "error_rate": self._error_count / max(self._processed_count, 1),
            "last_processed_timestamp": self._last_processed_timestamp,
            "circuit_breaker_open": self._circuit_breaker_open,
            "circuit_breaker_failures": self._circuit_breaker_failures,
            "running": self._running
        }

    @asynccontextmanager
    async def consumer_context(self):
        """Context manager for consumer lifecycle."""
        try:
            await self.start()
            yield self
        finally:
            await self.stop()


async def create_kafka_consumer(
    config: ConsumerConfig,
    feature_store: FeatureStore,
    sync_engine: SyncEngine,
    redis_client: redis.Redis,
    db_session_factory: sessionmaker
) -> KafkaFeatureConsumer:
    """Factory function to create configured Kafka consumer."""
    return KafkaFeatureConsumer(
        config=config,
        feature_store=feature_store,
        sync_engine=sync_engine,
        redis_client=redis_client,
        db_session_factory=db_session_factory
    )