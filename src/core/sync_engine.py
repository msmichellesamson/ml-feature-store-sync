import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Any, Tuple
from dataclasses import dataclass, asdict
from enum import Enum

import structlog
import redis.asyncio as redis
import asyncpg
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import numpy as np

from src.core.exceptions import (
    SyncEngineError,
    RedisConnectionError,
    PostgreSQLConnectionError,
    KafkaConnectionError,
    FeatureSyncError,
    InvalidFeatureError
)

logger = structlog.get_logger()

class SyncStrategy(Enum):
    """Synchronization strategies for feature data."""
    WRITE_THROUGH = "write_through"
    WRITE_BACK = "write_back"
    WRITE_BEHIND = "write_behind"
    REFRESH_AHEAD = "refresh_ahead"

class SyncStatus(Enum):
    """Status of sync operations."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"

@dataclass
class FeatureSyncEvent:
    """Event representing a feature synchronization operation."""
    feature_group: str
    entity_id: str
    features: Dict[str, Any]
    timestamp: datetime
    operation: str  # insert, update, delete
    sync_id: str
    metadata: Optional[Dict[str, Any]] = None

@dataclass
class SyncMetrics:
    """Metrics for sync operations."""
    total_synced: int = 0
    successful_syncs: int = 0
    failed_syncs: int = 0
    avg_sync_time_ms: float = 0.0
    redis_hits: int = 0
    redis_misses: int = 0
    postgres_operations: int = 0
    kafka_messages_processed: int = 0
    last_sync_timestamp: Optional[datetime] = None

class SyncEngine:
    """
    Production-grade feature store synchronization engine.
    
    Handles real-time synchronization between Redis (cache) and PostgreSQL (persistence)
    with Kafka-based event streaming for distributed feature updates.
    """

    def __init__(
        self,
        redis_url: str,
        postgres_url: str,
        kafka_bootstrap_servers: List[str],
        sync_strategy: SyncStrategy = SyncStrategy.WRITE_THROUGH,
        batch_size: int = 1000,
        sync_interval_seconds: int = 30,
        max_retry_attempts: int = 3,
        circuit_breaker_threshold: int = 5
    ):
        self.redis_url = redis_url
        self.postgres_url = postgres_url
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.sync_strategy = sync_strategy
        self.batch_size = batch_size
        self.sync_interval_seconds = sync_interval_seconds
        self.max_retry_attempts = max_retry_attempts
        self.circuit_breaker_threshold = circuit_breaker_threshold
        
        # Connection objects
        self.redis_client: Optional[redis.Redis] = None
        self.postgres_pool: Optional[asyncpg.Pool] = None
        self.kafka_producer: Optional[KafkaProducer] = None
        self.kafka_consumer: Optional[KafkaConsumer] = None
        
        # Internal state
        self.sync_queue: asyncio.Queue = asyncio.Queue()
        self.metrics = SyncMetrics()
        self.running = False
        self.circuit_breaker_failures: Dict[str, int] = {
            "redis": 0,
            "postgres": 0,
            "kafka": 0
        }
        
        # Feature locks for concurrent access
        self.feature_locks: Dict[str, asyncio.Lock] = {}
        
        logger.info(
            "sync_engine_initialized",
            strategy=sync_strategy.value,
            batch_size=batch_size,
            sync_interval=sync_interval_seconds
        )

    async def initialize(self) -> None:
        """Initialize all connections and prepare sync engine."""
        try:
            await self._initialize_redis()
            await self._initialize_postgres()
            await self._initialize_kafka()
            
            # Create sync tables if they don't exist
            await self._create_sync_tables()
            
            logger.info("sync_engine_ready")
        except Exception as e:
            logger.error("sync_engine_initialization_failed", error=str(e))
            raise SyncEngineError(f"Failed to initialize sync engine: {e}")

    async def _initialize_redis(self) -> None:
        """Initialize Redis connection with retry logic."""
        try:
            self.redis_client = redis.from_url(
                self.redis_url,
                encoding="utf-8",
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5,
                retry_on_timeout=True,
                health_check_interval=30
            )
            
            # Test connection
            await self.redis_client.ping()
            logger.info("redis_connection_established")
        except Exception as e:
            logger.error("redis_connection_failed", error=str(e))
            raise RedisConnectionError(f"Failed to connect to Redis: {e}")

    async def _initialize_postgres(self) -> None:
        """Initialize PostgreSQL connection pool."""
        try:
            self.postgres_pool = await asyncpg.create_pool(
                self.postgres_url,
                min_size=5,
                max_size=20,
                command_timeout=30,
                server_settings={
                    'jit': 'off',
                    'application_name': 'ml_feature_store_sync'
                }
            )
            logger.info("postgres_connection_established")
        except Exception as e:
            logger.error("postgres_connection_failed", error=str(e))
            raise PostgreSQLConnectionError(f"Failed to connect to PostgreSQL: {e}")

    async def _initialize_kafka(self) -> None:
        """Initialize Kafka producer and consumer."""
        try:
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
                key_serializer=lambda x: x.encode('utf-8') if isinstance(x, str) else x,
                acks='all',
                retries=3,
                batch_size=16384,
                linger_ms=10,
                compression_type='snappy'
            )
            
            self.kafka_consumer = KafkaConsumer(
                'feature-updates',
                'feature-deletions',
                'sync-commands',
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                key_deserializer=lambda x: x.decode('utf-8') if x else None,
                auto_offset_reset='latest',
                enable_auto_commit=False,
                group_id='feature-store-sync'
            )
            
            logger.info("kafka_connections_established")
        except Exception as e:
            logger.error("kafka_connection_failed", error=str(e))
            raise KafkaConnectionError(f"Failed to initialize Kafka: {e}")

    async def _create_sync_tables(self) -> None:
        """Create necessary sync tracking tables in PostgreSQL."""
        async with self.postgres_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS feature_sync_log (
                    sync_id VARCHAR(36) PRIMARY KEY,
                    feature_group VARCHAR(100) NOT NULL,
                    entity_id VARCHAR(100) NOT NULL,
                    operation VARCHAR(20) NOT NULL,
                    status VARCHAR(20) NOT NULL,
                    sync_strategy VARCHAR(20) NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    completed_at TIMESTAMP WITH TIME ZONE,
                    error_message TEXT,
                    retry_count INTEGER DEFAULT 0,
                    metadata JSONB
                );
                
                CREATE INDEX IF NOT EXISTS idx_feature_sync_log_status 
                ON feature_sync_log(status);
                
                CREATE INDEX IF NOT EXISTS idx_feature_sync_log_created_at 
                ON feature_sync_log(created_at);
                
                CREATE TABLE IF NOT EXISTS sync_metrics (
                    metric_date DATE PRIMARY KEY,
                    total_synced INTEGER DEFAULT 0,
                    successful_syncs INTEGER DEFAULT 0,
                    failed_syncs INTEGER DEFAULT 0,
                    avg_sync_time_ms DECIMAL(10,2) DEFAULT 0,
                    redis_hits INTEGER DEFAULT 0,
                    redis_misses INTEGER DEFAULT 0,
                    postgres_operations INTEGER DEFAULT 0,
                    kafka_messages_processed INTEGER DEFAULT 0,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                );
            """)

    async def start_sync_engine(self) -> None:
        """Start the synchronization engine with all background tasks."""
        if self.running:
            logger.warning("sync_engine_already_running")
            return
        
        self.running = True
        logger.info("starting_sync_engine")
        
        # Start background tasks
        tasks = [
            asyncio.create_task(self._sync_worker()),
            asyncio.create_task(self._kafka_consumer_worker()),
            asyncio.create_task(self._periodic_sync_worker()),
            asyncio.create_task(self._metrics_reporter()),
        ]
        
        # If using write-behind strategy, start batch processor
        if self.sync_strategy == SyncStrategy.WRITE_BEHIND:
            tasks.append(asyncio.create_task(self._batch_sync_worker()))
        
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.error("sync_engine_task_failed", error=str(e))
            self.running = False
            raise

    async def stop_sync_engine(self) -> None:
        """Stop the synchronization engine gracefully."""
        self.running = False
        logger.info("stopping_sync_engine")
        
        # Close connections
        if self.redis_client:
            await self.redis_client.close()
        
        if self.postgres_pool:
            await self.postgres_pool.close()
        
        if self.kafka_producer:
            self.kafka_producer.close()
        
        if self.kafka_consumer:
            self.kafka_consumer.close()

    async def sync_feature(
        self,
        feature_group: str,
        entity_id: str,
        features: Dict[str, Any],
        operation: str = "upsert"
    ) -> bool:
        """
        Synchronize a single feature set between Redis and PostgreSQL.
        
        Args:
            feature_group: Feature group identifier
            entity_id: Entity identifier
            features: Feature data dictionary
            operation: Operation type (upsert, delete)
            
        Returns:
            Success status
        """
        sync_id = f"{feature_group}:{entity_id}:{int(time.time() * 1000)}"
        sync_event = FeatureSyncEvent(
            feature_group=feature_group,
            entity_id=entity_id,
            features=features,
            timestamp=datetime.now(timezone.utc),
            operation=operation,
            sync_id=sync_id
        )
        
        try:
            if self.sync_strategy == SyncStrategy.WRITE_THROUGH:
                return await self._write_through_sync(sync_event)
            elif self.sync_strategy == SyncStrategy.WRITE_BEHIND:
                await self.sync_queue.put(sync_event)
                return True
            elif self.sync_strategy == SyncStrategy.WRITE_BACK:
                return await self._write_back_sync(sync_event)
            else:
                raise InvalidFeatureError(f"Unsupported sync strategy: {self.sync_strategy}")
                
        except Exception as e:
            logger.error(
                "feature_sync_failed",
                sync_id=sync_id,
                feature_group=feature_group,
                entity_id=entity_id,
                error=str(e)
            )
            await self._log_sync_operation(sync_event, SyncStatus.FAILED, str(e))
            return False

    async def _write_through_sync(self, sync_event: FeatureSyncEvent) -> bool:
        """Write-through synchronization: write to both Redis and PostgreSQL."""
        start_time = time.time()
        feature_key = f"{sync_event.feature_group}:{sync_event.entity_id}"
        
        # Get or create lock for this feature
        if feature_key not in self.feature_locks:
            self.feature_locks[feature_key] = asyncio.Lock()
        
        async with self.feature_locks[feature_key]:
            try:
                await self._log_sync_operation(sync_event, SyncStatus.IN_PROGRESS)
                
                if sync_event.operation == "delete":
                    # Delete from both stores
                    await self._delete_from_redis(feature_key)
                    await self._delete_from_postgres(sync_event)
                else:
                    # Write to both stores
                    await self._write_to_redis(feature_key, sync_event.features)
                    await self._write_to_postgres(sync_event)
                
                # Publish to Kafka for other instances
                await self._publish_sync_event(sync_event)
                
                sync_time_ms = (time.time() - start_time) * 1000
                await self._log_sync_operation(sync_event, SyncStatus.SUCCESS)
                
                # Update metrics
                self.metrics.total_synced += 1
                self.metrics.successful_syncs += 1
                self.metrics.postgres_operations += 1
                self.metrics.avg_sync_time_ms = (
                    (self.metrics.avg_sync_time_ms * (self.metrics.successful_syncs - 1) + sync_time_ms)
                    / self.metrics.successful_syncs
                )
                self.metrics.last_sync_timestamp = datetime.now(timezone.utc)
                
                logger.info(
                    "write_through_sync_success",
                    sync_id=sync_event.sync_id,
                    sync_time_ms=sync_time_ms
                )
                return True
                
            except Exception as e:
                await self._log_sync_operation(sync_event, SyncStatus.FAILED, str(e))
                self.metrics.failed_syncs += 1
                raise FeatureSyncError(f"Write-through sync failed: {e}")

    async def _write_back_sync(self, sync_event: FeatureSyncEvent) -> bool:
        """Write-back synchronization: write to Redis immediately, PostgreSQL later."""
        feature_key = f"{sync_event.feature_group}:{sync_event.entity_id}"
        
        try:
            # Write to Redis first
            if sync_event.operation == "delete":
                await self._delete_from_redis(feature_key)
            else:
                await self._write_to_redis(feature_key, sync_event.features)
            
            # Queue for PostgreSQL sync
            await self.sync_queue.put(sync_event)
            
            logger.info("write_back_sync_queued", sync_id=sync_event.sync_id)
            return True
            
        except Exception as e:
            await self._log_sync_operation(sync_event, SyncStatus.FAILED, str(e))
            raise FeatureSyncError(f"Write-back sync failed: {e}")

    async def _write_to_redis(self, key: str, features: Dict[str, Any]) -> None:
        """Write features to Redis with error handling."""
        try:
            # Serialize features with numpy array handling
            serialized_features = {}
            for k, v in features.items():
                if isinstance(v, np.ndarray):
                    serialized_features[k] = v.tolist()
                elif isinstance(v, (datetime,)):
                    serialized_features[k] = v.isoformat()
                else:
                    serialized_features[k] = v
            
            await self.redis_client.hset(key, mapping=serialized_features)
            await self.redis_client.expire(key, 3600)  # 1 hour TTL
            
        except Exception as e:
            self._handle_circuit_breaker("redis")
            raise RedisConnectionError(f"Failed to write to Redis: {e}")

    async def _delete_from_redis(self, key: str) -> None:
        """Delete features from Redis."""
        try:
            await self.redis_client.delete(key)
        except Exception as e:
            self._handle_circuit_breaker("redis")
            raise RedisConnectionError(f"Failed to delete from Redis: {e}")

    async def _write_to_postgres(self, sync_event: FeatureSyncEvent) -> None:
        """Write features to PostgreSQL."""
        async with self.postgres_pool.acquire() as conn:
            try:
                # Upsert features
                await conn.execute("""
                    INSERT INTO features (feature_group, entity_id, feature_data, updated_at)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (feature_group, entity_id)
                    DO UPDATE SET
                        feature_data = EXCLUDED.feature_data,
                        updated_at = EXCLUDED.updated_at
                """, 
                sync_event.feature_group,
                sync_event.entity_id,
                json.dumps(sync_event.features, default=str),
                sync_event.timestamp
                )
                
            except Exception as e:
                self._handle_circuit_breaker("postgres")
                raise PostgreSQLConnectionError(f"Failed to write to PostgreSQL: {e}")

    async def _delete_from_postgres(self, sync_event: FeatureSyncEvent) -> None:
        """Delete features from PostgreSQL."""
        async with self.postgres_pool.acquire() as conn:
            try:
                await conn.execute("""
                    DELETE FROM features 
                    WHERE feature_group = $1 AND entity_id = $2
                """, sync_event.feature_group, sync_event.entity_id)
                
            except Exception as e:
                self._handle_circuit_breaker("postgres")
                raise PostgreSQLConnectionError(f"Failed to delete from PostgreSQL: {e}")

    async def _publish_sync_event(self, sync_event: FeatureSyncEvent) -> None:
        """Publish sync event to Kafka."""
        try:
            message = {
                "sync_id": sync_event.sync_id,
                "feature_group": sync_event.feature_group,
                "entity_id": sync_event.entity_id,
                "operation": sync_event.operation,
                "timestamp": sync_event.timestamp.isoformat(),
                "features": sync_event.features
            }
            
            topic = "feature-deletions" if sync_event.operation == "delete" else "feature-updates"
            key = f"{sync_event.feature_group}:{sync_event.entity_id}"
            
            self.kafka_producer.send(topic, key=key, value=message)
            self.kafka_producer.flush()
            
        except KafkaError as e:
            self._handle_circuit_breaker("kafka")
            logger.warning("kafka_publish_failed", error=str(e))

    async def _log_sync_operation(
        self,
        sync_event: FeatureSyncEvent,
        status: SyncStatus,
        error_message: Optional[str] = None
    ) -> None:
        """Log sync operation to PostgreSQL."""
        async with self.postgres_pool.acquire() as conn:
            try:
                if status == SyncStatus.IN_PROGRESS:
                    await conn.execute("""
                        INSERT INTO feature_sync_log 
                        (sync_id, feature_group, entity_id, operation, status, sync_strategy, metadata)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                    """,
                    sync_event.sync_id,
                    sync_event.feature_group,
                    sync_event.entity_id,
                    sync_event.operation,
                    status.value,
                    self.sync_strategy.value,
                    json.dumps(sync_event.metadata or {})
                    )
                else:
                    await conn.execute("""
                        UPDATE feature_sync_log
                        SET status = $2, completed_at = NOW(), error_message = $3
                        WHERE sync_id = $1
                    """, sync_event.sync_id, status.value, error_message)
                    
            except Exception as e:
                logger.error("sync_log_failed", sync_id=sync_event.sync_id, error=str(e))

    def _handle_circuit_breaker(self, service: str) -> None:
        """Handle circuit breaker logic for service failures."""
        self.circuit_breaker_failures[service] += 1
        
        if self.circuit_breaker_failures[service] >= self.circuit_breaker_threshold:
            logger.error(
                "circuit_breaker_triggered",
                service=service,
                failures=self.circuit_breaker_failures[service]
            )

    async def _sync_worker(self) -> None:
        """Background worker for processing sync queue."""
        while self.running:
            try:
                sync_event = await asyncio.wait_for(self.sync_queue.get(), timeout=1.0)
                await self._process_queued_sync(sync_event)
                self.sync_queue.task_done()
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error("sync_worker_error", error=str(e))

    async def _process_queued_sync(self, sync_event: FeatureSyncEvent) -> None:
        """Process a queued sync event."""
        retry_count = 0
        
        while retry_count < self.max_retry_attempts:
            try:
                if sync_event.operation == "delete":
                    await self._delete_from_postgres(sync_event)
                else:
                    await self._write_to_postgres(sync_event)
                
                await self._log_sync_operation(sync_event, SyncStatus.SUCCESS)
                self.metrics.successful_syncs += 1
                break
                
            except Exception as e:
                retry_count += 1
                if retry_count >= self.max_retry_attempts:
                    await self._log_sync_operation(sync_event, SyncStatus.FAILED, str(e))
                    self.metrics.failed_syncs += 1
                    logger.error(
                        "queued_sync_failed_permanently",
                        sync_id=sync_event.sync_id,
                        retry_count=retry_count,
                        error=str(e)
                    )
                else:
                    await asyncio.sleep(2 ** retry_count)  # Exponential backoff

    async def _kafka_consumer_worker(self) -> None:
        """Background worker for consuming Kafka messages."""
        while self.running:
            try:
                messages = self.kafka_consumer.poll(timeout_ms=1000)
                for topic_partition, message_list in messages.items():
                    for message in message_list:
                        await self._process_kafka_message(message)
                        self.metrics.kafka_messages_processed += 1
                
                if messages:
                    self.kafka_consumer.commit()
                    
            except Exception as e:
                logger.error("kafka_consumer_error", error=str(e))
                await asyncio.sleep(5)

    async def _process_kafka_message(self, message) -> None:
        """Process incoming Kafka message."""
        try:
            data = message.value
            sync_event = FeatureSyncEvent(
                feature_group=data["feature_group"],
                entity_id=data["entity_id"],
                features=data.get("features", {}),
                timestamp=datetime.fromisoformat(data["timestamp"]),
                operation=data["operation"],
                sync_id=data["sync_id"]
            )
            
            # Process external sync event
            await self._handle_external_sync(sync_event)
            
        except Exception as e:
            logger.error("kafka_message_processing_failed", error=str(e))

    async def _handle_external_sync(self, sync_event: FeatureSyncEvent) -> None:
        """Handle sync event from external sources."""
        feature_key = f"{sync_event.feature_group}:{sync_event.entity_id}"
        
        # Update local Redis cache
        try:
            if sync_event.operation == "delete":
                await self._delete_from_redis(feature_key)
            else:
                await self._write_to_redis(feature_key, sync_event.features)
                
            logger.info("external_sync_processed", sync_id=sync_event.sync_id)
            
        except Exception as e:
            logger.error("external_sync_failed", sync_id=sync_event.sync_id, error=str(e))

    async def _periodic_sync_worker(self) -> None:
        """Periodic sync for consistency checks."""
        while self.running:
            try:
                await asyncio.sleep(self.sync_interval_seconds)
                await self._run_consistency_check()
            except Exception as e:
                logger.error("periodic_sync_error", error=str(e))

    async def _run_consistency_check(self) -> None:
        """Run consistency check between Redis and PostgreSQL."""
        try:
            # Get sample of keys from Redis
            redis_keys = await self.redis_client.keys("*:*")
            sample_size = min(100, len(redis_keys))
            
            if sample_size == 0:
                return
            
            import random
            sample_keys = random.sample(redis_keys, sample_size)
            
            inconsistencies = 0
            
            for key in sample_keys:
                parts = key.split(":", 1)
                if len(parts) != 2:
                    continue
                
                feature_group, entity_id = parts
                
                # Get data from both sources
                redis_data = await self.redis_client.hgetall(key)
                postgres_data = await self._get_from_postgres(feature_group, entity_id)
                
                if not self._data_consistent(redis_data, postgres_data):
                    inconsistencies += 1
                    await self._resolve_inconsistency(feature_group, entity_id, redis_data, postgres_data)
            
            if inconsistencies > 0:
                logger.warning(
                    "consistency_check_completed",
                    inconsistencies=inconsistencies,
                    sample_size=sample_size
                )
            else:
                logger.info("consistency_check_passed", sample_size=sample_size)
                
        except Exception as e:
            logger.error("consistency_check_failed", error=str(e))

    async def _get_from_postgres(self, feature_group: str, entity_id: str) -> Optional[Dict[str, Any]]:
        """Get feature data from PostgreSQL."""
        async with self.postgres_pool.acquire() as conn:
            try:
                row = await conn.fetchrow("""
                    SELECT feature_data FROM features 
                    WHERE feature_group = $1 AND entity_id = $2
                """, feature_group, entity_id)
                
                return json.loads(row["feature_data"]) if row else None
                
            except Exception as e:
                logger.error("postgres_get_failed", error=str(e))
                return None

    def _data_consistent(self, redis_data: Dict[str, Any], postgres_data: Optional[Dict[str, Any]]) -> bool:
        """Check if data is consistent between Redis and PostgreSQL."""
        if postgres_data is None:
            return len(redis_data) == 0
        
        if len(redis_data) != len(postgres_data):
            return False
        
        for key, redis_value in redis_data.items():
            postgres_value = postgres_data.get(key)
            if str(redis_value) != str(postgres_value):
                return False
        
        return True

    async def _resolve_inconsistency(
        self,
        feature_group: str,
        entity_id: str,
        redis_data: Dict[str, Any],
        postgres_data: Optional[Dict[str, Any]]
    ) -> None:
        """Resolve data inconsistency between Redis and PostgreSQL."""
        # In this implementation, PostgreSQL is considered the source of truth
        if postgres_data:
            await self._write_to_redis(f"{feature_group}:{entity_id}", postgres_data)
            logger.info("inconsistency_resolved_from_postgres", feature_group=feature_group, entity_id=entity_id)
        else:
            await self._delete_from_redis(f"{feature_group}:{entity_id}")
            logger.info("inconsistency_resolved_deleted_from_redis", feature_group=feature_group, entity_id=entity_id)

    async def _batch_sync_worker(self) -> None:
        """Batch sync worker for write-behind strategy."""
        batch = []
        
        while self.running:
            try:
                # Collect batch
                while len(batch) < self.batch_size:
                    try:
                        sync_event = await asyncio.wait_for(self.sync_queue.get(), timeout=5.0)
                        batch.append(sync_event)
                    except asyncio.TimeoutError:
                        break
                
                if batch:
                    await self._process_sync_batch(batch)
                    batch.clear()
                    
            except Exception as e:
                logger.error("batch_sync_worker_error", error=str(e))

    async def _process_sync_batch(self, batch: List[FeatureSyncEvent]) -> None:
        """Process a batch of sync events."""
        async with self.postgres_pool.acquire() as conn:
            async with conn.transaction():
                try:
                    for sync_event in batch:
                        if sync_event.operation == "delete":
                            await conn.execute("""
                                DELETE FROM features 
                                WHERE feature_group = $1 AND entity_id = $2
                            """, sync_event.feature_group, sync_event.entity_id)
                        else:
                            await conn.execute("""
                                INSERT INTO features (feature_group, entity_id, feature_data, updated_at)
                                VALUES ($1, $2, $3, $4)
                                ON CONFLICT (feature_group, entity_id)
                                DO UPDATE SET
                                    feature_data = EXCLUDED.feature_data,
                                    updated_at = EXCLUDED.updated_at
                            """, 
                            sync_event.feature_group,
                            sync_event.entity_id,
                            json.dumps(sync_event.features, default=str),
                            sync_event.timestamp
                            )
                    
                    self.metrics.successful_syncs += len(batch)
                    logger.info("batch_sync_completed", batch_size=len(batch))
                    
                except Exception as e:
                    self.metrics.failed_syncs += len(batch)
                    logger.error("batch_sync_failed", batch_size=len(batch), error=str(e))
                    raise

    async def _metrics_reporter(self) -> None:
        """Background worker for reporting metrics."""
        while self.running:
            try:
                await asyncio.sleep(60)  # Report every minute
                await self._persist_metrics()
            except Exception as e:
                logger.error("metrics_reporter_error", error=str(e))

    async def _persist_metrics(self) -> None:
        """Persist current metrics to database."""
        async with self.postgres_pool.acquire() as conn:
            try:
                today = datetime.now().date()
                await conn.execute("""
                    INSERT INTO sync_metrics 
                    (metric_date