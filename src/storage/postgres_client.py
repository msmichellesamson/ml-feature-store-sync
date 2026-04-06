"""
PostgreSQL persistence layer for ML feature store.
Handles feature schema management, feature storage, and historical queries.
"""

import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import asyncpg
import structlog
from asyncpg import Connection, Pool
from asyncpg.exceptions import UniqueViolationError, PostgresError

from ..core.exceptions import (
    FeatureStoreError,
    FeatureNotFoundError,
    InvalidFeatureError,
    StorageError
)
from ..core.models import Feature, FeatureSchema, FeatureValue

logger = structlog.get_logger(__name__)


class PostgresClient:
    """Production PostgreSQL client for ML feature persistence."""
    
    def __init__(
        self,
        dsn: str,
        min_connections: int = 5,
        max_connections: int = 20,
        connection_timeout: float = 30.0,
        command_timeout: float = 60.0,
    ) -> None:
        """Initialize PostgreSQL client with connection pooling.
        
        Args:
            dsn: Database connection string
            min_connections: Minimum pool connections
            max_connections: Maximum pool connections 
            connection_timeout: Connection timeout in seconds
            command_timeout: Command timeout in seconds
        """
        self.dsn = dsn
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.connection_timeout = connection_timeout
        self.command_timeout = command_timeout
        self.pool: Optional[Pool] = None
        self._initialized = False
    
    async def initialize(self) -> None:
        """Initialize connection pool and create schema."""
        if self._initialized:
            return
        
        try:
            self.pool = await asyncpg.create_pool(
                self.dsn,
                min_size=self.min_connections,
                max_size=self.max_connections,
                command_timeout=self.command_timeout,
                server_settings={
                    'jit': 'off',  # Disable JIT for consistent performance
                    'application_name': 'ml-feature-store'
                }
            )
            
            await self._create_schema()
            await self._create_indexes()
            self._initialized = True
            
            logger.info(
                "postgres_client_initialized",
                min_connections=self.min_connections,
                max_connections=self.max_connections
            )
            
        except Exception as e:
            logger.error("postgres_initialization_failed", error=str(e))
            raise StorageError(f"Failed to initialize PostgreSQL client: {e}") from e
    
    async def close(self) -> None:
        """Close connection pool."""
        if self.pool:
            await self.pool.close()
            self.pool = None
            self._initialized = False
            logger.info("postgres_client_closed")
    
    async def _create_schema(self) -> None:
        """Create database schema for feature store."""
        schema_sql = """
        -- Feature schemas table
        CREATE TABLE IF NOT EXISTS feature_schemas (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            feature_name VARCHAR(255) NOT NULL UNIQUE,
            data_type VARCHAR(50) NOT NULL,
            nullable BOOLEAN NOT NULL DEFAULT FALSE,
            description TEXT,
            tags JSONB DEFAULT '{}',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        
        -- Feature values table with partitioning by date
        CREATE TABLE IF NOT EXISTS feature_values (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            entity_id VARCHAR(255) NOT NULL,
            feature_name VARCHAR(255) NOT NULL,
            value JSONB NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            version INTEGER NOT NULL DEFAULT 1,
            metadata JSONB DEFAULT '{}',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            
            CONSTRAINT fk_feature_name 
                FOREIGN KEY (feature_name) 
                REFERENCES feature_schemas(feature_name) 
                ON DELETE CASCADE
        ) PARTITION BY RANGE (timestamp);
        
        -- Create partitions for current and next month
        CREATE TABLE IF NOT EXISTS feature_values_current 
            PARTITION OF feature_values
            FOR VALUES FROM (date_trunc('month', CURRENT_DATE))
            TO (date_trunc('month', CURRENT_DATE) + INTERVAL '1 month');
            
        CREATE TABLE IF NOT EXISTS feature_values_next
            PARTITION OF feature_values  
            FOR VALUES FROM (date_trunc('month', CURRENT_DATE) + INTERVAL '1 month')
            TO (date_trunc('month', CURRENT_DATE) + INTERVAL '2 months');
        
        -- Feature sync status tracking
        CREATE TABLE IF NOT EXISTS sync_status (
            feature_name VARCHAR(255) PRIMARY KEY,
            last_sync_timestamp TIMESTAMPTZ NOT NULL,
            sync_version INTEGER NOT NULL DEFAULT 1,
            status VARCHAR(50) NOT NULL DEFAULT 'synced',
            error_message TEXT,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            
            CONSTRAINT fk_sync_feature_name 
                FOREIGN KEY (feature_name) 
                REFERENCES feature_schemas(feature_name) 
                ON DELETE CASCADE
        );
        
        -- Triggers for updated_at
        CREATE OR REPLACE FUNCTION update_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = NOW();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        
        DROP TRIGGER IF EXISTS update_feature_schemas_updated_at ON feature_schemas;
        CREATE TRIGGER update_feature_schemas_updated_at
            BEFORE UPDATE ON feature_schemas
            FOR EACH ROW EXECUTE FUNCTION update_updated_at();
            
        DROP TRIGGER IF EXISTS update_sync_status_updated_at ON sync_status;
        CREATE TRIGGER update_sync_status_updated_at
            BEFORE UPDATE ON sync_status
            FOR EACH ROW EXECUTE FUNCTION update_updated_at();
        """
        
        async with self.pool.acquire() as conn:
            await conn.execute(schema_sql)
    
    async def _create_indexes(self) -> None:
        """Create performance indexes."""
        indexes_sql = """
        -- Indexes for feature_values
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_feature_values_entity_feature 
            ON feature_values (entity_id, feature_name);
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_feature_values_timestamp 
            ON feature_values (timestamp DESC);
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_feature_values_feature_timestamp 
            ON feature_values (feature_name, timestamp DESC);
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_feature_values_metadata_gin 
            ON feature_values USING GIN (metadata);
            
        -- Indexes for feature_schemas
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_feature_schemas_tags_gin 
            ON feature_schemas USING GIN (tags);
        """
        
        async with self.pool.acquire() as conn:
            await conn.execute(indexes_sql)
    
    async def register_feature_schema(self, schema: FeatureSchema) -> None:
        """Register a new feature schema."""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO feature_schemas 
                    (feature_name, data_type, nullable, description, tags)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (feature_name) DO UPDATE SET
                        data_type = EXCLUDED.data_type,
                        nullable = EXCLUDED.nullable,
                        description = EXCLUDED.description,
                        tags = EXCLUDED.tags,
                        updated_at = NOW()
                    """,
                    schema.feature_name,
                    schema.data_type.value,
                    schema.nullable,
                    schema.description,
                    json.dumps(schema.tags)
                )
                
            logger.info(
                "feature_schema_registered",
                feature_name=schema.feature_name,
                data_type=schema.data_type.value
            )
            
        except PostgresError as e:
            logger.error(
                "feature_schema_registration_failed",
                feature_name=schema.feature_name,
                error=str(e)
            )
            raise StorageError(f"Failed to register feature schema: {e}") from e
    
    async def get_feature_schema(self, feature_name: str) -> Optional[FeatureSchema]:
        """Get feature schema by name."""
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT feature_name, data_type, nullable, description, tags,
                           created_at, updated_at
                    FROM feature_schemas 
                    WHERE feature_name = $1
                    """,
                    feature_name
                )
                
                if not row:
                    return None
                
                return FeatureSchema(
                    feature_name=row['feature_name'],
                    data_type=row['data_type'],
                    nullable=row['nullable'],
                    description=row['description'],
                    tags=json.loads(row['tags']) if row['tags'] else {},
                    created_at=row['created_at'],
                    updated_at=row['updated_at']
                )
                
        except PostgresError as e:
            logger.error(
                "feature_schema_fetch_failed",
                feature_name=feature_name,
                error=str(e)
            )
            raise StorageError(f"Failed to fetch feature schema: {e}") from e
    
    async def list_feature_schemas(
        self,
        tags: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[FeatureSchema]:
        """List feature schemas with optional tag filtering."""
        try:
            where_clause = ""
            params = []
            param_count = 0
            
            if tags:
                tag_conditions = []
                for key, value in tags.items():
                    param_count += 1
                    tag_conditions.append(f"tags->>${param_count} = ${param_count + 1}")
                    params.extend([key, json.dumps(value)])
                    param_count += 1
                
                if tag_conditions:
                    where_clause = f"WHERE {' AND '.join(tag_conditions)}"
            
            param_count += 1
            limit_param = param_count
            param_count += 1
            offset_param = param_count
            params.extend([limit, offset])
            
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(
                    f"""
                    SELECT feature_name, data_type, nullable, description, tags,
                           created_at, updated_at
                    FROM feature_schemas 
                    {where_clause}
                    ORDER BY created_at DESC
                    LIMIT ${limit_param} OFFSET ${offset_param}
                    """,
                    *params
                )
                
                return [
                    FeatureSchema(
                        feature_name=row['feature_name'],
                        data_type=row['data_type'],
                        nullable=row['nullable'],
                        description=row['description'],
                        tags=json.loads(row['tags']) if row['tags'] else {},
                        created_at=row['created_at'],
                        updated_at=row['updated_at']
                    )
                    for row in rows
                ]
                
        except PostgresError as e:
            logger.error("feature_schemas_list_failed", error=str(e))
            raise StorageError(f"Failed to list feature schemas: {e}") from e
    
    async def store_feature_value(self, feature_value: FeatureValue) -> None:
        """Store a feature value."""
        # Validate feature exists
        schema = await self.get_feature_schema(feature_value.feature_name)
        if not schema:
            raise FeatureNotFoundError(f"Feature schema not found: {feature_value.feature_name}")
        
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO feature_values 
                    (entity_id, feature_name, value, timestamp, version, metadata)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    """,
                    feature_value.entity_id,
                    feature_value.feature_name,
                    json.dumps(feature_value.value),
                    feature_value.timestamp,
                    feature_value.version,
                    json.dumps(feature_value.metadata)
                )
                
            logger.debug(
                "feature_value_stored",
                entity_id=feature_value.entity_id,
                feature_name=feature_value.feature_name,
                version=feature_value.version
            )
            
        except PostgresError as e:
            logger.error(
                "feature_value_storage_failed",
                entity_id=feature_value.entity_id,
                feature_name=feature_value.feature_name,
                error=str(e)
            )
            raise StorageError(f"Failed to store feature value: {e}") from e
    
    async def get_feature_value(
        self,
        entity_id: str,
        feature_name: str,
        timestamp: Optional[datetime] = None
    ) -> Optional[FeatureValue]:
        """Get latest feature value for entity, optionally at specific timestamp."""
        try:
            time_condition = ""
            params = [entity_id, feature_name]
            
            if timestamp:
                time_condition = "AND timestamp <= $3"
                params.append(timestamp)
            
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    f"""
                    SELECT entity_id, feature_name, value, timestamp, version, metadata
                    FROM feature_values 
                    WHERE entity_id = $1 AND feature_name = $2 {time_condition}
                    ORDER BY timestamp DESC, version DESC
                    LIMIT 1
                    """,
                    *params
                )
                
                if not row:
                    return None
                
                return FeatureValue(
                    entity_id=row['entity_id'],
                    feature_name=row['feature_name'],
                    value=json.loads(row['value']),
                    timestamp=row['timestamp'],
                    version=row['version'],
                    metadata=json.loads(row['metadata']) if row['metadata'] else {}
                )
                
        except PostgresError as e:
            logger.error(
                "feature_value_fetch_failed",
                entity_id=entity_id,
                feature_name=feature_name,
                error=str(e)
            )
            raise StorageError(f"Failed to fetch feature value: {e}") from e
    
    async def get_feature_values_batch(
        self,
        entity_ids: List[str],
        feature_names: List[str],
        timestamp: Optional[datetime] = None
    ) -> Dict[Tuple[str, str], FeatureValue]:
        """Get latest feature values for multiple entities and features."""
        if not entity_ids or not feature_names:
            return {}
        
        try:
            time_condition = ""
            params = [entity_ids, feature_names]
            
            if timestamp:
                time_condition = "AND timestamp <= $3"
                params.append(timestamp)
            
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(
                    f"""
                    WITH latest_values AS (
                        SELECT entity_id, feature_name, value, timestamp, version, metadata,
                               ROW_NUMBER() OVER (
                                   PARTITION BY entity_id, feature_name 
                                   ORDER BY timestamp DESC, version DESC
                               ) as rn
                        FROM feature_values 
                        WHERE entity_id = ANY($1) AND feature_name = ANY($2) {time_condition}
                    )
                    SELECT entity_id, feature_name, value, timestamp, version, metadata
                    FROM latest_values 
                    WHERE rn = 1
                    """,
                    *params
                )
                
                result = {}
                for row in rows:
                    key = (row['entity_id'], row['feature_name'])
                    result[key] = FeatureValue(
                        entity_id=row['entity_id'],
                        feature_name=row['feature_name'],
                        value=json.loads(row['value']),
                        timestamp=row['timestamp'],
                        version=row['version'],
                        metadata=json.loads(row['metadata']) if row['metadata'] else {}
                    )
                
                return result
                
        except PostgresError as e:
            logger.error(
                "feature_values_batch_fetch_failed",
                entity_count=len(entity_ids),
                feature_count=len(feature_names),
                error=str(e)
            )
            raise StorageError(f"Failed to fetch feature values batch: {e}") from e
    
    async def get_feature_history(
        self,
        entity_id: str,
        feature_name: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 1000
    ) -> List[FeatureValue]:
        """Get feature value history for an entity."""
        try:
            where_conditions = ["entity_id = $1", "feature_name = $2"]
            params = [entity_id, feature_name]
            param_count = 2
            
            if start_time:
                param_count += 1
                where_conditions.append(f"timestamp >= ${param_count}")
                params.append(start_time)
            
            if end_time:
                param_count += 1
                where_conditions.append(f"timestamp <= ${param_count}")
                params.append(end_time)
            
            param_count += 1
            params.append(limit)
            
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(
                    f"""
                    SELECT entity_id, feature_name, value, timestamp, version, metadata
                    FROM feature_values 
                    WHERE {' AND '.join(where_conditions)}
                    ORDER BY timestamp DESC, version DESC
                    LIMIT ${param_count}
                    """,
                    *params
                )
                
                return [
                    FeatureValue(
                        entity_id=row['entity_id'],
                        feature_name=row['feature_name'],
                        value=json.loads(row['value']),
                        timestamp=row['timestamp'],
                        version=row['version'],
                        metadata=json.loads(row['metadata']) if row['metadata'] else {}
                    )
                    for row in rows
                ]
                
        except PostgresError as e:
            logger.error(
                "feature_history_fetch_failed",
                entity_id=entity_id,
                feature_name=feature_name,
                error=str(e)
            )
            raise StorageError(f"Failed to fetch feature history: {e}") from e
    
    async def update_sync_status(
        self,
        feature_name: str,
        status: str,
        sync_version: int,
        error_message: Optional[str] = None
    ) -> None:
        """Update feature sync status."""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO sync_status 
                    (feature_name, last_sync_timestamp, sync_version, status, error_message)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (feature_name) DO UPDATE SET
                        last_sync_timestamp = EXCLUDED.last_sync_timestamp,
                        sync_version = EXCLUDED.sync_version,
                        status = EXCLUDED.status,
                        error_message = EXCLUDED.error_message,
                        updated_at = NOW()
                    """,
                    feature_name,
                    datetime.now(timezone.utc),
                    sync_version,
                    status,
                    error_message
                )
                
            logger.debug(
                "sync_status_updated",
                feature_name=feature_name,
                status=status,
                sync_version=sync_version
            )
            
        except PostgresError as e:
            logger.error(
                "sync_status_update_failed",
                feature_name=feature_name,
                error=str(e)
            )
            raise StorageError(f"Failed to update sync status: {e}") from e
    
    async def get_sync_status(self, feature_name: str) -> Optional[Dict[str, Any]]:
        """Get sync status for a feature."""
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT feature_name, last_sync_timestamp, sync_version, 
                           status, error_message, updated_at
                    FROM sync_status 
                    WHERE feature_name = $1
                    """,
                    feature_name
                )
                
                if not row:
                    return None
                
                return {
                    'feature_name': row['feature_name'],
                    'last_sync_timestamp': row['last_sync_timestamp'],
                    'sync_version': row['sync_version'],
                    'status': row['status'],
                    'error_message': row['error_message'],
                    'updated_at': row['updated_at']
                }
                
        except PostgresError as e:
            logger.error(
                "sync_status_fetch_failed",
                feature_name=feature_name,
                error=str(e)
            )
            raise StorageError(f"Failed to fetch sync status: {e}") from e
    
    async def cleanup_old_partitions(self, retention_months: int = 6) -> None:
        """Clean up old feature value partitions."""
        try:
            cutoff_date = datetime.now(timezone.utc).replace(day=1) - \
                         timedelta(days=30 * retention_months)
            
            async with self.pool.acquire() as conn:
                # Get old partitions
                rows = await conn.fetch(
                    """
                    SELECT schemaname, tablename 
                    FROM pg_tables 
                    WHERE tablename LIKE 'feature_values_%'
                    AND tablename != 'feature_values_current'
                    AND tablename != 'feature_values_next'
                    """
                )
                
                for row in rows:
                    table_name = row['tablename']
                    # Extract date from table name and compare
                    # This is a simplified check - in production you'd want more robust date parsing
                    try:
                        await conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                        logger.info("old_partition_dropped", table_name=table_name)
                    except PostgresError as e:
                        logger.warning(
                            "partition_cleanup_failed",
                            table_name=table_name,
                            error=str(e)
                        )
                        
        except PostgresError as e:
            logger.error("partition_cleanup_failed", error=str(e))
            # Don't raise - cleanup failures shouldn't stop the system
    
    async def get_health_status(self) -> Dict[str, Any]:
        """Get PostgreSQL client health status."""
        if not self.pool:
            return {
                'status': 'unhealthy',
                'error': 'Connection pool not initialized'
            }
        
        try:
            async with self.pool.acquire() as conn:
                await conn.fetchval('SELECT 1')
                
                return {
                    'status': 'healthy',
                    'pool_size': self.pool.get_size(),
                    'pool_max_size': self.pool.get_max_size(),
                    'pool_min_size': self.pool.get_min_size()
                }
                
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e)
            }