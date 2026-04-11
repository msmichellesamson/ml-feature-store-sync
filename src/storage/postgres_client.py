import asyncio
import logging
from typing import Dict, List, Optional, Any
from contextlib import asynccontextmanager
import asyncpg
from asyncpg import Pool, Connection
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

class PostgresClient:
    def __init__(self, dsn: str, min_connections: int = 5, max_connections: int = 20):
        self.dsn = dsn
        self.min_connections = min_connections
        self.max_connections = max_connections
        self._pool: Optional[Pool] = None

    async def initialize(self) -> None:
        """Initialize connection pool with retry logic."""
        try:
            self._pool = await asyncpg.create_pool(
                self.dsn,
                min_size=self.min_connections,
                max_size=self.max_connections,
                command_timeout=10,
                server_settings={
                    'application_name': 'ml-feature-store'
                }
            )
            logger.info(f"PostgreSQL pool initialized with {self.min_connections}-{self.max_connections} connections")
        except Exception as e:
            logger.error(f"Failed to initialize PostgreSQL pool: {e}")
            raise

    @asynccontextmanager
    async def get_connection(self) -> Connection:
        """Get connection from pool with proper error handling."""
        if not self._pool:
            raise RuntimeError("PostgresClient not initialized")
        
        conn = None
        try:
            conn = await self._pool.acquire()
            yield conn
        except Exception as e:
            logger.error(f"Database operation failed: {e}")
            raise
        finally:
            if conn:
                await self._pool.release(conn)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def store_features(self, namespace: str, features: Dict[str, Any]) -> None:
        """Store features with retry logic."""
        async with self.get_connection() as conn:
            await conn.execute(
                """
                INSERT INTO feature_store (namespace, feature_key, feature_value, created_at)
                VALUES ($1, $2, $3, NOW())
                ON CONFLICT (namespace, feature_key) 
                DO UPDATE SET feature_value = $3, updated_at = NOW()
                """,
                namespace, list(features.keys())[0], list(features.values())[0]
            )

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def get_features(self, namespace: str, feature_keys: List[str]) -> Dict[str, Any]:
        """Retrieve features with retry logic."""
        async with self.get_connection() as conn:
            rows = await conn.fetch(
                "SELECT feature_key, feature_value FROM feature_store WHERE namespace = $1 AND feature_key = ANY($2)",
                namespace, feature_keys
            )
            return {row['feature_key']: row['feature_value'] for row in rows}

    async def close(self) -> None:
        """Close connection pool."""
        if self._pool:
            await self._pool.close()
            logger.info("PostgreSQL pool closed")