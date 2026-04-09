from fastapi import FastAPI
from contextlib import asynccontextmanager
from .api import features, health, lineage
from .core.feature_store import FeatureStore
from .storage.postgres_client import PostgresClient
from .storage.redis_client import RedisClient
from .streaming.kafka_consumer import start_kafka_consumer
import asyncio
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting ML Feature Store Sync...")
    
    # Initialize clients
    postgres = PostgresClient()
    redis = RedisClient()
    feature_store = FeatureStore(postgres, redis)
    
    await postgres.connect()
    await redis.connect()
    
    # Start background Kafka consumer
    consumer_task = asyncio.create_task(start_kafka_consumer(feature_store))
    
    app.state.feature_store = feature_store
    app.state.consumer_task = consumer_task
    
    yield
    
    # Shutdown
    logger.info("Shutting down ML Feature Store Sync...")
    consumer_task.cancel()
    await postgres.disconnect()
    await redis.disconnect()

app = FastAPI(
    title="ML Feature Store Sync",
    description="Production feature store with Redis caching, PostgreSQL persistence, and real-time ML feature pipeline synchronization",
    version="1.0.0",
    lifespan=lifespan
)

# Include routers
app.include_router(health.router)
app.include_router(features.router)
app.include_router(lineage.router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)