import asyncio
import os
import signal
from contextlib import asynccontextmanager
from typing import Any, Dict

import structlog
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.exception_handlers import http_exception_handler
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_client import make_asgi_app

from src.api.routes import features, health, metrics
from src.core.config import Settings
from src.core.exceptions import FeatureStoreError, ValidationError
from src.core.logging import setup_logging
from src.services.cache import CacheService
from src.services.database import DatabaseService
from src.services.kafka_service import KafkaService
from src.services.feature_store import FeatureStoreService


logger = structlog.get_logger()


class FeatureStoreApp:
    """Feature store application with proper lifecycle management."""
    
    def __init__(self) -> None:
        self.settings = Settings()
        self.db_service: DatabaseService | None = None
        self.cache_service: CacheService | None = None
        self.kafka_service: KafkaService | None = None
        self.feature_store: FeatureStoreService | None = None
        self._shutdown_event = asyncio.Event()
    
    async def initialize_services(self) -> None:
        """Initialize all application services with proper error handling."""
        try:
            logger.info("Initializing feature store services")
            
            # Initialize database service
            self.db_service = DatabaseService(
                database_url=self.settings.database_url,
                pool_size=self.settings.db_pool_size,
                max_overflow=self.settings.db_max_overflow
            )
            await self.db_service.initialize()
            
            # Initialize cache service
            self.cache_service = CacheService(
                redis_url=self.settings.redis_url,
                pool_size=self.settings.redis_pool_size,
                default_ttl=self.settings.cache_ttl
            )
            await self.cache_service.initialize()
            
            # Initialize Kafka service
            self.kafka_service = KafkaService(
                bootstrap_servers=self.settings.kafka_bootstrap_servers,
                consumer_group=self.settings.kafka_consumer_group,
                feature_topic=self.settings.kafka_feature_topic
            )
            await self.kafka_service.initialize()
            
            # Initialize feature store service
            self.feature_store = FeatureStoreService(
                db_service=self.db_service,
                cache_service=self.cache_service,
                kafka_service=self.kafka_service
            )
            await self.feature_store.initialize()
            
            logger.info("All services initialized successfully")
            
        except Exception as e:
            logger.error("Failed to initialize services", error=str(e))
            await self.cleanup_services()
            raise
    
    async def cleanup_services(self) -> None:
        """Cleanup all services in reverse order."""
        cleanup_tasks = []
        
        if self.feature_store:
            cleanup_tasks.append(self.feature_store.cleanup())
            
        if self.kafka_service:
            cleanup_tasks.append(self.kafka_service.cleanup())
            
        if self.cache_service:
            cleanup_tasks.append(self.cache_service.cleanup())
            
        if self.db_service:
            cleanup_tasks.append(self.db_service.cleanup())
        
        if cleanup_tasks:
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)
            
        logger.info("Service cleanup completed")
    
    async def start_background_tasks(self) -> None:
        """Start background tasks for feature synchronization."""
        if not self.feature_store:
            raise RuntimeError("Feature store not initialized")
        
        # Start feature sync task
        asyncio.create_task(
            self._run_feature_sync(),
            name="feature_sync"
        )
        
        # Start health check task
        asyncio.create_task(
            self._run_health_checks(),
            name="health_check"
        )
        
        logger.info("Background tasks started")
    
    async def _run_feature_sync(self) -> None:
        """Run continuous feature synchronization."""
        while not self._shutdown_event.is_set():
            try:
                await self.feature_store.sync_features()
                await asyncio.sleep(self.settings.sync_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Feature sync error", error=str(e))
                await asyncio.sleep(30)  # Wait before retry
    
    async def _run_health_checks(self) -> None:
        """Run periodic health checks."""
        while not self._shutdown_event.is_set():
            try:
                health_status = await self.get_health_status()
                if not health_status["healthy"]:
                    logger.warning("Health check failed", status=health_status)
                
                await asyncio.sleep(self.settings.health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Health check error", error=str(e))
                await asyncio.sleep(60)
    
    async def get_health_status(self) -> Dict[str, Any]:
        """Get comprehensive health status."""
        health_status = {
            "healthy": True,
            "services": {},
            "timestamp": asyncio.get_event_loop().time()
        }
        
        # Check database health
        if self.db_service:
            try:
                db_healthy = await self.db_service.health_check()
                health_status["services"]["database"] = {
                    "healthy": db_healthy,
                    "status": "connected" if db_healthy else "disconnected"
                }
            except Exception as e:
                health_status["services"]["database"] = {
                    "healthy": False,
                    "status": "error",
                    "error": str(e)
                }
                health_status["healthy"] = False
        
        # Check cache health
        if self.cache_service:
            try:
                cache_healthy = await self.cache_service.health_check()
                health_status["services"]["cache"] = {
                    "healthy": cache_healthy,
                    "status": "connected" if cache_healthy else "disconnected"
                }
            except Exception as e:
                health_status["services"]["cache"] = {
                    "healthy": False,
                    "status": "error",
                    "error": str(e)
                }
                health_status["healthy"] = False
        
        # Check Kafka health
        if self.kafka_service:
            try:
                kafka_healthy = await self.kafka_service.health_check()
                health_status["services"]["kafka"] = {
                    "healthy": kafka_healthy,
                    "status": "connected" if kafka_healthy else "disconnected"
                }
            except Exception as e:
                health_status["services"]["kafka"] = {
                    "healthy": False,
                    "status": "error",
                    "error": str(e)
                }
                health_status["healthy"] = False
        
        return health_status
    
    async def shutdown(self) -> None:
        """Graceful shutdown."""
        logger.info("Initiating graceful shutdown")
        
        # Signal background tasks to stop
        self._shutdown_event.set()
        
        # Cancel all running tasks
        tasks = [task for task in asyncio.all_tasks() if not task.done()]
        if tasks:
            for task in tasks:
                if task.get_name() in ["feature_sync", "health_check"]:
                    task.cancel()
            
            await asyncio.gather(*tasks, return_exceptions=True)
        
        # Cleanup services
        await self.cleanup_services()
        
        logger.info("Shutdown completed")


# Global app instance
app_instance = FeatureStoreApp()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan management."""
    # Startup
    setup_logging(
        level=app_instance.settings.log_level,
        service_name="feature-store"
    )
    
    try:
        await app_instance.initialize_services()
        await app_instance.start_background_tasks()
        
        # Make services available to routes
        app.state.feature_store = app_instance.feature_store
        app.state.health_checker = app_instance.get_health_status
        
        logger.info(
            "Feature store started",
            version=app_instance.settings.app_version,
            environment=app_instance.settings.environment
        )
        
        yield
        
    except Exception as e:
        logger.error("Failed to start application", error=str(e))
        raise
    finally:
        # Shutdown
        await app_instance.shutdown()


def create_application() -> FastAPI:
    """Create FastAPI application with all middleware and routes."""
    application = FastAPI(
        title="ML Feature Store",
        description="Production feature store with Redis caching and PostgreSQL persistence",
        version=Settings().app_version,
        lifespan=lifespan,
        docs_url="/docs" if Settings().environment != "production" else None,
        redoc_url="/redoc" if Settings().environment != "production" else None
    )
    
    # Add CORS middleware
    application.add_middleware(
        CORSMiddleware,
        allow_origins=Settings().allowed_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE"],
        allow_headers=["*"],
    )
    
    # Add exception handlers
    @application.exception_handler(FeatureStoreError)
    async def feature_store_exception_handler(request: Request, exc: FeatureStoreError) -> JSONResponse:
        logger.error(
            "Feature store error",
            path=request.url.path,
            method=request.method,
            error=str(exc),
            error_code=exc.error_code
        )
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={
                "detail": str(exc),
                "error_code": exc.error_code,
                "type": "feature_store_error"
            }
        )
    
    @application.exception_handler(ValidationError)
    async def validation_exception_handler(request: Request, exc: ValidationError) -> JSONResponse:
        logger.error(
            "Validation error",
            path=request.url.path,
            method=request.method,
            error=str(exc),
            field=exc.field
        )
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={
                "detail": str(exc),
                "field": exc.field,
                "type": "validation_error"
            }
        )
    
    @application.exception_handler(Exception)
    async def general_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.error(
            "Unhandled exception",
            path=request.url.path,
            method=request.method,
            error=str(exc),
            error_type=type(exc).__name__
        )
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "detail": "Internal server error",
                "type": "internal_error"
            }
        )
    
    # Include API routes
    application.include_router(health.router, prefix="/health", tags=["health"])
    application.include_router(features.router, prefix="/api/v1/features", tags=["features"])
    application.include_router(metrics.router, prefix="/api/v1/metrics", tags=["metrics"])
    
    # Mount Prometheus metrics
    metrics_app = make_asgi_app()
    application.mount("/metrics", metrics_app)
    
    return application


def setup_signal_handlers() -> None:
    """Setup signal handlers for graceful shutdown."""
    def signal_handler(signum: int, frame) -> None:
        logger.info(f"Received signal {signum}, initiating shutdown")
        # The lifespan context manager will handle cleanup
        raise KeyboardInterrupt()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


# Create the FastAPI app
app = create_application()


if __name__ == "__main__":
    import uvicorn
    
    setup_signal_handlers()
    
    settings = Settings()
    
    uvicorn.run(
        "src.main:app",
        host=settings.host,
        port=settings.port,
        workers=1,  # Single worker due to in-memory state
        log_config=None,  # Use our custom logging
        access_log=False,  # We log requests ourselves
        reload=settings.environment == "development"
    )