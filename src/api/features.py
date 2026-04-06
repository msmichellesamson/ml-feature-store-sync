from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union, Any
from uuid import UUID, uuid4

import structlog
from fastapi import APIRouter, HTTPException, Depends, Query, Path, BackgroundTasks
from pydantic import BaseModel, Field, validator
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.feature_store import FeatureStore, FeatureNotFoundError, FeatureValidationError
from src.core.sync_engine import SyncEngine
from src.database import get_db_session
from src.schemas import (
    FeatureDefinition,
    FeatureValue,
    FeatureBatch,
    FeatureQuery,
    SyncStatus
)

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/features", tags=["features"])


class FeatureCreateRequest(BaseModel):
    """Request model for creating a new feature definition."""
    name: str = Field(..., min_length=1, max_length=100, regex="^[a-zA-Z][a-zA-Z0-9_]*$")
    description: str = Field(..., min_length=1, max_length=500)
    data_type: str = Field(..., regex="^(int|float|string|bool|json)$")
    default_value: Optional[Union[int, float, str, bool, Dict]] = None
    ttl_seconds: Optional[int] = Field(None, ge=60, le=86400 * 30)  # 1 min to 30 days
    tags: Optional[List[str]] = Field(default_factory=list, max_items=10)
    
    @validator('tags')
    def validate_tags(cls, v):
        if v:
            for tag in v:
                if not isinstance(tag, str) or len(tag) > 50:
                    raise ValueError("Tags must be strings with max length 50")
        return v


class FeatureUpdateRequest(BaseModel):
    """Request model for updating feature definition."""
    description: Optional[str] = Field(None, min_length=1, max_length=500)
    default_value: Optional[Union[int, float, str, bool, Dict]] = None
    ttl_seconds: Optional[int] = Field(None, ge=60, le=86400 * 30)
    tags: Optional[List[str]] = Field(None, max_items=10)


class FeatureValueRequest(BaseModel):
    """Request model for setting feature values."""
    entity_id: str = Field(..., min_length=1, max_length=100)
    value: Union[int, float, str, bool, Dict] = Field(...)
    timestamp: Optional[datetime] = None
    
    @validator('timestamp', pre=True, always=True)
    def set_timestamp(cls, v):
        return v or datetime.utcnow()


class FeatureBatchRequest(BaseModel):
    """Request model for batch feature operations."""
    values: List[FeatureValueRequest] = Field(..., min_items=1, max_items=1000)
    sync_mode: str = Field(default="async", regex="^(sync|async)$")


class FeatureResponse(BaseModel):
    """Response model for feature definition."""
    id: UUID
    name: str
    description: str
    data_type: str
    default_value: Optional[Union[int, float, str, bool, Dict]]
    ttl_seconds: Optional[int]
    tags: List[str]
    created_at: datetime
    updated_at: datetime
    version: int


class FeatureValueResponse(BaseModel):
    """Response model for feature values."""
    feature_name: str
    entity_id: str
    value: Union[int, float, str, bool, Dict]
    timestamp: datetime
    source: str  # 'cache' or 'database'
    ttl_remaining: Optional[int]


class BatchOperationResponse(BaseModel):
    """Response model for batch operations."""
    operation_id: UUID
    processed_count: int
    failed_count: int
    sync_status: str
    errors: Optional[List[Dict[str, Any]]] = None


async def get_feature_store() -> FeatureStore:
    """Dependency to get feature store instance."""
    # This would typically be injected via DI container
    return FeatureStore()


async def get_sync_engine() -> SyncEngine:
    """Dependency to get sync engine instance."""
    return SyncEngine()


@router.post("/definitions", response_model=FeatureResponse, status_code=201)
async def create_feature_definition(
    request: FeatureCreateRequest,
    feature_store: FeatureStore = Depends(get_feature_store),
    db: AsyncSession = Depends(get_db_session)
) -> FeatureResponse:
    """Create a new feature definition."""
    logger.info("Creating feature definition", feature_name=request.name)
    
    try:
        feature_def = FeatureDefinition(
            id=uuid4(),
            name=request.name,
            description=request.description,
            data_type=request.data_type,
            default_value=request.default_value,
            ttl_seconds=request.ttl_seconds,
            tags=request.tags or [],
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            version=1
        )
        
        created_feature = await feature_store.create_feature_definition(
            feature_def, 
            session=db
        )
        
        logger.info(
            "Feature definition created successfully", 
            feature_id=created_feature.id,
            feature_name=created_feature.name
        )
        
        return FeatureResponse(**created_feature.dict())
        
    except FeatureValidationError as e:
        logger.warning("Feature validation failed", error=str(e), feature_name=request.name)
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to create feature definition", error=str(e), feature_name=request.name)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/definitions/{feature_name}", response_model=FeatureResponse)
async def get_feature_definition(
    feature_name: str = Path(..., min_length=1, max_length=100),
    feature_store: FeatureStore = Depends(get_feature_store),
    db: AsyncSession = Depends(get_db_session)
) -> FeatureResponse:
    """Get feature definition by name."""
    logger.info("Retrieving feature definition", feature_name=feature_name)
    
    try:
        feature_def = await feature_store.get_feature_definition(
            feature_name, 
            session=db
        )
        
        if not feature_def:
            raise HTTPException(
                status_code=404, 
                detail=f"Feature '{feature_name}' not found"
            )
        
        return FeatureResponse(**feature_def.dict())
        
    except FeatureNotFoundError:
        logger.warning("Feature definition not found", feature_name=feature_name)
        raise HTTPException(status_code=404, detail=f"Feature '{feature_name}' not found")
    except Exception as e:
        logger.error("Failed to retrieve feature definition", error=str(e), feature_name=feature_name)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/definitions/{feature_name}", response_model=FeatureResponse)
async def update_feature_definition(
    request: FeatureUpdateRequest,
    feature_name: str = Path(..., min_length=1, max_length=100),
    feature_store: FeatureStore = Depends(get_feature_store),
    db: AsyncSession = Depends(get_db_session)
) -> FeatureResponse:
    """Update feature definition."""
    logger.info("Updating feature definition", feature_name=feature_name)
    
    try:
        updated_feature = await feature_store.update_feature_definition(
            feature_name,
            description=request.description,
            default_value=request.default_value,
            ttl_seconds=request.ttl_seconds,
            tags=request.tags,
            session=db
        )
        
        logger.info(
            "Feature definition updated successfully",
            feature_id=updated_feature.id,
            feature_name=updated_feature.name,
            new_version=updated_feature.version
        )
        
        return FeatureResponse(**updated_feature.dict())
        
    except FeatureNotFoundError:
        logger.warning("Feature definition not found for update", feature_name=feature_name)
        raise HTTPException(status_code=404, detail=f"Feature '{feature_name}' not found")
    except FeatureValidationError as e:
        logger.warning("Feature update validation failed", error=str(e), feature_name=feature_name)
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to update feature definition", error=str(e), feature_name=feature_name)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/definitions/{feature_name}", status_code=204)
async def delete_feature_definition(
    feature_name: str = Path(..., min_length=1, max_length=100),
    feature_store: FeatureStore = Depends(get_feature_store),
    db: AsyncSession = Depends(get_db_session)
) -> None:
    """Delete feature definition and all associated values."""
    logger.info("Deleting feature definition", feature_name=feature_name)
    
    try:
        await feature_store.delete_feature_definition(feature_name, session=db)
        logger.info("Feature definition deleted successfully", feature_name=feature_name)
        
    except FeatureNotFoundError:
        logger.warning("Feature definition not found for deletion", feature_name=feature_name)
        raise HTTPException(status_code=404, detail=f"Feature '{feature_name}' not found")
    except Exception as e:
        logger.error("Failed to delete feature definition", error=str(e), feature_name=feature_name)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/values/{feature_name}", response_model=Dict[str, str], status_code=201)
async def set_feature_value(
    request: FeatureValueRequest,
    feature_name: str = Path(..., min_length=1, max_length=100),
    feature_store: FeatureStore = Depends(get_feature_store),
    sync_engine: SyncEngine = Depends(get_sync_engine),
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db_session)
) -> Dict[str, str]:
    """Set a feature value for an entity."""
    logger.info(
        "Setting feature value", 
        feature_name=feature_name, 
        entity_id=request.entity_id
    )
    
    try:
        feature_value = FeatureValue(
            feature_name=feature_name,
            entity_id=request.entity_id,
            value=request.value,
            timestamp=request.timestamp
        )
        
        # Store in cache and schedule database sync
        await feature_store.set_feature_value(feature_value)
        
        # Schedule async sync to database
        background_tasks.add_task(
            sync_engine.sync_feature_value,
            feature_value,
            db
        )
        
        logger.info(
            "Feature value set successfully",
            feature_name=feature_name,
            entity_id=request.entity_id
        )
        
        return {"status": "success", "message": "Feature value set"}
        
    except FeatureNotFoundError:
        logger.warning("Feature not found when setting value", feature_name=feature_name)
        raise HTTPException(status_code=404, detail=f"Feature '{feature_name}' not found")
    except FeatureValidationError as e:
        logger.warning("Feature value validation failed", error=str(e), feature_name=feature_name)
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to set feature value", error=str(e), feature_name=feature_name)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/values/{feature_name}/{entity_id}", response_model=FeatureValueResponse)
async def get_feature_value(
    feature_name: str = Path(..., min_length=1, max_length=100),
    entity_id: str = Path(..., min_length=1, max_length=100),
    fallback_to_db: bool = Query(True, description="Fallback to database if not in cache"),
    feature_store: FeatureStore = Depends(get_feature_store),
    db: AsyncSession = Depends(get_db_session)
) -> FeatureValueResponse:
    """Get feature value for an entity."""
    logger.info(
        "Retrieving feature value",
        feature_name=feature_name,
        entity_id=entity_id,
        fallback_to_db=fallback_to_db
    )
    
    try:
        feature_value, source, ttl_remaining = await feature_store.get_feature_value(
            feature_name,
            entity_id,
            fallback_to_db=fallback_to_db,
            session=db if fallback_to_db else None
        )
        
        if not feature_value:
            raise HTTPException(
                status_code=404,
                detail=f"Feature value not found for '{feature_name}' and entity '{entity_id}'"
            )
        
        return FeatureValueResponse(
            feature_name=feature_value.feature_name,
            entity_id=feature_value.entity_id,
            value=feature_value.value,
            timestamp=feature_value.timestamp,
            source=source,
            ttl_remaining=ttl_remaining
        )
        
    except FeatureNotFoundError:
        logger.warning("Feature not found when getting value", feature_name=feature_name)
        raise HTTPException(status_code=404, detail=f"Feature '{feature_name}' not found")
    except Exception as e:
        logger.error(
            "Failed to retrieve feature value",
            error=str(e),
            feature_name=feature_name,
            entity_id=entity_id
        )
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/batch/{feature_name}", response_model=BatchOperationResponse, status_code=202)
async def set_feature_values_batch(
    request: FeatureBatchRequest,
    feature_name: str = Path(..., min_length=1, max_length=100),
    feature_store: FeatureStore = Depends(get_feature_store),
    sync_engine: SyncEngine = Depends(get_sync_engine),
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db_session)
) -> BatchOperationResponse:
    """Set multiple feature values in batch."""
    operation_id = uuid4()
    logger.info(
        "Processing batch feature values",
        feature_name=feature_name,
        operation_id=operation_id,
        batch_size=len(request.values),
        sync_mode=request.sync_mode
    )
    
    try:
        feature_values = []
        for value_req in request.values:
            feature_values.append(FeatureValue(
                feature_name=feature_name,
                entity_id=value_req.entity_id,
                value=value_req.value,
                timestamp=value_req.timestamp
            ))
        
        batch = FeatureBatch(
            operation_id=operation_id,
            feature_name=feature_name,
            values=feature_values,
            sync_mode=request.sync_mode
        )
        
        if request.sync_mode == "sync":
            # Synchronous batch processing
            result = await feature_store.set_feature_values_batch(batch, session=db)
            sync_status = "completed"
        else:
            # Asynchronous batch processing
            await feature_store.set_feature_values_batch_async(batch)
            background_tasks.add_task(
                sync_engine.sync_feature_batch,
                batch,
                db
            )
            result = {
                "processed_count": len(request.values),
                "failed_count": 0,
                "errors": None
            }
            sync_status = "pending"
        
        logger.info(
            "Batch operation completed",
            operation_id=operation_id,
            processed_count=result["processed_count"],
            failed_count=result["failed_count"],
            sync_status=sync_status
        )
        
        return BatchOperationResponse(
            operation_id=operation_id,
            processed_count=result["processed_count"],
            failed_count=result["failed_count"],
            sync_status=sync_status,
            errors=result.get("errors")
        )
        
    except FeatureNotFoundError:
        logger.warning("Feature not found for batch operation", feature_name=feature_name)
        raise HTTPException(status_code=404, detail=f"Feature '{feature_name}' not found")
    except FeatureValidationError as e:
        logger.warning("Batch validation failed", error=str(e), feature_name=feature_name)
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(
            "Failed to process batch operation",
            error=str(e),
            feature_name=feature_name,
            operation_id=operation_id
        )
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/query", response_model=List[FeatureValueResponse])
async def query_features(
    query: FeatureQuery,
    feature_store: FeatureStore = Depends(get_feature_store),
    db: AsyncSession = Depends(get_db_session)
) -> List[FeatureValueResponse]:
    """Query multiple features for multiple entities."""
    logger.info(
        "Processing feature query",
        feature_count=len(query.features),
        entity_count=len(query.entities),
        use_cache=query.use_cache
    )
    
    try:
        results = await feature_store.query_features(
            query,
            session=db if not query.use_cache else None
        )
        
        response = []
        for result in results:
            response.append(FeatureValueResponse(
                feature_name=result.feature_name,
                entity_id=result.entity_id,
                value=result.value,
                timestamp=result.timestamp,
                source=result.source,
                ttl_remaining=result.ttl_remaining
            ))
        
        logger.info(
            "Feature query completed",
            results_count=len(response),
            cache_hits=sum(1 for r in response if r.source == "cache"),
            db_hits=sum(1 for r in response if r.source == "database")
        )
        
        return response
        
    except FeatureValidationError as e:
        logger.warning("Feature query validation failed", error=str(e))
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to process feature query", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/sync/status/{operation_id}", response_model=SyncStatus)
async def get_sync_status(
    operation_id: UUID = Path(...),
    sync_engine: SyncEngine = Depends(get_sync_engine)
) -> SyncStatus:
    """Get synchronization status for a batch operation."""
    logger.info("Retrieving sync status", operation_id=operation_id)
    
    try:
        status = await sync_engine.get_sync_status(operation_id)
        
        if not status:
            raise HTTPException(
                status_code=404,
                detail=f"Sync operation '{operation_id}' not found"
            )
        
        return status
        
    except Exception as e:
        logger.error("Failed to retrieve sync status", error=str(e), operation_id=operation_id)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/values/{feature_name}/{entity_id}", status_code=204)
async def delete_feature_value(
    feature_name: str = Path(..., min_length=1, max_length=100),
    entity_id: str = Path(..., min_length=1, max_length=100),
    feature_store: FeatureStore = Depends(get_feature_store),
    sync_engine: SyncEngine = Depends(get_sync_engine),
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db_session)
) -> None:
    """Delete a feature value for an entity."""
    logger.info(
        "Deleting feature value",
        feature_name=feature_name,
        entity_id=entity_id
    )
    
    try:
        await feature_store.delete_feature_value(feature_name, entity_id)
        
        # Schedule async deletion from database
        background_tasks.add_task(
            sync_engine.sync_feature_deletion,
            feature_name,
            entity_id,
            db
        )
        
        logger.info(
            "Feature value deleted successfully",
            feature_name=feature_name,
            entity_id=entity_id
        )
        
    except FeatureNotFoundError:
        logger.warning("Feature not found for deletion", feature_name=feature_name)
        raise HTTPException(status_code=404, detail=f"Feature '{feature_name}' not found")
    except Exception as e:
        logger.error(
            "Failed to delete feature value",
            error=str(e),
            feature_name=feature_name,
            entity_id=entity_id
        )
        raise HTTPException(status_code=500, detail="Internal server error")