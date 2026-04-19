from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, Any, List, Optional
import structlog
from datetime import datetime, timedelta
from ..storage.redis_client import RedisClient
from ..storage.postgres_client import PostgresClient
from ..models.feature_schema import FeatureMetadata

logger = structlog.get_logger()
router = APIRouter(prefix="/api/v1/metadata", tags=["metadata"])


@router.get("/features/{feature_name}")
async def get_feature_metadata(
    feature_name: str,
    version: Optional[str] = None,
    redis: RedisClient = Depends(),
    postgres: PostgresClient = Depends()
) -> Dict[str, Any]:
    """Get feature metadata with Redis caching."""
    cache_key = f"metadata:{feature_name}:{version or 'latest'}"
    
    # Try cache first
    try:
        cached = await redis.get(cache_key)
        if cached:
            logger.info("metadata_cache_hit", feature=feature_name, version=version)
            return cached
    except Exception as e:
        logger.warning("metadata_cache_error", error=str(e))
    
    # Query database
    try:
        query = """
            SELECT name, version, schema_json, created_at, updated_at, 
                   tags, description, data_type, is_active
            FROM feature_metadata 
            WHERE name = $1
        """
        params = [feature_name]
        
        if version:
            query += " AND version = $2"
            params.append(version)
        else:
            query += " AND is_active = true ORDER BY created_at DESC LIMIT 1"
        
        result = await postgres.fetch_one(query, params)
        if not result:
            raise HTTPException(status_code=404, detail=f"Feature {feature_name} not found")
        
        metadata = {
            "name": result["name"],
            "version": result["version"],
            "schema": result["schema_json"],
            "created_at": result["created_at"].isoformat(),
            "updated_at": result["updated_at"].isoformat(),
            "tags": result["tags"] or [],
            "description": result["description"],
            "data_type": result["data_type"],
            "is_active": result["is_active"]
        }
        
        # Cache for 5 minutes
        try:
            await redis.setex(cache_key, 300, metadata)
        except Exception as e:
            logger.warning("metadata_cache_set_error", error=str(e))
        
        logger.info("metadata_retrieved", feature=feature_name, version=version)
        return metadata
        
    except Exception as e:
        logger.error("metadata_query_error", error=str(e), feature=feature_name)
        raise HTTPException(status_code=500, detail="Failed to retrieve metadata")


@router.get("/features")
async def list_features(
    limit: int = 100,
    offset: int = 0,
    active_only: bool = True,
    postgres: PostgresClient = Depends()
) -> Dict[str, Any]:
    """List all features with pagination."""
    try:
        query = """
            SELECT name, version, data_type, created_at, is_active
            FROM feature_metadata
        """
        params = []
        
        if active_only:
            query += " WHERE is_active = true"
        
        query += " ORDER BY created_at DESC LIMIT $1 OFFSET $2"
        params.extend([limit, offset])
        
        results = await postgres.fetch_all(query, params)
        
        features = [{
            "name": row["name"],
            "version": row["version"],
            "data_type": row["data_type"],
            "created_at": row["created_at"].isoformat(),
            "is_active": row["is_active"]
        } for row in results]
        
        return {
            "features": features,
            "count": len(features),
            "limit": limit,
            "offset": offset
        }
        
    except Exception as e:
        logger.error("list_features_error", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to list features")