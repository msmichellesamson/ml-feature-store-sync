from typing import List, Dict, Any
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
import asyncio
from ..core.feature_store import FeatureStore
from ..models.feature_schema import FeatureKey

router = APIRouter(prefix="/batch", tags=["batch"])

class BatchFeatureRequest(BaseModel):
    feature_keys: List[str]
    entity_ids: List[str]
    include_metadata: bool = False

class BatchFeatureResponse(BaseModel):
    features: Dict[str, Dict[str, Any]]
    missing_keys: List[str]
    total_retrieved: int

@router.post("/features", response_model=BatchFeatureResponse)
async def get_batch_features(
    request: BatchFeatureRequest,
    feature_store: FeatureStore = Depends()
):
    """Retrieve multiple features for multiple entities in a single request."""
    try:
        # Create feature keys
        feature_keys = [
            FeatureKey(name=key, entity_id=entity_id)
            for key in request.feature_keys
            for entity_id in request.entity_ids
        ]
        
        # Batch retrieve features
        tasks = [
            feature_store.get_feature(key, include_metadata=request.include_metadata)
            for key in feature_keys
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        features = {}
        missing_keys = []
        
        for key, result in zip(feature_keys, results):
            key_str = f"{key.name}:{key.entity_id}"
            if isinstance(result, Exception) or result is None:
                missing_keys.append(key_str)
            else:
                features[key_str] = result
        
        return BatchFeatureResponse(
            features=features,
            missing_keys=missing_keys,
            total_retrieved=len(features)
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Batch retrieval failed: {str(e)}")

@router.get("/stats")
async def get_batch_stats():
    """Get batch operation statistics."""
    return {
        "max_batch_size": 1000,
        "supported_operations": ["get_features"],
        "concurrent_requests": True
    }