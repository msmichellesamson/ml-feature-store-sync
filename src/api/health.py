from fastapi import APIRouter, HTTPException
from datetime import datetime, timedelta
from typing import Dict, List
from ..core.feature_store import FeatureStore
from ..storage.redis_client import RedisClient

router = APIRouter(prefix="/health", tags=["health"])


@router.get("/features/freshness")
async def check_feature_freshness(
    feature_store: FeatureStore,
    redis_client: RedisClient,
    max_age_minutes: int = 30
) -> Dict[str, any]:
    """Check freshness of cached features and identify stale data."""
    try:
        stale_features = []
        fresh_features = []
        cutoff_time = datetime.utcnow() - timedelta(minutes=max_age_minutes)
        
        # Get all feature keys from Redis
        feature_keys = await redis_client.scan_match("feature:*")
        
        for key in feature_keys:
            feature_data = await redis_client.get_with_metadata(key)
            if not feature_data or 'timestamp' not in feature_data:
                stale_features.append({
                    "key": key,
                    "reason": "missing_timestamp",
                    "last_updated": None
                })
                continue
                
            last_updated = datetime.fromisoformat(feature_data['timestamp'])
            if last_updated < cutoff_time:
                stale_features.append({
                    "key": key,
                    "reason": "outdated",
                    "last_updated": last_updated.isoformat(),
                    "age_minutes": int((datetime.utcnow() - last_updated).total_seconds() / 60)
                })
            else:
                fresh_features.append({
                    "key": key,
                    "last_updated": last_updated.isoformat()
                })
        
        return {
            "status": "healthy" if len(stale_features) == 0 else "degraded",
            "total_features": len(feature_keys),
            "fresh_count": len(fresh_features),
            "stale_count": len(stale_features),
            "stale_features": stale_features[:10],  # Limit output
            "max_age_minutes": max_age_minutes,
            "checked_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Freshness check failed: {str(e)}")


@router.get("/features/staleness-summary")
async def get_staleness_summary(
    redis_client: RedisClient
) -> Dict[str, int]:
    """Get summary of feature staleness across different time windows."""
    try:
        now = datetime.utcnow()
        windows = {
            "last_5_min": 5,
            "last_15_min": 15,
            "last_30_min": 30,
            "last_hour": 60,
            "last_day": 1440
        }
        
        feature_keys = await redis_client.scan_match("feature:*")
        summary = {window: 0 for window in windows.keys()}
        summary["total"] = len(feature_keys)
        
        for key in feature_keys:
            feature_data = await redis_client.get_with_metadata(key)
            if not feature_data or 'timestamp' not in feature_data:
                continue
                
            last_updated = datetime.fromisoformat(feature_data['timestamp'])
            age_minutes = (now - last_updated).total_seconds() / 60
            
            for window, threshold in windows.items():
                if age_minutes <= threshold:
                    summary[window] += 1
                    
        return summary
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Summary failed: {str(e)}")