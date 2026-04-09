from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, List, Optional
from datetime import datetime
from ..core.feature_store import FeatureStore
from ..models.feature_schema import FeatureMetadata
from pydantic import BaseModel
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/lineage", tags=["lineage"])

class FeatureLineage(BaseModel):
    feature_name: str
    source_features: List[str]
    transformations: List[str]
    created_at: datetime
    updated_at: datetime

class LineageGraph(BaseModel):
    nodes: List[str]
    edges: List[Dict[str, str]]
    metadata: Dict[str, FeatureMetadata]

@router.get("/feature/{feature_name}", response_model=FeatureLineage)
async def get_feature_lineage(
    feature_name: str,
    store: FeatureStore = Depends()
) -> FeatureLineage:
    """Get lineage information for a specific feature."""
    try:
        lineage_data = await store.get_feature_lineage(feature_name)
        if not lineage_data:
            raise HTTPException(status_code=404, detail=f"Lineage not found for feature: {feature_name}")
        
        return FeatureLineage(**lineage_data)
    except Exception as e:
        logger.error(f"Failed to get lineage for {feature_name}: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve feature lineage")

@router.get("/graph/{root_feature}", response_model=LineageGraph)
async def get_lineage_graph(
    root_feature: str,
    depth: Optional[int] = 3,
    store: FeatureStore = Depends()
) -> LineageGraph:
    """Get feature dependency graph starting from root feature."""
    try:
        graph_data = await store.build_lineage_graph(root_feature, max_depth=depth)
        return LineageGraph(**graph_data)
    except Exception as e:
        logger.error(f"Failed to build lineage graph for {root_feature}: {e}")
        raise HTTPException(status_code=500, detail="Failed to build lineage graph")

@router.post("/track")
async def track_feature_transformation(
    feature_name: str,
    source_features: List[str],
    transformation: str,
    store: FeatureStore = Depends()
):
    """Track a new feature transformation for lineage."""
    try:
        await store.track_lineage(
            feature_name=feature_name,
            source_features=source_features,
            transformation=transformation
        )
        return {"status": "tracked", "feature": feature_name}
    except Exception as e:
        logger.error(f"Failed to track lineage for {feature_name}: {e}")
        raise HTTPException(status_code=500, detail="Failed to track feature lineage")