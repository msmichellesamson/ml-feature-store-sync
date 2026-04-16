"""Feature transformation pipeline with validation and error handling."""
import asyncio
import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from datetime import datetime
import json

from ..models.feature_schema import FeatureSchema
from ..storage.redis_client import RedisClient

logger = logging.getLogger(__name__)

@dataclass
class TransformationResult:
    """Result of feature transformation."""
    features: Dict[str, Any]
    metadata: Dict[str, Any]
    errors: List[str]
    transformed_at: datetime

class FeatureTransformer:
    """Real-time feature transformation pipeline."""
    
    def __init__(self, redis_client: RedisClient, schema: FeatureSchema):
        self.redis = redis_client
        self.schema = schema
        self.transformation_cache = {}
        
    async def transform_batch(self, raw_features: List[Dict[str, Any]]) -> List[TransformationResult]:
        """Transform a batch of raw features with validation."""
        results = []
        
        for features in raw_features:
            try:
                result = await self._transform_single(features)
                results.append(result)
            except Exception as e:
                logger.error(f"Transformation failed: {e}")
                results.append(TransformationResult(
                    features={},
                    metadata={"error": str(e)},
                    errors=[str(e)],
                    transformed_at=datetime.utcnow()
                ))
                
        return results
    
    async def _transform_single(self, features: Dict[str, Any]) -> TransformationResult:
        """Transform single feature set with caching."""
        errors = []
        transformed = {}
        
        # Validate against schema
        validation_errors = self.schema.validate_features(features)
        if validation_errors:
            errors.extend(validation_errors)
            
        # Apply transformations
        for feature_name, value in features.items():
            try:
                transformed_value = await self._apply_transformation(feature_name, value)
                transformed[feature_name] = transformed_value
            except Exception as e:
                errors.append(f"Transform error for {feature_name}: {e}")
                
        # Cache transformation results
        cache_key = f"transform:{hash(json.dumps(features, sort_keys=True))}"
        await self.redis.setex(cache_key, 300, json.dumps(transformed))
        
        return TransformationResult(
            features=transformed,
            metadata={"cache_key": cache_key, "feature_count": len(transformed)},
            errors=errors,
            transformed_at=datetime.utcnow()
        )
    
    async def _apply_transformation(self, feature_name: str, value: Any) -> Any:
        """Apply feature-specific transformations."""
        # Get cached transformation if available
        cache_key = f"transform_rule:{feature_name}"
        cached_rule = await self.redis.get(cache_key)
        
        if cached_rule:
            # Apply cached transformation rule
            return self._execute_transformation_rule(json.loads(cached_rule), value)
            
        # Default transformations based on feature type
        if isinstance(value, str):
            return value.strip().lower()
        elif isinstance(value, (int, float)):
            return float(value)  # Normalize to float
        else:
            return value
            
    def _execute_transformation_rule(self, rule: Dict, value: Any) -> Any:
        """Execute a transformation rule on a value."""
        rule_type = rule.get("type")
        
        if rule_type == "normalize":
            min_val, max_val = rule["min"], rule["max"]
            return (float(value) - min_val) / (max_val - min_val)
        elif rule_type == "categorical":
            return rule["mapping"].get(str(value), rule.get("default", value))
        else:
            return value
