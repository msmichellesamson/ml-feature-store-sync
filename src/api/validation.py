from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, ValidationError
from typing import Dict, Any, List
import logging
from ..core.feature_store import FeatureStore
from ..models.feature_schema import FeatureSchema

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/validation", tags=["validation"])

class ValidationRequest(BaseModel):
    feature_group: str
    features: Dict[str, Any]
    version: str = "latest"

class ValidationResult(BaseModel):
    valid: bool
    errors: List[str]
    warnings: List[str]
    schema_version: str

@router.post("/validate", response_model=ValidationResult)
async def validate_features(
    request: ValidationRequest,
    feature_store: FeatureStore = Depends()
) -> ValidationResult:
    """Validate feature values against registered schema."""
    try:
        schema = await feature_store.get_schema(
            request.feature_group, 
            request.version
        )
        
        if not schema:
            raise HTTPException(
                status_code=404, 
                detail=f"Schema not found for {request.feature_group}"
            )
        
        errors = []
        warnings = []
        
        # Check required features
        required_features = {f.name for f in schema.features if f.required}
        missing_features = required_features - set(request.features.keys())
        if missing_features:
            errors.append(f"Missing required features: {list(missing_features)}")
        
        # Validate feature types and constraints
        for feature_name, value in request.features.items():
            feature_def = next(
                (f for f in schema.features if f.name == feature_name), 
                None
            )
            
            if not feature_def:
                warnings.append(f"Unknown feature: {feature_name}")
                continue
                
            # Type validation
            expected_type = feature_def.dtype
            if not _validate_type(value, expected_type):
                errors.append(
                    f"Feature {feature_name}: expected {expected_type}, "
                    f"got {type(value).__name__}"
                )
        
        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            schema_version=schema.version
        )
        
    except Exception as e:
        logger.error(f"Validation error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def _validate_type(value: Any, expected_type: str) -> bool:
    """Validate feature value type."""
    type_map = {
        "string": str,
        "integer": int,
        "float": (int, float),
        "boolean": bool,
        "array": list
    }
    
    expected = type_map.get(expected_type.lower())
    if not expected:
        return True  # Unknown type, skip validation
    
    return isinstance(value, expected)

@router.get("/schema/{feature_group}")
async def get_validation_schema(
    feature_group: str,
    version: str = "latest",
    feature_store: FeatureStore = Depends()
) -> FeatureSchema:
    """Get validation schema for a feature group."""
    schema = await feature_store.get_schema(feature_group, version)
    if not schema:
        raise HTTPException(
            status_code=404,
            detail=f"Schema not found for {feature_group}:{version}"
        )
    return schema