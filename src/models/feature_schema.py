from datetime import datetime, timezone
from typing import Dict, List, Optional, Union, Any, Literal
from enum import Enum
from dataclasses import dataclass, field
from pydantic import BaseModel, validator, Field
import structlog

logger = structlog.get_logger(__name__)


class FeatureValueType(str, Enum):
    """Supported feature value types."""
    INT = "int"
    FLOAT = "float" 
    STRING = "string"
    BOOL = "bool"
    LIST_INT = "list_int"
    LIST_FLOAT = "list_float"
    LIST_STRING = "list_string"
    EMBEDDING = "embedding"


class FeatureStoreException(Exception):
    """Base exception for feature store operations."""
    pass


class FeatureValidationException(FeatureStoreException):
    """Feature validation failed."""
    pass


class FeatureSchemaException(FeatureStoreException):
    """Feature schema definition error."""
    pass


@dataclass
class FeatureMetadata:
    """Metadata for a feature definition."""
    description: str
    owner: str
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    tags: List[str] = field(default_factory=list)
    source_system: Optional[str] = None
    refresh_frequency: Optional[str] = None  # cron expression
    sla_hours: Optional[int] = None
    dependencies: List[str] = field(default_factory=list)


class FeatureDefinition(BaseModel):
    """Defines a feature with validation rules."""
    
    name: str = Field(..., regex=r'^[a-z0-9_]+$', min_length=1, max_length=100)
    feature_group: str = Field(..., regex=r'^[a-z0-9_]+$')
    value_type: FeatureValueType
    metadata: FeatureMetadata
    
    # Validation constraints
    nullable: bool = False
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    allowed_values: Optional[List[Union[str, int, float]]] = None
    max_length: Optional[int] = None  # for strings/lists
    min_length: Optional[int] = None
    embedding_dimension: Optional[int] = None  # for embeddings
    
    # Storage configuration
    ttl_seconds: Optional[int] = None  # Redis TTL
    index_columns: List[str] = Field(default_factory=list)  # PostgreSQL indexes
    
    class Config:
        use_enum_values = True
    
    @validator('embedding_dimension')
    def validate_embedding_dimension(cls, v, values):
        """Validate embedding dimension is set for embedding types."""
        if values.get('value_type') == FeatureValueType.EMBEDDING:
            if v is None or v <= 0:
                raise ValueError("embedding_dimension must be positive for embedding features")
        return v
    
    @validator('min_value', 'max_value')
    def validate_numeric_constraints(cls, v, values):
        """Validate numeric constraints apply to numeric types only."""
        value_type = values.get('value_type')
        if v is not None and value_type not in [FeatureValueType.INT, FeatureValueType.FLOAT]:
            raise ValueError(f"min/max_value only valid for numeric types, got {value_type}")
        return v
    
    @validator('max_value')
    def validate_max_greater_than_min(cls, v, values):
        """Validate max_value > min_value."""
        min_val = values.get('min_value')
        if v is not None and min_val is not None and v <= min_val:
            raise ValueError("max_value must be greater than min_value")
        return v


class FeatureValue(BaseModel):
    """Represents a feature value with metadata."""
    
    feature_name: str
    entity_id: str
    value: Union[int, float, str, bool, List[Union[int, float, str]], List[float]]
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    version: int = 1
    source: str = "unknown"
    
    class Config:
        json_encoders = {
            datetime: lambda dt: dt.isoformat()
        }


class FeatureGroup(BaseModel):
    """Collection of related features."""
    
    name: str = Field(..., regex=r'^[a-z0-9_]+$')
    description: str
    features: Dict[str, FeatureDefinition] = Field(default_factory=dict)
    entity_key: str = "entity_id"  # Primary key field name
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    version: int = 1
    
    def add_feature(self, feature_def: FeatureDefinition) -> None:
        """Add a feature to this group."""
        if feature_def.feature_group != self.name:
            raise FeatureSchemaException(
                f"Feature group mismatch: {feature_def.feature_group} != {self.name}"
            )
        
        self.features[feature_def.name] = feature_def
        logger.info("Added feature to group", 
                   feature=feature_def.name, 
                   group=self.name)
    
    def remove_feature(self, feature_name: str) -> None:
        """Remove a feature from this group."""
        if feature_name not in self.features:
            raise FeatureSchemaException(f"Feature {feature_name} not found in group {self.name}")
        
        del self.features[feature_name]
        logger.info("Removed feature from group", 
                   feature=feature_name, 
                   group=self.name)
    
    def get_feature(self, feature_name: str) -> FeatureDefinition:
        """Get a feature definition by name."""
        if feature_name not in self.features:
            raise FeatureSchemaException(f"Feature {feature_name} not found in group {self.name}")
        return self.features[feature_name]


class FeatureRegistry:
    """Central registry for all feature definitions."""
    
    def __init__(self):
        self.feature_groups: Dict[str, FeatureGroup] = {}
        self.logger = structlog.get_logger(__name__)
    
    def register_feature_group(self, feature_group: FeatureGroup) -> None:
        """Register a new feature group."""
        if feature_group.name in self.feature_groups:
            raise FeatureSchemaException(f"Feature group {feature_group.name} already exists")
        
        self.feature_groups[feature_group.name] = feature_group
        self.logger.info("Registered feature group", 
                        group=feature_group.name,
                        features=len(feature_group.features))
    
    def get_feature_group(self, group_name: str) -> FeatureGroup:
        """Get a feature group by name."""
        if group_name not in self.feature_groups:
            raise FeatureSchemaException(f"Feature group {group_name} not found")
        return self.feature_groups[group_name]
    
    def get_feature_definition(self, feature_name: str, group_name: str) -> FeatureDefinition:
        """Get a specific feature definition."""
        group = self.get_feature_group(group_name)
        return group.get_feature(feature_name)
    
    def list_feature_groups(self) -> List[str]:
        """List all registered feature group names."""
        return list(self.feature_groups.keys())
    
    def list_features(self, group_name: Optional[str] = None) -> List[str]:
        """List feature names, optionally filtered by group."""
        if group_name:
            group = self.get_feature_group(group_name)
            return list(group.features.keys())
        
        all_features = []
        for group in self.feature_groups.values():
            all_features.extend(group.features.keys())
        return all_features
    
    def validate_feature_value(self, feature_value: FeatureValue, feature_def: FeatureDefinition) -> None:
        """Validate a feature value against its definition."""
        try:
            self._validate_type(feature_value.value, feature_def.value_type)
            self._validate_constraints(feature_value.value, feature_def)
            
            self.logger.debug("Feature value validated",
                            feature=feature_value.feature_name,
                            entity=feature_value.entity_id)
        except Exception as e:
            raise FeatureValidationException(
                f"Validation failed for feature {feature_value.feature_name}: {str(e)}"
            )
    
    def _validate_type(self, value: Any, expected_type: FeatureValueType) -> None:
        """Validate value type matches expected type."""
        type_validators = {
            FeatureValueType.INT: lambda v: isinstance(v, int) and not isinstance(v, bool),
            FeatureValueType.FLOAT: lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
            FeatureValueType.STRING: lambda v: isinstance(v, str),
            FeatureValueType.BOOL: lambda v: isinstance(v, bool),
            FeatureValueType.LIST_INT: lambda v: isinstance(v, list) and all(isinstance(x, int) and not isinstance(x, bool) for x in v),
            FeatureValueType.LIST_FLOAT: lambda v: isinstance(v, list) and all(isinstance(x, (int, float)) and not isinstance(x, bool) for x in v),
            FeatureValueType.LIST_STRING: lambda v: isinstance(v, list) and all(isinstance(x, str) for x in v),
            FeatureValueType.EMBEDDING: lambda v: isinstance(v, list) and all(isinstance(x, (int, float)) and not isinstance(x, bool) for x in v)
        }
        
        validator_func = type_validators.get(expected_type)
        if not validator_func or not validator_func(value):
            raise ValueError(f"Value type mismatch: expected {expected_type}, got {type(value)}")
    
    def _validate_constraints(self, value: Any, feature_def: FeatureDefinition) -> None:
        """Validate value meets constraint requirements."""
        # Nullable check
        if value is None and not feature_def.nullable:
            raise ValueError("Value cannot be null")
        
        if value is None:
            return
        
        # Numeric constraints
        if feature_def.min_value is not None and value < feature_def.min_value:
            raise ValueError(f"Value {value} below minimum {feature_def.min_value}")
        
        if feature_def.max_value is not None and value > feature_def.max_value:
            raise ValueError(f"Value {value} above maximum {feature_def.max_value}")
        
        # Allowed values
        if feature_def.allowed_values is not None and value not in feature_def.allowed_values:
            raise ValueError(f"Value {value} not in allowed values {feature_def.allowed_values}")
        
        # Length constraints for strings and lists
        if isinstance(value, (str, list)):
            if feature_def.min_length is not None and len(value) < feature_def.min_length:
                raise ValueError(f"Length {len(value)} below minimum {feature_def.min_length}")
            
            if feature_def.max_length is not None and len(value) > feature_def.max_length:
                raise ValueError(f"Length {len(value)} above maximum {feature_def.max_length}")
        
        # Embedding dimension
        if feature_def.value_type == FeatureValueType.EMBEDDING:
            if feature_def.embedding_dimension is not None and len(value) != feature_def.embedding_dimension:
                raise ValueError(f"Embedding dimension mismatch: expected {feature_def.embedding_dimension}, got {len(value)}")


# Global registry instance
feature_registry = FeatureRegistry()