from typing import Dict, List, Optional, Any
from datetime import datetime, timezone
import json
import hashlib
from dataclasses import dataclass
from redis import Redis


@dataclass
class FeatureVersion:
    version_id: str
    timestamp: datetime
    feature_name: str
    schema_hash: str
    metadata: Dict[str, Any]


class FeatureVersionManager:
    def __init__(self, redis_client: Redis, namespace: str = "feature_versions"):
        self.redis = redis_client
        self.namespace = namespace
    
    def _version_key(self, feature_name: str, version_id: str) -> str:
        return f"{self.namespace}:{feature_name}:{version_id}"
    
    def _versions_list_key(self, feature_name: str) -> str:
        return f"{self.namespace}:{feature_name}:versions"
    
    def _generate_schema_hash(self, schema: Dict[str, Any]) -> str:
        """Generate deterministic hash for feature schema"""
        schema_str = json.dumps(schema, sort_keys=True)
        return hashlib.sha256(schema_str.encode()).hexdigest()[:12]
    
    def create_version(self, feature_name: str, schema: Dict[str, Any], 
                      metadata: Optional[Dict[str, Any]] = None) -> str:
        """Create new feature version and return version ID"""
        timestamp = datetime.now(timezone.utc)
        schema_hash = self._generate_schema_hash(schema)
        version_id = f"v{timestamp.strftime('%Y%m%d_%H%M%S')}_{schema_hash}"
        
        version = FeatureVersion(
            version_id=version_id,
            timestamp=timestamp,
            feature_name=feature_name,
            schema_hash=schema_hash,
            metadata=metadata or {}
        )
        
        # Store version data
        version_key = self._version_key(feature_name, version_id)
        version_data = {
            "version_id": version_id,
            "timestamp": timestamp.isoformat(),
            "feature_name": feature_name,
            "schema_hash": schema_hash,
            "schema": json.dumps(schema),
            "metadata": json.dumps(version.metadata)
        }
        
        pipe = self.redis.pipeline()
        pipe.hset(version_key, mapping=version_data)
        pipe.lpush(self._versions_list_key(feature_name), version_id)
        pipe.expire(version_key, 2592000)  # 30 days TTL
        pipe.execute()
        
        return version_id
    
    def get_version(self, feature_name: str, version_id: str) -> Optional[FeatureVersion]:
        """Retrieve specific feature version"""
        version_key = self._version_key(feature_name, version_id)
        version_data = self.redis.hgetall(version_key)
        
        if not version_data:
            return None
            
        return FeatureVersion(
            version_id=version_data[b'version_id'].decode(),
            timestamp=datetime.fromisoformat(version_data[b'timestamp'].decode()),
            feature_name=version_data[b'feature_name'].decode(),
            schema_hash=version_data[b'schema_hash'].decode(),
            metadata=json.loads(version_data[b'metadata'].decode())
        )
    
    def list_versions(self, feature_name: str, limit: int = 10) -> List[FeatureVersion]:
        """List recent versions for a feature"""
        versions_key = self._versions_list_key(feature_name)
        version_ids = self.redis.lrange(versions_key, 0, limit - 1)
        
        versions = []
        for version_id in version_ids:
            version = self.get_version(feature_name, version_id.decode())
            if version:
                versions.append(version)
        
        return versions
