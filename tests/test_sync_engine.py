import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timedelta

from src.core.sync_engine import SyncEngine
from src.models.feature_schema import Feature, FeatureMetadata


class TestSyncEngine:
    
    @pytest.fixture
    def mock_dependencies(self):
        return {
            'redis_client': Mock(),
            'postgres_client': AsyncMock(),
            'kafka_consumer': AsyncMock(),
            'drift_detector': Mock()
        }
    
    @pytest.fixture
    def sync_engine(self, mock_dependencies):
        return SyncEngine(**mock_dependencies)
    
    @pytest.mark.asyncio
    async def test_sync_feature_success(self, sync_engine):
        """Test successful feature synchronization"""
        feature = Feature(
            name="user_score",
            value=0.85,
            timestamp=datetime.utcnow(),
            metadata=FeatureMetadata(version="1.0", source="kafka")
        )
        
        sync_engine.redis_client.set_feature.return_value = True
        sync_engine.postgres_client.store_feature.return_value = None
        
        result = await sync_engine.sync_feature(feature)
        
        assert result is True
        sync_engine.redis_client.set_feature.assert_called_once_with(feature)
        sync_engine.postgres_client.store_feature.assert_called_once_with(feature)
    
    @pytest.mark.asyncio
    async def test_sync_feature_redis_failure(self, sync_engine):
        """Test sync failure when Redis is down"""
        feature = Feature(
            name="user_score",
            value=0.85,
            timestamp=datetime.utcnow(),
            metadata=FeatureMetadata(version="1.0", source="kafka")
        )
        
        sync_engine.redis_client.set_feature.side_effect = ConnectionError("Redis down")
        
        result = await sync_engine.sync_feature(feature)
        
        assert result is False
        sync_engine.postgres_client.store_feature.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_batch_sync_performance(self, sync_engine):
        """Test batch sync maintains acceptable throughput"""
        features = [
            Feature(
                name=f"feature_{i}",
                value=i * 0.1,
                timestamp=datetime.utcnow(),
                metadata=FeatureMetadata(version="1.0", source="batch")
            ) for i in range(100)
        ]
        
        sync_engine.redis_client.batch_set.return_value = True
        sync_engine.postgres_client.batch_store.return_value = None
        
        start_time = datetime.utcnow()
        await sync_engine.batch_sync(features)
        duration = (datetime.utcnow() - start_time).total_seconds()
        
        # Should process 100 features in under 1 second
        assert duration < 1.0
        sync_engine.redis_client.batch_set.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_drift_detection_triggered(self, sync_engine):
        """Test that drift detection is triggered for anomalous features"""
        anomalous_feature = Feature(
            name="user_score",
            value=99.9,  # Anomalously high value
            timestamp=datetime.utcnow(),
            metadata=FeatureMetadata(version="1.0", source="kafka")
        )
        
        sync_engine.drift_detector.detect_drift.return_value = True
        sync_engine.redis_client.set_feature.return_value = True
        
        await sync_engine.sync_feature(anomalous_feature)
        
        sync_engine.drift_detector.detect_drift.assert_called_once_with(anomalous_feature)
