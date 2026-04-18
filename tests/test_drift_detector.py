import pytest
import numpy as np
from unittest.mock import Mock, patch
from src.monitoring.drift_detector import DriftDetector
from src.models.feature_schema import Feature, FeatureType


class TestDriftDetector:
    @pytest.fixture
    def detector(self):
        return DriftDetector(
            threshold=0.05,
            window_size=1000,
            metrics_client=Mock()
        )
    
    @pytest.fixture
    def sample_feature(self):
        return Feature(
            name="test_feature",
            type=FeatureType.FLOAT,
            version="v1"
        )
    
    def test_psi_calculation_no_drift(self, detector, sample_feature):
        """Test PSI calculation when distributions are similar"""
        reference_data = np.random.normal(0, 1, 1000)
        current_data = np.random.normal(0.01, 1.02, 1000)
        
        psi_score = detector._calculate_psi(reference_data, current_data)
        
        assert 0 <= psi_score <= 0.1  # No significant drift
        assert isinstance(psi_score, float)
    
    def test_psi_calculation_with_drift(self, detector, sample_feature):
        """Test PSI calculation when distributions differ significantly"""
        reference_data = np.random.normal(0, 1, 1000)
        current_data = np.random.normal(5, 2, 1000)  # Shifted distribution
        
        psi_score = detector._calculate_psi(reference_data, current_data)
        
        assert psi_score > 0.1  # Significant drift detected
    
    def test_drift_detection_triggers_alert(self, detector, sample_feature):
        """Test that drift above threshold triggers alert"""
        with patch.object(detector, '_calculate_psi', return_value=0.15):
            result = detector.detect_drift(
                feature=sample_feature,
                reference_data=np.array([1, 2, 3]),
                current_data=np.array([10, 20, 30])
            )
            
            assert result.drift_detected is True
            assert result.psi_score == 0.15
            assert result.feature_name == "test_feature"
    
    def test_drift_detection_no_alert(self, detector, sample_feature):
        """Test that drift below threshold doesn't trigger alert"""
        with patch.object(detector, '_calculate_psi', return_value=0.02):
            result = detector.detect_drift(
                feature=sample_feature,
                reference_data=np.array([1, 2, 3]),
                current_data=np.array([1.1, 2.1, 3.1])
            )
            
            assert result.drift_detected is False
            assert result.psi_score == 0.02
    
    def test_metrics_are_published(self, detector, sample_feature):
        """Test that drift metrics are published to monitoring system"""
        mock_metrics = Mock()
        detector.metrics_client = mock_metrics
        
        with patch.object(detector, '_calculate_psi', return_value=0.08):
            detector.detect_drift(
                feature=sample_feature,
                reference_data=np.array([1, 2, 3]),
                current_data=np.array([1.2, 2.2, 3.2])
            )
            
            mock_metrics.gauge.assert_called_with(
                'feature_drift_psi_score',
                0.08,
                tags={'feature': 'test_feature', 'version': 'v1'}
            )
    
    def test_empty_data_handling(self, detector, sample_feature):
        """Test handling of empty data arrays"""
        with pytest.raises(ValueError, match="Data arrays cannot be empty"):
            detector.detect_drift(
                feature=sample_feature,
                reference_data=np.array([]),
                current_data=np.array([1, 2, 3])
            )