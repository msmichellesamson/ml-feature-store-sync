import logging
import statistics
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta

from ..storage.postgres_client import PostgresClient
from ..storage.redis_client import RedisClient


@dataclass
class DriftAlert:
    feature_name: str
    drift_score: float
    threshold: float
    timestamp: datetime
    sample_count: int


class FeatureDriftDetector:
    """Detects statistical drift in feature values over time."""
    
    def __init__(self, postgres: PostgresClient, redis: RedisClient, 
                 alert_threshold: float = 0.3):
        self.postgres = postgres
        self.redis = redis
        self.alert_threshold = alert_threshold
        self.logger = logging.getLogger(__name__)

    async def detect_drift(self, feature_name: str, 
                          window_hours: int = 24) -> Optional[DriftAlert]:
        """Detect drift by comparing recent vs historical distributions."""
        try:
            # Get recent feature values
            recent_values = await self._get_recent_values(feature_name, window_hours)
            if len(recent_values) < 10:
                return None
            
            # Get historical baseline
            historical_values = await self._get_historical_values(feature_name)
            if len(historical_values) < 50:
                return None
                
            # Calculate drift score using KL divergence approximation
            drift_score = self._calculate_drift_score(historical_values, recent_values)
            
            if drift_score > self.alert_threshold:
                alert = DriftAlert(
                    feature_name=feature_name,
                    drift_score=drift_score,
                    threshold=self.alert_threshold,
                    timestamp=datetime.utcnow(),
                    sample_count=len(recent_values)
                )
                
                await self._store_alert(alert)
                self.logger.warning(f"Feature drift detected: {feature_name} "
                                  f"(score: {drift_score:.3f})")
                return alert
                
            return None
            
        except Exception as e:
            self.logger.error(f"Drift detection failed for {feature_name}: {e}")
            return None

    async def _get_recent_values(self, feature_name: str, hours: int) -> List[float]:
        """Get feature values from the last N hours."""
        since = datetime.utcnow() - timedelta(hours=hours)
        
        query = """
            SELECT value FROM feature_values 
            WHERE feature_name = %s AND created_at > %s
            AND value IS NOT NULL
            ORDER BY created_at DESC LIMIT 1000
        """
        
        rows = await self.postgres.fetch(query, feature_name, since)
        return [float(row['value']) for row in rows]

    async def _get_historical_values(self, feature_name: str) -> List[float]:
        """Get historical baseline (30 days ago to 7 days ago)."""
        end_date = datetime.utcnow() - timedelta(days=7)
        start_date = datetime.utcnow() - timedelta(days=30)
        
        query = """
            SELECT value FROM feature_values 
            WHERE feature_name = %s 
            AND created_at BETWEEN %s AND %s
            AND value IS NOT NULL
            ORDER BY created_at DESC LIMIT 5000
        """
        
        rows = await self.postgres.fetch(query, feature_name, start_date, end_date)
        return [float(row['value']) for row in rows]

    def _calculate_drift_score(self, historical: List[float], recent: List[float]) -> float:
        """Simple drift score based on mean and std deviation changes."""
        if not historical or not recent:
            return 0.0
            
        hist_mean = statistics.mean(historical)
        hist_std = statistics.stdev(historical) if len(historical) > 1 else 1.0
        
        recent_mean = statistics.mean(recent)
        recent_std = statistics.stdev(recent) if len(recent) > 1 else 1.0
        
        # Normalized difference in means
        mean_drift = abs(hist_mean - recent_mean) / (hist_std + 1e-8)
        
        # Ratio of standard deviations
        std_ratio = max(recent_std / (hist_std + 1e-8), 
                       hist_std / (recent_std + 1e-8))
        
        return (mean_drift + abs(std_ratio - 1.0)) / 2.0

    async def _store_alert(self, alert: DriftAlert) -> None:
        """Store drift alert in database and cache."""
        query = """
            INSERT INTO drift_alerts 
            (feature_name, drift_score, threshold, sample_count, created_at)
            VALUES (%s, %s, %s, %s, %s)
        """
        
        await self.postgres.execute(
            query, alert.feature_name, alert.drift_score, 
            alert.threshold, alert.sample_count, alert.timestamp
        )
        
        # Cache recent alert
        cache_key = f"drift_alert:{alert.feature_name}:latest"
        await self.redis.set(cache_key, {
            "drift_score": alert.drift_score,
            "timestamp": alert.timestamp.isoformat(),
            "sample_count": alert.sample_count
        }, expire=3600)
