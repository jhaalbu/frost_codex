import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from frost_sync.db import Base
from frost_sync.models import Observation, Station, StationLatest
from frost_sync.web import build_latest_7d_geojson


class SevenDayGeojsonTest(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 8, 31, 12, tzinfo=timezone.utc)
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.factory = sessionmaker(self.engine, expire_on_commit=False)
        self.session = self.factory()
        self.addCleanup(self.engine.dispose)
        self.addCleanup(self.session.close)

    def station(self, source, provider="frost", longitude=10):
        station = Station(source_id=source, provider=provider, longitude=longitude,
                          latitude=62, last_seen_at=self.now, last_observation_time=self.now)
        self.session.add(station)
        self.session.flush()
        self.session.add(StationLatest(station_id=station.id, source_id=source,
                                      observed_at=self.now, updated_at=self.now))
        return station

    def observation(self, station, hours_ago, element, value):
        self.session.add(Observation(station_id=station.id,
            reference_time=self.now - timedelta(hours=hours_ago),
            element_id=element, value=value, fetched_at=self.now))

    def build(self):
        self.session.commit()
        with patch("frost_sync.web.datetime", wraps=datetime) as clock:
            clock.now.return_value = self.now
            result = build_latest_7d_geojson(self.factory)
        json.dumps(result, allow_nan=False)
        self.assertEqual(result["type"], "FeatureCollection")
        return {f["properties"]["source_id"]: f["properties"] for f in result["features"]}

    def test_extrema_timestamps_and_station_filtering(self):
        station = self.station("SN123")
        for hours, value in [(169, 99), (167, -3), (10, 20), (-1, 80)]:
            self.observation(station, hours, "air_temperature", value)
        self.observation(station, 10, "wind_speed", 12)
        self.observation(station, 10, "wind_from_direction", 270)
        stale = self.station("stale")
        self.observation(stale, 200, "air_temperature", 10)
        missing_location = self.station("missing-location", longitude=None)
        self.observation(missing_location, 1, "air_temperature", 10)
        result = self.build()
        self.assertEqual(set(result), {"SN123"})
        p = result["SN123"]
        self.assertEqual(p["air_temperature_max_7d"], 20)
        self.assertEqual(p["air_temperature_min_7d"], -3)
        self.assertEqual(datetime.fromisoformat(p["air_temperature_max_7d_time"]),
                         self.now - timedelta(hours=10))
        self.assertEqual(p["wind_direction_at_7dmax"], 270)
        self.assertNotIn("has_recent_data", p)

    def test_precipitation_lookback_and_provider_filter(self):
        station = self.station("SN123")
        # 24 hourly values ending at the first instant of the seven-day window.
        for hours in range(168, 192):
            self.observation(station, hours, "sum(precipitation_amount PT1H)", 1)
        self.observation(station, 1, "sum(precipitation_amount PT1H)", 2)
        nve = self.station("1.1.0", "nve_hydapi")
        for hours, value in [(3, 5), (2, 6), (1, -1)]:
            self.observation(nve, hours, "precipitation_1h", value)
        result = self.build()
        self.assertEqual(result["SN123"]["precipitation_7d_accumulated"], 3)
        self.assertEqual(result["SN123"]["precipitation_24h_max"], 24)
        self.assertEqual(result["SN123"]["precipitation_1h_max_7d"], 2)
        self.assertEqual(result["1.1.0"]["precipitation_7d_accumulated"], 5)
        self.assertEqual(result["1.1.0"]["precipitation_24h_max"], 5)

    def test_bad_historical_temperatures_are_excluded_with_boundaries_kept(self):
        station = self.station("SN123")
        for hours, value in [(6, -1310.72), (5, -60.01), (4, 60.01), (3, -60), (2, 60)]:
            self.observation(station, hours, "air_temperature", value)
        self.observation(station, 1, "mean(air_temperature PT1H)", 999)
        self.observation(station, -1, "wind_speed", 99)
        result = self.build()["SN123"]
        self.assertEqual(result["air_temperature_min_7d"], -60)
        self.assertEqual(result["air_temperature_max_7d"], 60)
        self.assertNotIn("wind_speed_max_7d", result)

    def test_future_latest_metadata_is_recovered_without_deleting_history(self):
        station = self.station("SN123")
        self.observation(station, 1, "air_temperature", 12)
        self.session.flush()
        latest = self.session.get(StationLatest, station.id)
        latest.observed_at = self.now + timedelta(days=6000)
        latest.wind_speed = 30
        latest.wind_speed_observed_at = latest.observed_at
        latest.air_temperature = 12
        latest.air_temperature_observed_at = self.now - timedelta(hours=1)
        station.last_observation_time = latest.observed_at
        result = self.build()["SN123"]
        self.assertEqual(result["minutes_since_observation"], 60)
        self.assertEqual(datetime.fromisoformat(result["observed_at"]),
                         self.now - timedelta(hours=1))
        # Export sanitization must not silently write to the source database.
        self.session.expire_all()
        self.assertEqual(self.session.get(StationLatest, station.id).observed_at.year, 2043)


if __name__ == "__main__":
    unittest.main()
