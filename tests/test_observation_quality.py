import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from frost_sync.db import Base
from frost_sync.models import Observation, Station, StationLatest
from frost_sync.service import SyncService
from frost_sync.web import build_latest_geojson, build_latest_compact_geojson, _build_direct_points, _daily_aggregates_by_station
from sqlalchemy.orm import sessionmaker


class ObservationQualityTest(unittest.TestCase):
    def test_live_timeseries_and_daily_reject_invalid_measurements(self):
        now = datetime.now(timezone.utc)
        start = now - timedelta(days=1)
        end = now + timedelta(days=1)
        rows = []
        observations = []
        for hours, value in [(5, -60), (4, 60), (3, -1310.72), (2, 999), (-1, 30)]:
            stamp = now - timedelta(hours=hours)
            rows.append({"referenceTime": stamp.isoformat(), "observations": [
                {"elementId": "air_temperature", "value": value}]})
            observations.append(Observation(station_id=1, reference_time=stamp,
                                            element_id="air_temperature", value=value))
        points = _build_direct_points("air_temperature", ["air_temperature"], rows, start, end)
        self.assertEqual([p["value"] for p in points], [-60, 60])
        station = Station(id=1, source_id="SN123", provider="frost")
        result = _daily_aggregates_by_station(observations, {1: station}, start, end)[1]
        self.assertEqual(result["air_temperature_min"], -60)
        self.assertEqual(result["air_temperature_max"], 60)

    def test_ingestion_rejects_future_and_invalid_temperature_and_recovers_latest(self):
        engine = create_engine("sqlite:///:memory:")
        self.addCleanup(engine.dispose)
        Base.metadata.create_all(engine)
        now = datetime.now(timezone.utc)
        with Session(engine, expire_on_commit=False) as session:
            station = Station(source_id="SN123", provider="frost", last_seen_at=now)
            session.add(station)
            session.flush()
            station.latest = StationLatest(station_id=station.id, source_id=station.source_id,
                observed_at=now + timedelta(days=6000), updated_at=now,
                air_temperature=99, air_temperature_observed_at=now + timedelta(days=6000))
            session.commit()
            rows = []
            for hours, value in [(6, -1310.72), (5, 60.01), (4, -60), (3, 60), (2, 12), (-1, 20)]:
                rows.append({"sourceId": "SN123", "referenceTime": (now-timedelta(hours=hours)).isoformat(),
                    "observations": [{"elementId": "air_temperature", "value": value, "unit": "degC"}]})
            # A bad temperature must not discard another valid element in the same row.
            rows[0]["observations"].append({"elementId": "wind_speed", "value": 4, "unit": "m/s"})
            rows[-1]["observations"].append({"elementId": "wind_speed", "value": 90, "unit": "m/s"})
            written, _ = SyncService(session, Mock())._store_observations_batch({"SN123": station}, rows)
            session.commit()
            self.assertEqual(written, 4)
        with Session(engine) as session:
            observations = list(session.scalars(select(Observation)))
            self.assertEqual(sorted(r.value for r in observations if r.element_id == "air_temperature"), [-60, 12, 60])
            latest = session.get(StationLatest, station.id)
            self.assertEqual(latest.air_temperature, 12)
            self.assertEqual(latest.wind_speed, 4)
            self.assertEqual(latest.air_temperature_min, -60)
            self.assertEqual(latest.air_temperature_max, 60)
            self.assertLess(latest.observed_at.replace(tzinfo=timezone.utc), now)

    def test_latest_exports_hide_invalid_cached_values(self):
        engine = create_engine("sqlite:///:memory:")
        self.addCleanup(engine.dispose)
        Base.metadata.create_all(engine)
        factory = sessionmaker(engine)
        now = datetime.now(timezone.utc)
        with factory() as session:
            station = Station(source_id="SN123", provider="frost", latitude=62, longitude=10,
                              last_seen_at=now, last_observation_time=now + timedelta(days=1))
            session.add(station)
            session.flush()
            session.add(StationLatest(station_id=station.id, source_id="SN123", updated_at=now,
                observed_at=now + timedelta(days=1), air_temperature=-1310.72,
                air_temperature_min=-1310.72, air_temperature_max=999,
                wind_speed=90, wind_speed_observed_at=now + timedelta(days=1)))
            session.commit()
        for builder in (build_latest_geojson, build_latest_compact_geojson):
            with self.subTest(builder=builder.__name__):
                properties = builder(factory)["features"][0]["properties"]
                for key in ("air_temperature", "air_temperature_min", "air_temperature_max", "wind_speed", "observed_at", "minutes_since_observation"):
                    self.assertIsNone(properties.get(key))


if __name__ == "__main__":
    unittest.main()
