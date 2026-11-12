import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from frost_sync.db import Base
from frost_sync.frost_api import FrostClient
from frost_sync.models import Observation, Station, StationCapability, StationLatest
from frost_sync.service import SyncService


class FrostSnowSyncTest(unittest.TestCase):
    def test_hourly_sync_discovers_and_keeps_snow_at_its_own_timestamp(self):
        engine = create_engine("sqlite:///:memory:")
        self.addCleanup(engine.dispose)
        Base.metadata.create_all(engine)
        now = datetime.now(timezone.utc).replace(microsecond=0)
        snow_time = now - timedelta(hours=25)
        source = "SN60310"
        element = "surface_snow_thickness"

        def row(stamp, *observations):
            return {"sourceId": f"{source}:0", "referenceTime": stamp.isoformat(), "observations": list(observations)}

        def snow(value, resolution, offset, quality=0):
            return {"elementId": element, "value": value, "unit": "cm", "timeResolution": resolution,
                    "timeOffset": offset, "qualityCode": quality}

        wind = [row(now, {"elementId": "wind_speed", "value": 6, "unit": "m/s"})]

        def fetch(path, params):
            if params["elements"] != element:
                return wind
            self.assertNotIn("timeresolutions", params)
            self.assertEqual(params["timeoffsets"], "default")
            self.assertEqual(params["levels"], "default")
            if path == "/observations/availableTimeSeries/v0.jsonld":
                self.assertNotIn("sources", params)
                return [{"sourceId": f"{source}:0"}, {"sourceId": "SN99999:0"}]
            self.assertEqual(params["sources"], source)
            self.assertEqual(params["qualities"], "0,1,2,3,4")
            return [
                row(snow_time - timedelta(hours=22), snow(10, "P1D", "PT6H")),
                row(snow_time, snow(15, "PT10M", "PT0H"), snow(16, "PT1H", "PT0H"), snow(14, "P1D", "PT6H")),
                row(snow_time + timedelta(minutes=10), snow(99, "PT10M", "PT0H", quality=5)),
            ]

        frost = FrostClient("https://example.test", "test")
        self.addCleanup(frost.session.close)
        with Session(engine, expire_on_commit=False) as session:
            station = Station(source_id=source, provider="frost", last_seen_at=now)
            other = Station(source_id="SN123", provider="frost", last_seen_at=now)
            session.add_all([station, other])
            session.flush()
            session.add_all([
                StationCapability(station_id=station.id, element_id="wind_speed", available=True,
                                  first_seen_at=now, last_seen_at=now),
                StationCapability(station_id=other.id, element_id=element, available=True,
                                  first_seen_at=now, last_seen_at=now),
            ])
            session.commit()
            with patch.object(frost, "_get", side_effect=fetch):
                summary = SyncService(session, frost).run_hourly_sync(page_limit=1000)
            self.assertEqual(summary.station_errors, 0)
            self.assertEqual(summary.observations_written, 3)

        with Session(engine) as session:
            latest = session.scalars(select(StationLatest).where(StationLatest.source_id == source)).one()
            self.assertEqual(latest.wind_speed, 6)
            self.assertEqual(latest.observed_at.replace(tzinfo=timezone.utc), now)
            self.assertEqual(latest.snow_depth, 15)
            self.assertEqual(latest.snow_depth_unit, "cm")
            self.assertEqual(latest.snow_depth_observed_at.replace(tzinfo=timezone.utc), snow_time)
            self.assertEqual(latest.snow_depth_change, 5)
            history = session.scalars(select(Observation).where(Observation.element_id == element)
                                      .order_by(Observation.reference_time)).all()
            self.assertEqual([(item.value, item.time_offset) for item in history], [(10, "PT6H"), (15, "PT0H")])
            capability = session.scalars(select(StationCapability).where(
                StationCapability.station_id == latest.station_id, StationCapability.element_id == element)).one()
            self.assertTrue(capability.available)

            # Repair a cached value cleared by the previous 24-hour window, even with no fresh snow data.
            latest.snow_depth = latest.snow_depth_unit = None
            session.commit()
            with patch.object(frost, "_get", side_effect=[wind, []]):
                SyncService(session, frost).run_hourly_sync(page_limit=1000)
            session.refresh(latest)
            self.assertEqual(latest.snow_depth, 15)
            self.assertEqual(latest.snow_depth_observed_at.replace(tzinfo=timezone.utc), snow_time)
