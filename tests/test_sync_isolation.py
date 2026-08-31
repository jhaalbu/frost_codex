import unittest
from datetime import datetime, timezone
from unittest.mock import Mock

from requests.exceptions import ConnectTimeout
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from frost_sync.db import Base
from frost_sync.models import Observation, Station, StationCapability, StationLatest
from frost_sync.nve_hydapi import NveHydApiSeriesSpec, NveHydApiStation
from frost_sync.service import SyncService


class SyncIsolationTest(unittest.TestCase):
    def run_scenario(self, snower_fails):
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        now = datetime.now(timezone.utc)

        def rows(source, element, value, unit):
            return [{"sourceId": source, "referenceTime": now.isoformat(),
                     "observations": [{"elementId": element, "value": value, "unit": unit}]}]

        frost = Mock()
        frost.fetch_latest_observations.return_value = rows("SN123", "air_temperature", 12, "degC")
        frost.fetch_snow_series_ids.return_value = []
        nve = Mock()
        spec = NveHydApiSeriesSpec("1.1.0", "1001", "discharge", 0, "m3/s")
        nve.fetch_stations.return_value = [NveHydApiStation(
            source_id="1.1.0", name="Test NVE", country="NO", county=None,
            municipality=None, masl=None, longitude=None, latitude=None,
            stationholder="NVE", active=True, series_specs=(spec,))]
        nve.fetch_latest_observations.return_value = rows("1.1.0", "discharge", 42, "m3/s")
        snower = Mock()
        if snower_fails:
            snower.fetch_latest_observations.side_effect = ConnectTimeout("Simulated Snower outage")
        else:
            snower.fetch_latest_observations.return_value = rows("snower-test", "snow_depth", 30, "cm")

        with Session(engine, expire_on_commit=False) as session:
            station = Station(source_id="SN123", provider="frost", last_seen_at=now)
            session.add_all([station, Station(source_id="snower-test", provider="snower",
                                             provider_context="{}", last_seen_at=now)])
            session.flush()
            session.add(StationCapability(station_id=station.id, element_id="air_temperature",
                                          available=True, first_seen_at=now, last_seen_at=now))
            session.commit()
            service = SyncService(session, frost, nve, snower)
            if snower_fails:
                with self.assertLogs("frost_sync.service", level="ERROR"):
                    summary = service.run_hourly_sync(page_limit=100)
            else:
                summary = service.run_hourly_sync(page_limit=100)

        # Read from a fresh session: ORM state alone does not prove persistence.
        with Session(engine) as session:
            latest = {row.source_id: row for row in session.scalars(select(StationLatest))}
            self.assertEqual(latest["SN123"].air_temperature, 12)
            self.assertEqual(latest["1.1.0"].discharge, 42)
            persisted = list(session.scalars(select(Observation)))
            self.assertEqual(len(persisted), 2 if snower_fails else 3)
            self.assertEqual(summary.observations_written, len(persisted))
            self.assertEqual(summary.station_errors, int(snower_fails))
            if snower_fails:
                self.assertNotIn("snower-test", latest)
            else:
                self.assertEqual(latest["snower-test"].snow_depth, 30)
        nve.fetch_latest_observations.assert_called_once_with([spec])
        snower.fetch_latest_observations.assert_called_once()
        engine.dispose()

    def test_all_providers_succeed(self):
        self.run_scenario(snower_fails=False)

    def test_snower_timeout_preserves_frost_and_nve(self):
        self.run_scenario(snower_fails=True)


if __name__ == "__main__":
    unittest.main()
