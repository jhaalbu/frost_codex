import json
import logging
import unittest
from datetime import datetime, timezone
from unittest.mock import Mock

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from frost_sync.db import Base
from frost_sync.frost_api import FrostSource
from frost_sync.models import NveDischargeFloodThreshold, Station
from frost_sync.netatmo_api import NetatmoMonitor
from frost_sync.nve_hydapi import NveDischargeFloodThresholds, NveHydApiSeriesSpec, NveHydApiStation
from frost_sync.service import SyncService
from frost_sync.skred_api import SkredMonitor
from frost_sync.snower_api import SnowerMonitor
from frost_sync.tilsig_api import TilsigMonitor
from frost_sync.wyssen_api import WyssenMonitor


class MetadataLoggingTest(unittest.TestCase):
    def setUp(self):
        engine = create_engine("sqlite:///:memory:")
        self.addCleanup(engine.dispose)
        Base.metadata.create_all(engine)
        self.factory = sessionmaker(engine, expire_on_commit=False)
        self.frost = Mock()
        self.frost.fetch_sources.return_value = []
        self.frost.fetch_capability_source_ids.return_value = set()

    def test_all_providers_report_new_and_existing_stations_after_commit(self):
        frost_sources = [FrostSource(f"SN{i}", "Frost", None, "NO", None, None,
                                     None, 7, 61, None, None) for i in (1, 2)]
        self.frost.fetch_sources.return_value = frost_sources
        nve = Mock()
        nve_stations = [NveHydApiStation(f"1.{i}.0", "NVE", "NO", None, None,
                                        None, 7, 61, "NVE", True) for i in (1, 2)]
        nve.fetch_stations.return_value = nve_stations
        nve.fetch_series_specs_for_stations.return_value = []
        provider_sources = {"frost": frost_sources, "nve_hydapi": nve_stations}
        clients = {}
        for provider, monitor_type, prefix in [
            ("snower", SnowerMonitor, "SNWR-"), ("tilsig", TilsigMonitor, "TILSIG-"),
            ("wyssen", WyssenMonitor, "WYSSEN-"), ("skred", SkredMonitor, "SKRED-"),
            ("netatmo", NetatmoMonitor, "NETATMO-"),
        ]:
            context = json.dumps({"device_id": "70:ee:50:00:00:01",
                                  "modules": [{"module_id": "outdoor", "type": "NAModule1"}]})
            monitors = [monitor_type(f"{prefix}{i}", provider, 7, 61, context) for i in (1, 2)]
            client = Mock()
            client.fetch_stations.return_value = monitors
            clients[f"{provider}_client"] = client
            provider_sources[provider] = monitors

        with self.factory() as session:
            for provider, sources in provider_sources.items():
                session.add(Station(source_id=sources[0].source_id, provider=provider,
                                    last_seen_at=datetime.now(timezone.utc)))
            # Stored stations not returned by a provider must not count as updated.
            session.add(Station(source_id="SN-not-returned", provider="frost",
                                last_seen_at=datetime.now(timezone.utc)))
            session.commit()
            service = SyncService(session, self.frost, nve_hydapi_client=nve, **clients)
            for registered, updated in [(1, 1), (0, 2)]:
                with self.assertLogs("frost_sync.service", level="INFO") as logs:
                    summary = service.sync_metadata(100)
                messages = [r.getMessage() for r in logs.records if r.getMessage().startswith("Metadata ")]
                self.assertEqual(messages, [
                    f"Metadata {provider}: registered={registered} updated={updated} total=2 failed=0"
                    for provider in provider_sources
                ])
                self.assertEqual(summary.stations_seen, 14)
                self.assertEqual(summary.station_errors, 0)
                with self.factory() as verify:
                    self.assertEqual(len(verify.scalars(select(Station)).all()), 15)

    def test_empty_and_failed_discovery_are_distinguished(self):
        netatmo = Mock()
        netatmo.fetch_stations.side_effect = RuntimeError("Discovery failed")
        with self.factory() as session, self.assertLogs("frost_sync.service", level="INFO") as logs:
            summary = SyncService(session, self.frost, netatmo_client=netatmo).sync_metadata(100)
        messages = [r.getMessage() for r in logs.records if r.getMessage().startswith("Metadata ")]
        self.assertEqual(messages, [
            "Metadata frost: registered=0 updated=0 total=0 failed=0",
            "Metadata netatmo: registered=0 updated=0 total=0 failed=1",
        ])
        self.assertEqual(summary.station_errors, 1)

    def test_flood_thresholds_have_one_summary_and_keep_failure_details_at_debug(self):
        nve = Mock()
        specs = [NveHydApiSeriesSpec(source, "1001", "discharge", 0, "m3/s", version)
                 for source, version in [("1.1.0", 1), ("1.2.0", 1), ("75.43.0", 1),
                                         ("75.44.0", 1), ("1.5.0", None), ("1.6.0", 1)]]

        def fetch(source_id, series_version):
            if source_id in {"75.43.0", "75.44.0"}:
                raise RuntimeError("NVE Chartserver returned incomplete flood thresholds: qc5, qc50, qcm")
            return NveDischargeFloodThresholds(source_id, series_version, 10, 20, 30)

        nve.fetch_discharge_flood_thresholds.side_effect = fetch
        with self.factory() as session:
            stations = {spec.source_id: Station(source_id=spec.source_id, provider="nve_hydapi",
                                               last_seen_at=datetime.now(timezone.utc)) for spec in specs[:-1]}
            session.add_all(stations.values())
            session.flush()
            service = SyncService(session, self.frost, nve_hydapi_client=nve)
            for source in ("1.2.0", "75.43.0"):
                service._upsert_nve_discharge_flood_threshold(
                    stations[source], NveDischargeFloodThresholds(source, 1, 10, 20, 30),
                )
            session.commit()
            with self.assertLogs("frost_sync.service", level="INFO") as logs:
                changed = service._sync_nve_discharge_flood_thresholds(specs, stations)
                session.commit()
            self.assertEqual(changed, 1)
            self.assertEqual([r.getMessage() for r in logs.records], [
                "NVE flood thresholds: updated=1 unchanged=1 failed=2 skipped=2",
            ])
            self.assertEqual(logs.records[0].levelno, logging.INFO)
            # Previously stored thresholds survive missing values at the provider.
            self.assertEqual(session.get(NveDischargeFloodThreshold, stations["75.43.0"].id).discharge_q50, 30)

            with self.assertLogs("frost_sync.service", level="DEBUG") as logs:
                service._sync_nve_discharge_flood_thresholds(specs, stations)
            failures = [r for r in logs.records if "Failed to sync" in r.getMessage()]
            self.assertEqual(len(failures), 2)
            self.assertTrue(all(r.levelno == logging.DEBUG for r in failures))
            self.assertEqual(logs.records[-1].getMessage(),
                             "NVE flood thresholds: updated=0 unchanged=2 failed=2 skipped=2")

            with self.assertLogs("frost_sync.service", level="INFO") as logs:
                self.assertEqual(service._sync_nve_discharge_flood_thresholds([], stations), 0)
            self.assertEqual(logs.records[0].getMessage(),
                             "NVE flood thresholds: updated=0 unchanged=0 failed=0 skipped=0")


if __name__ == "__main__":
    unittest.main()
