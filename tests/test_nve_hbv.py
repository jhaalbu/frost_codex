import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

import requests
from flask import Flask
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

from frost_sync.config import Settings
from frost_sync.db import Base, create_schema
from frost_sync.models import Observation, Station, StationLatest
from frost_sync.nve_hydapi import NveHydApiSeriesSpec
from frost_sync.nve_veps import NveVepsClient, next_hbv_reference_time
from frost_sync.service import SyncService
from frost_sync import web


UTC = timezone.utc
NOW = datetime(2026, 10, 28, 12, tzinfo=UTC)
TARGET = datetime(2026, 10, 29, 6, tzinfo=UTC)


def model_row(source="12.215.0", stamp=TARGET, value=6.822):
    return {"sourceId": source, "referenceTime": stamp.isoformat(), "observations": [
        {"elementId": "discharge_hbv", "value": value, "unit": "m3/s", "qualityCode": None}
    ]}


class VepsClientTest(unittest.TestCase):
    def client(self, payload):
        client = NveVepsClient()
        client.session = Mock()
        client.session.get.return_value.json.return_value = payload
        return client

    def test_parser_filters_invalid_data_but_keeps_future_zero(self):
        def point(value, **extras):
            return {"source": "hf_hbv", "reference_time": TARGET.isoformat(),
                    "values": [value], **extras}
        payload = {"locations": [{"station_id": "12.215.0", "elements": [{
            "parameter_key": 6004, "method_name": "mean", "unit": "m^3/s", "data": [
                point(0), point(-1), point(float("nan")), point(float("inf")),
                point(None), point(True), point(8, source="odm_hbv"),
                point(8, values=[1, 2]), point(8, reference_time="bad"),
                point(8, reference_time="2026-10-29T06:00:00"),
                point(8, reference_time="2026-11-01T06:00:00Z"),
            ]}]}]}
        client = self.client(payload)
        rows = client.fetch_observations_range(["12.215.0"], NOW, TARGET)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["observations"][0]["value"], 0)
        params = client.session.get.call_args.kwargs["params"]
        self.assertEqual(params["dataSource"], "hf_hbv")
        self.assertEqual(params["timeResolutions"], "P1D")
        self.assertNotIn("X-API-Key", params)

    def test_empty_payload_batching_and_errors(self):
        client = self.client({"locations": []})
        self.assertEqual(client.fetch_observations_range([str(x) for x in range(26)], NOW, TARGET), [])
        self.assertEqual(client.session.get.call_count, 2)
        client.session.get.side_effect = requests.Timeout("outage")
        with self.assertRaises(RuntimeError):
            client.fetch_observations_range(["12.215.0"], NOW, TARGET)
        client.session.get.side_effect = None
        client.session.get.return_value.json.return_value = {"error": "bad request"}
        with self.assertRaises(RuntimeError):
            client.fetch_observations_range(["12.215.0"], NOW, TARGET)

    def test_target_uses_norwegian_date_including_dst(self):
        self.assertEqual(next_hbv_reference_time(NOW), TARGET)
        # UTC is still Oct 28; Norwegian date is already Oct 29.
        self.assertEqual(next_hbv_reference_time(datetime(2026, 10, 28, 23, 30, tzinfo=UTC)),
                         datetime(2026, 10, 30, 6, tzinfo=UTC))
        self.assertEqual(next_hbv_reference_time(datetime(2026, 3, 28, 23, 30, tzinfo=UTC)),
                         datetime(2026, 3, 30, 6, tzinfo=UTC))


class HbvIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.factory = sessionmaker(self.engine, expire_on_commit=False)
        self.addCleanup(self.engine.dispose)
        with self.factory() as session:
            station = Station(source_id="12.215.0", provider="nve_hydapi", name="Storeskar",
                              longitude=8.33, latitude=60.89, last_seen_at=NOW,
                              last_observation_time=NOW)
            session.add(station)
            session.flush()
            self.station_id = station.id
            session.add(StationLatest(station_id=station.id, source_id=station.source_id,
                                     observed_at=NOW, discharge=3, discharge_observed_at=NOW,
                                     updated_at=NOW))
            session.add(Observation(station_id=station.id, reference_time=NOW,
                                    element_id="discharge", value=3, unit="m3/s", fetched_at=NOW))
            session.commit()

    def sync(self, rows=None, error=None):
        veps = Mock()
        veps.fetch_observations_range.return_value = rows or []
        veps.fetch_observations_range.side_effect = error
        with self.factory() as session:
            result = SyncService(session, Mock(), veps_client=veps).sync_hbv_forecasts(NOW)
        return result

    def test_sync_is_separate_and_requires_exact_tomorrow(self):
        self.assertEqual(self.sync([model_row(), model_row(stamp=TARGET + timedelta(days=1), value=99)]), (1, 0))
        with self.factory() as session:
            latest = session.get(StationLatest, self.station_id)
            self.assertEqual(latest.discharge_hbv, 6.822)
            self.assertEqual(latest.discharge, 3)
            self.assertEqual(latest.observed_at.replace(tzinfo=UTC), NOW)
            self.assertEqual(latest.updated_at.replace(tzinfo=UTC), NOW)
            self.assertEqual(session.get(Station, self.station_id).last_observation_time.replace(tzinfo=UTC), NOW)
            self.assertEqual([o.element_id for o in session.scalars(select(Observation))], ["discharge"])
        # A successful response without tomorrow's point clears yesterday's model value.
        self.sync([model_row(stamp=TARGET + timedelta(days=1))])
        with self.factory() as session:
            self.assertIsNone(session.get(StationLatest, self.station_id).discharge_hbv)

    def test_hourly_job_runs_hbv_after_observation_commit(self):
        frost = Mock()
        frost.fetch_latest_observations.return_value = []
        frost.fetch_snow_series_ids.return_value = []
        with self.factory() as session:
            session.add(Station(source_id="SN123", provider="frost", last_seen_at=NOW))
            session.commit()
            service = SyncService(session, frost, veps_client=Mock())
            with patch.object(service, "sync_hbv_forecasts", return_value=(1, 0)) as sync:
                summary = service.run_hourly_sync(page_limit=100)
                sync.assert_called_once()
                self.assertGreaterEqual(summary.latest_updated, 1)

    def test_veps_outage_preserves_committed_observations_and_previous_fetch(self):
        self.sync([model_row()])
        with self.assertLogs("frost_sync.service", level="ERROR"):
            self.assertEqual(self.sync(error=RuntimeError("offline")), (0, 1))
        with self.factory() as session:
            latest = session.get(StationLatest, self.station_id)
            self.assertEqual(latest.discharge, 3)
            self.assertEqual(latest.discharge_hbv, 6.822)
            self.assertEqual(len(list(session.scalars(select(Observation)))), 1)

    def test_every_latest_variant_and_date_rollover(self):
        self.sync([model_row()])
        builders = [web.build_latest_geojson, web.build_latest_compact_geojson,
                    web.build_latest_fme_geojson, web.build_latest_7d_geojson]
        with patch("frost_sync.web.datetime", wraps=datetime) as clock:
            clock.now.return_value = NOW
            for builder in builders:
                with self.subTest(builder=builder.__name__):
                    payload = builder(self.factory)
                    json.dumps(payload, allow_nan=False)
                    props = payload["features"][0]["properties"]
                    self.assertEqual(props["discharge_hbv"], 6.822)
                    self.assertEqual(props["discharge_hbv_forecast_time"], "2026-10-29T06:00:00Z")
                    self.assertEqual(props["discharge_hbv_model"], "hf_hbv")
                    self.assertEqual(props["discharge_hbv_unit"], "m3/s")
            self.assertEqual(len(web.build_latest_geojson(self.factory, "discharge_hbv")["features"]), 1)
            clock.now.return_value = NOW + timedelta(days=1)
            self.assertIsNone(web.build_latest_geojson(self.factory)["features"][0]["properties"]["discharge_hbv"])
            self.assertNotIn("discharge_hbv", web.build_latest_compact_geojson(self.factory)["features"][0]["properties"])
            self.assertEqual(web.build_latest_geojson(self.factory, "discharge_hbv")["features"], [])

    def test_model_only_station_is_included_in_7d_without_fake_observed_time(self):
        with self.factory() as session:
            other = Station(source_id="2.2.0", provider="nve_hydapi", longitude=9, latitude=61, last_seen_at=NOW)
            session.add(other)
            session.commit()
        self.sync([model_row(source="2.2.0")])
        with patch("frost_sync.web.datetime", wraps=datetime) as clock:
            clock.now.return_value = NOW
            result = web.build_latest_7d_geojson(self.factory)
            props = next(f["properties"] for f in result["features"] if f["properties"]["source_id"] == "2.2.0")
            self.assertEqual(props["discharge_hbv"], 6.822)
            self.assertNotIn("last_observation_time", props)
            self.assertNotIn("discharge_max_7d", props)

    def test_observed_and_model_series_and_both_routes(self):
        observed = {"sourceId": "12.215.0", "referenceTime": NOW.isoformat(),
                    "observations": [{"elementId": "discharge", "value": 3, "unit": "m3/s"}]}
        future_observed = {**observed, "referenceTime": TARGET.isoformat()}
        hydapi = Mock()
        hydapi.fetch_series_specs_for_station.return_value = [
            NveHydApiSeriesSpec("12.215.0", "1001", "discharge", 0, "m3/s")]
        hydapi.fetch_observations_range.return_value = [observed, future_observed]
        hydapi.veps_client.fetch_observations_range.return_value = [model_row()]
        settings = Settings(None, "test", None, None)
        with patch.object(web, "load_settings", return_value=settings), \
             patch.object(web, "create_session_factory", return_value=self.factory), \
             patch.object(web, "NveHydApiClient", return_value=hydapi), \
             patch("frost_sync.web.datetime", wraps=datetime) as clock:
            clock.now.return_value = NOW
            app = web.create_app()
            client = app.test_client()
            args = "parameters=discharge,discharge_hbv&from=2026-10-28T00:00:00Z&to=2026-10-30T00:00:00Z"
            single = client.get("/api/stations/12.215.0/timeseries?" + args)
            self.assertEqual(single.status_code, 200)
            series = single.json["series"]
            self.assertEqual([p["value"] for p in series["discharge"]["data"]], [3])
            self.assertEqual([p["value"] for p in series["discharge_hbv"]["data"]], [6.822])
            self.assertEqual(series["discharge_hbv"]["data_type"], "model")
            self.assertNotIn("thresholds", series["discharge_hbv"])
            multi = client.get("/api/timeseries?stations=12.215.0,unknown&" + args)
            self.assertEqual(multi.status_code, 200)
            self.assertEqual(multi.json["stations"][0]["series"], series)
            self.assertEqual(multi.json["missing"], ["unknown"])
            self.assertIn("discharge_hbv", [p["id"] for p in client.get("/api/parameters").json])
            hydapi.veps_client.fetch_observations_range.return_value = []
            self.assertEqual(client.get("/api/stations/12.215.0/timeseries?" + args).json["series"]["discharge_hbv"]["data"], [])

    def test_hbv_only_plot_does_not_require_observation_series(self):
        hydapi = Mock()
        hydapi.veps_client.fetch_observations_range.return_value = [model_row()]
        result = web._fetch_nve_timeseries_rows(hydapi, "12.215.0", ["discharge_hbv"], NOW, TARGET)
        self.assertEqual(result, [model_row()])
        hydapi.fetch_series_specs_for_station.assert_not_called()
        hydapi.fetch_observations_range.assert_not_called()

    def test_frost_requests_do_not_receive_the_veps_parameter(self):
        frost = Mock()
        self.assertEqual(web._fetch_frost_timeseries_rows(
            frost, "SN123", ["discharge_hbv"], NOW, TARGET), [])
        frost.fetch_observations_range.assert_not_called()
        web._fetch_frost_timeseries_rows(frost, "SN123", ["air_temperature", "discharge_hbv"], NOW, TARGET)
        elements = frost.fetch_observations_range.call_args.kwargs["elements"]
        self.assertNotIn("discharge_hbv", elements)
        self.assertIn("air_temperature", elements)


class HbvMigrationAndCacheTest(unittest.TestCase):
    def test_existing_schema_upgrade_is_idempotent(self):
        engine = create_engine("sqlite:///:memory:")
        self.addCleanup(engine.dispose)
        Base.metadata.create_all(engine)
        with engine.begin() as connection:
            for column in ["discharge_hbv", "discharge_hbv_forecast_time", "discharge_hbv_fetched_at"]:
                connection.execute(text(f"ALTER TABLE station_latest DROP COLUMN {column}"))
        with patch("frost_sync.db.create_engine", return_value=engine):
            create_schema("sqlite:///:memory:")
            create_schema("sqlite:///:memory:")
        with engine.connect() as connection:
            connection.execute(select(StationLatest)).all()

    def test_cache_from_previous_norwegian_date_is_bypassed_including_gzip(self):
        with TemporaryDirectory(dir=Path(__file__).resolve().parent) as folder:
            path = Path(folder) / "latest.geojson"
            path.write_text('{}', encoding="utf-8")
            path.with_suffix('.geojson.gz').write_bytes(b'old')
            os.utime(path, (NOW.timestamp() - 86400,) * 2)
            settings = Settings(None, None, None, None, geojson_cache_dir=folder)
            app = Flask(__name__)
            with app.test_request_context(headers={"Accept-Encoding": "gzip"}), \
                 patch("frost_sync.web.datetime", wraps=datetime) as clock:
                clock.now.return_value = NOW
                self.assertIsNone(web._cached_geojson_response(settings, "latest.geojson"))


if __name__ == "__main__":
    unittest.main()
