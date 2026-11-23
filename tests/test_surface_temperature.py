import json
import unittest
from datetime import datetime, timedelta, timezone
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

from frost_sync import web
from frost_sync.config import Settings
from frost_sync.db import Base, create_schema
from frost_sync.frost_api import FrostClient
from frost_sync.models import Observation, Station, StationCapability, StationLatest
from frost_sync.service import SyncService
from frost_sync.snower_api import SnowerClient, SnowerMonitor


class SurfaceTemperatureTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        self.addCleanup(self.engine.dispose)
        Base.metadata.create_all(self.engine)
        self.factory = sessionmaker(self.engine, expire_on_commit=False)
        self.now = datetime.now(timezone.utc).replace(microsecond=0)
        self.stamp = self.now - timedelta(hours=1)

    def row(self, source, value, stamp=None, element="surface_temperature", quality=0):
        return {"sourceId": source, "referenceTime": (stamp or self.stamp).isoformat(),
                "observations": [{"elementId": element, "value": value, "unit": "degC", "qualityCode": quality}]}

    def station(self, session, source="SN123", provider="frost"):
        station = Station(source_id=source, provider=provider, snower_active=True,
                          stationholder="Statens vegvesen", longitude=10, latitude=60,
                          provider_context=json.dumps({"permanent_id": "test"}), last_seen_at=self.now)
        session.add(station)
        session.flush()
        return station

    def test_frost_hourly_requests_surface_only_station_and_keeps_own_timestamp(self):
        frost = FrostClient("https://example.test", "test")
        self.addCleanup(frost.session.close)

        def fetch(path, params):
            if params.get("referencetime") != "latest":
                return []
            self.assertIn("surface_temperature", params["elements"].split(","))
            self.assertEqual(params["sources"], "SN123")
            return [self.row("SN123:0", 0),
                    self.row("SN123:0", 4, self.now, element="wind_speed")]

        with self.factory.begin() as session:
            station = self.station(session)
            session.add(StationCapability(station_id=station.id, element_id="surface_temperature",
                                         available=True, first_seen_at=self.now, last_seen_at=self.now))
        with self.factory() as session, patch.object(frost, "_get", side_effect=fetch):
            summary = SyncService(session, frost).run_hourly_sync(page_limit=1000)
            self.assertEqual(summary.station_errors, 0)
        with self.factory() as session:
            latest = session.scalars(select(StationLatest)).one()
            self.assertEqual(latest.surface_temperature, 0)
            self.assertEqual(latest.surface_temperature_unit, "degC")
            self.assertEqual(web._isoformat(latest.surface_temperature_observed_at), web._isoformat(self.stamp))
            self.assertEqual(web._isoformat(latest.observed_at), web._isoformat(self.now))

    def test_snower_requests_and_normalizes_surface_in_latest_and_history(self):
        client = SnowerClient("https://example.test", "snk_test")
        self.addCleanup(client.session.close)
        context = json.dumps({"permanent_id": "test"})
        monitor = SnowerMonitor("SNWR-test", "Test", 10, 60, context)
        reading = {"date_time": self.stamp.isoformat(), "value": -2,
                   "non_approximate_value": -2.35, "unit": "celsius"}
        with patch.object(client, "_post", return_value={"ir_temperature": reading}) as post:
            rows = client.fetch_latest_observations([monitor])
            self.assertIn("ir_temperature", post.call_args.args[1]["readings_types"])
        self.assertEqual(rows[0]["observations"], [{"elementId": "surface_temperature", "value": -2.35,
                                                  "unit": "degC", "qualityCode": None}])
        response = Mock(headers={})
        response.json.return_value = {self.stamp.isoformat(): {"ir_temperature": reading}}
        with patch.object(client, "_post_response", return_value=response) as post:
            history = client.fetch_observations_range(context, self.stamp, self.now, ["surface_temperature"])
            self.assertEqual(post.call_args.args[0], "/readings_list")
            self.assertEqual(post.call_args.args[1]["readings_types"], ["ir_temperature"])
        self.assertEqual(history, rows)
        self.assertEqual(client._normalize_last_reading_payload("SNWR-test", {}), [])
        self.assertEqual(client._normalize_last_reading_payload("SNWR-test", {"ir_temperature": None}), [])
        reading["value"] = reading["non_approximate_value"] = None
        self.assertEqual(client._normalize_last_reading_payload("SNWR-test", {"ir_temperature": reading}), [])

    def test_storage_exports_quality_and_surface_zero_for_both_providers(self):
        with self.factory.begin() as session:
            for source, provider in [("SN123", "frost"), ("SNWR-test", "snower")]:
                station = self.station(session, source, provider)
                service = SyncService(session, Mock())
                rows = [self.row(source, -5, self.stamp - timedelta(days=1)), self.row(source, 0),
                        self.row(source, 99, self.stamp + timedelta(minutes=1)),
                        self.row(source, float("nan"), self.stamp + timedelta(minutes=2)),
                        self.row(source, -3, self.stamp + timedelta(minutes=3), quality=5),
                        self.row(source, 2, self.now + timedelta(days=1))]
                service._store_observations_batch({source: station}, rows)
                session.flush()
                service._store_observations_batch({source: station}, [self.row(source, -4, self.stamp - timedelta(hours=1))])
                self.assertEqual(station.latest.surface_temperature, 0)
        for builder in [web.build_latest_geojson, web.build_latest_compact_geojson, web.build_latest_fme_geojson]:
            with self.subTest(builder=builder.__name__):
                features = builder(self.factory)["features"]
                self.assertEqual(len(features), 2)
                for feature in features:
                    properties = feature["properties"]
                    self.assertEqual(properties["surface_temperature"], 0)
                    self.assertEqual(properties["surface_temperature_observed_at"], web._isoformat(self.stamp))
                    if builder == web.build_latest_geojson:
                        self.assertTrue(properties["has_surface_temperature"])
                        self.assertEqual(properties["surface_temperature_unit"], "degC")
        self.assertEqual(len(web.build_latest_geojson(self.factory, has_filter="surface_temperature")["features"]), 2)
        for feature in web.build_latest_7d_geojson(self.factory)["features"]:
            self.assertEqual(feature["properties"]["surface_temperature_min_7d"], -5)
            self.assertEqual(feature["properties"]["surface_temperature_max_7d"], 0)
            self.assertEqual(feature["properties"]["surface_temperature_max_7d_time"], web._isoformat(self.stamp))
        with self.factory() as session:
            self.assertEqual(len(session.scalars(select(Observation)).all()), 6)

    def test_routes_for_single_and_multiple_stations_and_catalog(self):
        frost = Mock()
        frost.fetch_observations_range.return_value = [self.row("SN123:0", -1.5)]
        snower = Mock()
        snower.fetch_observations_range.return_value = [self.row("SNWR-test", 0)]
        with self.factory.begin() as session:
            self.station(session)
            self.station(session, "SNWR-test", "snower")
        settings = Settings("test-frost", None, "test-domain", "snk_test", proxy_api_key="test")
        with patch.object(web, "load_settings", return_value=settings), \
             patch.object(web, "create_session_factory", return_value=self.factory), \
             patch.object(web, "FrostClient", return_value=frost), \
             patch.object(web, "SnowerClient", return_value=snower):
            client = web.create_app().test_client()
        headers = {"X-API-Key": "test"}
        args = f"parameters=surface_temperature&from={web._isoformat(self.stamp)}&to={web._isoformat(self.now)}"
        single = {}
        for source, value in [("SN123", -1.5), ("SNWR-test", 0)]:
            response = client.get(f"/api/stations/{source}/timeseries?{args}", headers=headers)
            self.assertEqual(response.status_code, 200)
            single[source] = response.json["series"]
            series = single[source]["surface_temperature"]
            self.assertEqual(series["unit"], "degC")
            self.assertEqual([p["value"] for p in series["data"]], [value])
        multi = client.get(f"/api/timeseries?stations=SN123,SNWR-test&{args}", headers=headers)
        self.assertEqual(multi.status_code, 200)
        self.assertEqual(multi.json["errors"], [])
        for result in multi.json["stations"]:
            self.assertEqual(result["series"], single[result["station"]["source_id"]])
        self.assertEqual(frost.fetch_observations_range.call_args.kwargs["elements"], ["surface_temperature"])
        self.assertEqual(snower.fetch_observations_range.call_args.kwargs["logical_parameter_ids"], ["surface_temperature"])
        catalog = client.get("/api/parameters", headers=headers).json
        self.assertEqual(next(p for p in catalog if p["id"] == "surface_temperature")["label"], "Overflatetemperatur")

    def test_timeseries_and_cached_latest_reject_invalid_values(self):
        rows = [self.row("SN123", -60, self.stamp), self.row("SN123", 0, self.stamp + timedelta(minutes=1)),
                self.row("SN123", 60, self.stamp + timedelta(minutes=2)),
                self.row("SN123", 61, self.stamp + timedelta(minutes=3)),
                self.row("SN123", float("inf"), self.stamp + timedelta(minutes=4)),
                self.row("SN123", 5, self.now + timedelta(days=1))]
        points = web._build_direct_points("surface_temperature", ["surface_temperature"], rows, self.stamp, self.now + timedelta(days=2))
        self.assertEqual([point["value"] for point in points], [-60, 0, 60])
        with self.factory.begin() as session:
            station = self.station(session)
            session.add(StationLatest(station_id=station.id, source_id=station.source_id, updated_at=self.now,
                                      observed_at=self.stamp, surface_temperature=99,
                                      surface_temperature_unit="degC", surface_temperature_observed_at=self.stamp))
        properties = web.build_latest_geojson(self.factory)["features"][0]["properties"]
        for field in ["surface_temperature", "surface_temperature_unit", "surface_temperature_observed_at"]:
            self.assertIsNone(properties[field])

    def test_existing_database_upgrade_backfills_surface_and_is_idempotent(self):
        with self.factory.begin() as session:
            station = self.station(session)
            session.add(StationLatest(station_id=station.id, source_id=station.source_id, updated_at=self.now))
            session.add(Observation(station_id=station.id, reference_time=self.stamp,
                                    element_id="surface_temperature", value=-3, unit="degC", fetched_at=self.now))
        with self.engine.begin() as connection:
            for column in ["surface_temperature", "surface_temperature_unit", "surface_temperature_observed_at"]:
                connection.execute(text(f"ALTER TABLE station_latest DROP COLUMN {column}"))
        with patch("frost_sync.db.create_engine", return_value=self.engine):
            create_schema("sqlite:///:memory:")
            create_schema("sqlite:///:memory:")
        with self.factory() as session:
            latest = session.scalars(select(StationLatest)).one()
            self.assertEqual(latest.surface_temperature, -3)
            self.assertEqual(latest.surface_temperature_unit, "degC")
            self.assertEqual(web._isoformat(latest.surface_temperature_observed_at), web._isoformat(self.stamp))

    def test_delta_t_in_latest_routes_and_station_details(self):
        samples = [("SN123", "frost", -5, -12, 7),
                   ("SNWR-test", "snower", -2, 1, -3),
                   ("SN124", "frost", 0, 0, 0)]
        with self.factory.begin() as session:
            for source, provider, air, surface, expected in samples:
                station = self.station(session, source, provider)
                session.add(StationLatest(
                    station_id=station.id, source_id=source, observed_at=self.stamp, updated_at=self.now,
                    air_temperature=air, air_temperature_unit="degC", air_temperature_observed_at=self.stamp,
                    surface_temperature=surface, surface_temperature_unit="degC",
                    surface_temperature_observed_at=self.stamp - timedelta(minutes=15)))
        expected_values = {source: expected for source, provider, air, surface, expected in samples}
        with TemporaryDirectory() as folder:
            settings = Settings(None, None, None, None, proxy_api_key="test", geojson_cache_dir=folder)
            with patch.object(web, "load_settings", return_value=settings), \
                 patch.object(web, "create_session_factory", return_value=self.factory):
                client = web.create_app().test_client()
            for suffix in ["latest.geojson", "latest.compact.geojson", "latest.fme.geojson"]:
                with self.subTest(suffix=suffix):
                    response = client.get("/api/stations/" + suffix)
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual({f["properties"]["source_id"]: f["properties"]["delta_t"]
                                      for f in response.json["features"]}, expected_values)
            for source, expected in expected_values.items():
                response = client.get("/api/stations/" + source, headers={"X-API-Key": "test"})
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json["latest"]["delta_t"], expected)

    def test_delta_t_missing_invalid_and_mismatched_measurements(self):
        cases = [
            ({"air_temperature": None}, None),
            ({"surface_temperature": None}, None),
            ({"air_temperature_observed_at": None}, None),
            ({"surface_temperature_observed_at": None}, None),
            ({"surface_temperature_observed_at": self.stamp - timedelta(minutes=15, seconds=1)}, None),
            ({"air_temperature_observed_at": self.stamp - timedelta(minutes=15, seconds=1)}, None),
            ({"air_temperature_observed_at": self.now + timedelta(minutes=1)}, None),
            ({"surface_temperature_observed_at": self.now + timedelta(minutes=1)}, None),
            ({"air_temperature": float("nan")}, None),
            ({"surface_temperature": float("inf")}, None),
            ({"surface_temperature": 99}, None),
            ({"air_temperature": -40}, None),  # Existing road-station sentinel filter.
            ({"air_temperature_unit": "K"}, None),
            ({"surface_temperature_unit": None}, None),
            ({"air_temperature": -5.12, "surface_temperature": -12.34}, 7.22),
            ({"air_temperature_observed_at": self.stamp.replace(tzinfo=None)}, 7),
            ({"air_temperature_observed_at": self.stamp - timedelta(days=3),
              "surface_temperature_observed_at": self.stamp - timedelta(days=3)}, 7),
        ]
        with self.factory.begin() as session:
            for index, (changes, expected) in enumerate(cases):
                station = self.station(session, f"SN{index}")
                values = dict(air_temperature=-5, air_temperature_unit="degC", air_temperature_observed_at=self.stamp,
                              surface_temperature=-12, surface_temperature_unit="degC", surface_temperature_observed_at=self.stamp)
                values.update(changes)
                session.add(StationLatest(station_id=station.id, source_id=station.source_id,
                                          observed_at=self.stamp, updated_at=self.now, **values))
        for builder in [web.build_latest_geojson, web.build_latest_compact_geojson, web.build_latest_fme_geojson]:
            properties = {f["properties"]["source_id"]: f["properties"] for f in builder(self.factory)["features"]}
            for index, (changes, expected) in enumerate(cases):
                with self.subTest(builder=builder.__name__, changes=changes):
                    result = properties[f"SN{index}"]
                    self.assertEqual(result.get("delta_t"), expected)
                    if expected is None:
                        if builder == web.build_latest_compact_geojson:
                            self.assertNotIn("delta_t", result)
                        else:
                            self.assertIn("delta_t", result)


if __name__ == "__main__":
    unittest.main()
