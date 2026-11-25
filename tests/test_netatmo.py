import copy
from contextlib import closing
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch

import requests
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from frost_sync import web
from frost_sync.config import Settings, load_settings
from frost_sync.db import Base
from frost_sync.models import Observation, Station, StationLatest
from frost_sync.netatmo_api import NetatmoClient, NetatmoAuthError, create_netatmo_client
from frost_sync.service import SyncService


NOW = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
DEVICE = {
    "_id": "70:ee:50:00:00:01", "station_name": "Test (Indoor)",
    "place": {"location": [7.3, 61.9], "altitude": 885, "country": "NO"},
    "dashboard_data": {"time_utc": NOW.timestamp(), "Temperature": 22},
    "modules": [
        {"_id": "02:00:00:00:00:01", "type": "NAModule1",
         "dashboard_data": {"time_utc": NOW.timestamp(), "Temperature": 0}},
        {"_id": "03:00:00:00:00:01", "type": "NAModule4",
         "dashboard_data": {"time_utc": NOW.timestamp(), "Temperature": 25}},
        {"_id": "06:00:00:00:00:01", "type": "NAModule2",
         "dashboard_data": {"time_utc": (NOW - timedelta(minutes=5)).timestamp(),
                            "WindStrength": 36, "WindAngle": 0, "GustStrength": 72}},
        {"_id": "05:00:00:00:00:01", "type": "NAModule3",
         "dashboard_data": {"time_utc": NOW.timestamp(), "Rain": 0.1, "sum_rain_1": 3}},
    ],
}


def response(payload, status=200, headers=None):
    result = Mock(status_code=status, ok=200 <= status < 300, headers=headers or {})
    result.json.return_value = payload
    return result


class NetatmoClientTest(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = str(Path(self.temp.name) / "tokens.db")
        self.client = self.new_client()

    def new_client(self, refresh="seed", access="old-access"):
        client = NetatmoClient("client", "secret", refresh, access, self.path)
        self.addCleanup(client.session.close)
        return client

    def test_config_accepts_existing_names_and_prefers_canonical_names(self):
        env = {"NETAMO_CLIENT_ID": "legacy-client", "NETATMO_SECRET_ID": "secret",
               "NETAMO_ACCESS_TOKEN": "access", "NETAMO_REFRESH_TOKEN": "refresh"}
        with patch.dict("os.environ", env, clear=True), patch("frost_sync.config._load_dotenv"):
            settings = load_settings()
            self.assertEqual(settings.netatmo_client_id, "legacy-client")
            self.assertEqual(settings.netatmo_client_secret, "secret")
            self.assertEqual(settings.netatmo_access_token, "access")
            with patch.dict("os.environ", {"NETATMO_CLIENT_ID": "canonical"}):
                self.assertEqual(load_settings().netatmo_client_id, "canonical")
        self.assertIsNone(create_netatmo_client(Settings(None, None, None, None)))
        with self.assertRaises(NetatmoAuthError):
            create_netatmo_client(Settings(None, None, None, None, netatmo_client_id="client"))

    def test_rotation_survives_restart_and_does_not_repeat_stale_refresh(self):
        with patch.object(self.client.session, "request", side_effect=[
            response({"error": {"code": 2}}, 403),
            response({"access_token": "new-access", "refresh_token": "new-refresh", "expires_in": 3600}),
            response({"body": {"devices": []}}),
        ]) as request:
            self.assertEqual(self.client.fetch_stations(), [])
        self.assertEqual(request.call_args_list[1].kwargs["data"]["refresh_token"], "seed")
        self.assertEqual(request.call_args.kwargs["headers"], {"Authorization": "Bearer new-access"})
        restarted = self.new_client()
        with patch.object(restarted.session, "request", return_value=response({"body": {"devices": []}})) as request:
            restarted.fetch_stations()
        self.assertEqual(request.call_count, 1)
        self.assertEqual(request.call_args.kwargs["headers"], {"Authorization": "Bearer new-access"})
        with closing(sqlite3.connect(self.path)) as db:
            saved = json.loads(db.execute("SELECT payload FROM oauth_token").fetchone()[0])
        self.assertEqual(saved["refresh_token"], "new-refresh")
        self.assertNotIn("secret", saved.values())

    def test_concurrent_workers_refresh_once_and_explicit_new_seed_is_used(self):
        clients = [self.new_client(access=None), self.new_client(access=None)]
        with patch.object(NetatmoClient, "_http", return_value=response({
            "access_token": "fresh", "refresh_token": "rotated", "expires_in": 3600,
        })) as http, ThreadPoolExecutor(max_workers=2) as pool:
            self.assertEqual(list(pool.map(lambda c: c._token(), clients)), ["fresh", "fresh"])
            self.assertEqual(http.call_count, 1)
        new_grant = self.new_client(refresh="new-seed", access=None)
        with patch.object(new_grant, "_http", return_value=response({
            "access_token": "grant-two", "refresh_token": "refresh-two", "expires_in": 3600,
        })) as http:
            self.assertEqual(new_grant._token(), "grant-two")
        self.assertEqual(http.call_args.kwargs["data"]["refresh_token"], "new-seed")

    def test_errors_do_not_expose_credentials_or_response(self):
        with patch.object(self.client.session, "request", side_effect=requests.ConnectionError("secret seed old-access")), \
             patch("frost_sync.netatmo_api.time.sleep"), self.assertLogs("frost_sync.netatmo_api", "WARNING"):
            with self.assertRaisesRegex(RuntimeError, "^Netatmo network failure: ConnectionError$"):
                self.client.fetch_stations()
        self.client.access_token = None
        with patch.object(self.client.session, "request", return_value=response({"error": "secret seed"}, 400)):
            with self.assertRaises(NetatmoAuthError) as raised:
                self.client.fetch_stations()
        self.assertNotIn("secret", str(raised.exception))
        self.assertNotIn("seed", str(raised.exception))

    def test_temporary_503_recovers_without_refreshing_tokens(self):
        unavailable = response(None, 503)
        unavailable.json.side_effect = ValueError("HTML gateway error")
        with patch.object(self.client.session, "request", side_effect=[
            unavailable, response({"error": {}}, 502), response({"body": {"devices": [DEVICE]}}),
        ]) as request, patch("frost_sync.netatmo_api.time.sleep") as sleep, \
             self.assertLogs("frost_sync.netatmo_api", "WARNING") as logs:
            monitors = self.client.fetch_stations()
        self.assertEqual(len(monitors), 1)
        self.assertEqual(request.call_count, 3)
        self.assertTrue(all(call.args[0] == "GET" for call in request.call_args_list))
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [2, 4])
        self.assertEqual(len(logs.records), 2)
        unavailable.json.assert_not_called()
        unavailable.close.assert_called_once()
        self.assertTrue(all("secret" not in record.getMessage() and "old-access" not in record.getMessage()
                            for record in logs.records))

    def test_persistent_503_stops_after_three_attempts_with_the_http_status(self):
        unavailable = response(None, 503)
        unavailable.json.side_effect = ValueError("HTML gateway error")
        with patch.object(self.client.session, "request", return_value=unavailable) as request, \
             patch("frost_sync.netatmo_api.time.sleep") as sleep, \
             self.assertLogs("frost_sync.netatmo_api", "WARNING"):
            with self.assertRaisesRegex(RuntimeError, r"^Netatmo API failure \(HTTP 503\)$"):
                self.client.fetch_stations()
        self.assertEqual(request.call_count, 3)
        self.assertEqual(sleep.call_count, 2)
        unavailable.json.assert_not_called()

    def test_read_timeout_retries_but_token_rotation_post_does_not(self):
        with patch.object(self.client.session, "request", side_effect=[
            requests.ReadTimeout("secret"), response({"body": {"devices": []}}),
        ]) as request, patch("frost_sync.netatmo_api.time.sleep") as sleep, \
             self.assertLogs("frost_sync.netatmo_api", "WARNING"):
            self.assertEqual(self.client.fetch_stations(), [])
        self.assertEqual(request.call_count, 2)
        sleep.assert_called_once_with(2)
        self.client.access_token = None
        with patch.object(self.client.session, "request", side_effect=requests.ReadTimeout("secret")) as request, \
             patch("frost_sync.netatmo_api.time.sleep") as sleep:
            with self.assertRaisesRegex(RuntimeError, "^Netatmo network failure: ReadTimeout$"):
                self.client.fetch_stations()
        self.assertEqual(request.call_count, 1)
        self.assertEqual(request.call_args.args[0], "POST")
        sleep.assert_not_called()

    def test_rate_limit_respects_retry_after_and_does_not_retry_other_client_errors(self):
        with patch.object(self.client.session, "request", side_effect=[
            response({}, 429, {"Retry-After": "10"}), response({"body": {"devices": []}}),
        ]), patch("frost_sync.netatmo_api.time.sleep") as sleep, \
             self.assertLogs("frost_sync.netatmo_api", "WARNING"):
            self.assertEqual(self.client.fetch_stations(), [])
        sleep.assert_called_once_with(10)
        now = datetime(2026, 11, 25, 17, 30, tzinfo=timezone.utc)
        retry_at = format_datetime(now + timedelta(seconds=20), usegmt=True)
        with patch.object(self.client.session, "request", side_effect=[
            response({}, 503, {"Retry-After": retry_at}), response({"body": {"devices": []}}),
        ]), patch("frost_sync.netatmo_api.time.sleep") as sleep, \
             patch("frost_sync.netatmo_api.datetime") as clock, \
             self.assertLogs("frost_sync.netatmo_api", "WARNING"):
            clock.now.return_value = now
            self.assertEqual(self.client.fetch_stations(), [])
        sleep.assert_called_once_with(20)
        for status, headers in [(429, {"Retry-After": "120"}), (400, {}), (404, {})]:
            with self.subTest(status=status), \
                 patch.object(self.client.session, "request", return_value=response({}, status, headers)) as request, \
                 patch("frost_sync.netatmo_api.time.sleep") as sleep:
                with self.assertRaisesRegex(RuntimeError, f"HTTP {status}"):
                    self.client.fetch_stations()
                self.assertEqual(request.call_count, 1)
                sleep.assert_not_called()

    def test_outdoor_only_per_module_time_units_and_missing_values(self):
        devices = [copy.deepcopy(DEVICE), {**copy.deepcopy(DEVICE), "_id": "70:ee:50:00:00:02"}]
        for module in devices[1]["modules"]:
            module.pop("dashboard_data", None)
        with patch.object(self.client, "_get", return_value={"body": {"devices": devices}}) as get:
            monitors = self.client.fetch_stations()
            rows = self.client.fetch_latest_observations(monitors)
        self.assertEqual(get.call_args.args[1], {"get_favorites": "true"})
        self.assertEqual([m.source_id for m in monitors], ["NETATMO-70ee50000001", "NETATMO-70ee50000002"])
        self.assertEqual(len(json.loads(monitors[0].provider_context)["modules"]), 3)
        self.assertEqual(len(rows), 2)
        values = {o["elementId"]: o["value"] for row in rows for o in row["observations"]}
        self.assertEqual(values, {"air_temperature": 0, "wind_speed": 10,
                                  "wind_from_direction": 0, "wind_speed_of_gust": 20})
        self.assertNotEqual(rows[0]["referenceTime"], rows[1]["referenceTime"])
        devices[0]["modules"][0]["dashboard_data"]["Temperature"] = float("nan")
        with patch.object(self.client, "_get", return_value={"body": {"devices": devices}}):
            rows = self.client.fetch_latest_observations(monitors)
        self.assertNotIn("air_temperature", [o["elementId"] for row in rows for o in row["observations"]])

    def test_history_chunks_deduplicates_and_converts_units(self):
        monitor = self.client._monitor(DEVICE)
        start = NOW - timedelta(days=8)
        stamp = int((start + timedelta(days=7)).timestamp())
        with patch.object(self.client, "_get", return_value={"body": {str(stamp): [36, 0, 72],
                str(int((NOW + timedelta(days=1)).timestamp())): [99, 99, 99]}}) as get:
            rows = self.client.fetch_observations_range(monitor.provider_context, start, NOW,
                                                       ["wind_speed", "wind_from_direction", "wind_speed_of_gust"])
        self.assertEqual(get.call_count, 2)
        self.assertEqual(len(rows), 1)
        self.assertEqual([o["value"] for o in rows[0]["observations"]], [10, 0, 20])
        self.assertEqual(get.call_args.args[1]["optimize"], "false")
        self.assertEqual(get.call_args.args[1]["type"], "windstrength,windangle,guststrength")
        with patch.object(self.client, "_get") as get:
            self.assertEqual(self.client.fetch_observations_range(monitor.provider_context, start, NOW, ["snow_depth"]), [])
            get.assert_not_called()

    def test_rain_uses_closed_fixed_hour_buckets_and_keeps_zero(self):
        monitor = self.client._monitor(DEVICE)
        with patch.object(self.client, "_get", return_value={"body": {
            str(int((NOW - timedelta(hours=1)).timestamp())): [0],
            str(int(NOW.timestamp())): [3],
        }}) as get:
            rows = self.client.fetch_observations_range(monitor.provider_context, NOW, NOW, ["precipitation_1h"])
        self.assertEqual(get.call_args.args[1]["type"], "sum_rain")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["referenceTime"], NOW.isoformat())
        self.assertEqual(rows[0]["observations"][0]["value"], 0)


class NetatmoIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        self.addCleanup(self.engine.dispose)
        Base.metadata.create_all(self.engine)
        self.factory = sessionmaker(self.engine, expire_on_commit=False)
        self.monitor = NetatmoClient._monitor(DEVICE)
        self.frost = Mock()
        self.frost.fetch_sources.return_value = []
        self.frost.fetch_capability_source_ids.return_value = set()
        self.frost.fetch_latest_observations.return_value = []
        self.frost.fetch_snow_series_ids.return_value = []
        self.netatmo = Mock()
        self.netatmo.fetch_stations.return_value = [self.monitor]
        self.rows = [{"sourceId": self.monitor.source_id, "referenceTime": NOW.isoformat(),
                      "observations": [{"elementId": "air_temperature", "value": -5, "unit": "degC"},
                                       {"elementId": "wind_speed", "value": 10, "unit": "m/s"}]}]
        self.netatmo.fetch_latest_observations.return_value = self.rows
        self.netatmo.fetch_observations_range.return_value = []

    def sync(self):
        with self.factory() as session:
            service = SyncService(session, self.frost, netatmo_client=self.netatmo)
            metadata = service.sync_metadata(100)
            hourly = service.run_hourly_sync(100)
        return metadata, hourly

    def test_metadata_hourly_latest_exports_and_http_timeseries(self):
        metadata, hourly = self.sync()
        self.assertEqual(metadata.station_errors, 0)
        self.assertEqual(hourly.station_errors, 0)
        self.assertEqual(hourly.observations_written, 2)
        for builder in [web.build_latest_geojson, web.build_latest_compact_geojson,
                        web.build_latest_fme_geojson, web.build_latest_7d_geojson]:
            features = builder(self.factory)["features"]
            self.assertEqual(len(features), 1)
            self.assertEqual(features[0]["properties"]["source_id"], self.monitor.source_id)
        with self.factory() as session:
            latest = session.scalars(select(StationLatest)).one()
            self.assertEqual(latest.air_temperature, -5)
            self.assertEqual(len(session.scalars(select(Observation)).all()), 2)
        self.netatmo.fetch_observations_range.return_value = self.rows
        settings = Settings(None, None, None, None, proxy_api_key="test")
        with patch.object(web, "load_settings", return_value=settings), \
             patch.object(web, "create_session_factory", return_value=self.factory), \
             patch.object(web, "create_netatmo_client", return_value=self.netatmo):
            client = web.create_app().test_client()
        args = f"parameters=air_temperature,wind_speed&from={web._isoformat(NOW)}&to={web._isoformat(NOW + timedelta(hours=1))}"
        headers = {"X-API-Key": "test"}
        single = client.get(f"/api/stations/{self.monitor.source_id}/timeseries?{args}", headers=headers)
        self.assertEqual(single.status_code, 200)
        self.assertEqual(single.json["series"]["air_temperature"]["data"][0]["value"], -5)
        multi = client.get(f"/api/timeseries?stations={self.monitor.source_id}&{args}", headers=headers)
        self.assertEqual(multi.status_code, 200)
        self.assertEqual(multi.json["errors"], [])
        self.assertEqual(len(multi.json["stations"]), 1)

    def test_netatmo_outage_does_not_rollback_another_provider(self):
        self.sync()
        snower = Mock()
        snower.fetch_latest_observations.return_value = [{**self.rows[0], "sourceId": "SNWR-test"}]
        self.netatmo.fetch_latest_observations.side_effect = RuntimeError("Netatmo unavailable")
        with self.factory() as session:
            session.add(Station(source_id="SNWR-test", provider="snower", snower_active=True,
                                provider_context="{}", last_seen_at=NOW))
            session.commit()
            with self.assertLogs("frost_sync.service", "ERROR"):
                summary = SyncService(session, self.frost, snower_client=snower,
                                      netatmo_client=self.netatmo).run_hourly_sync(100)
        self.assertEqual(summary.station_errors, 1)
        with self.factory() as session:
            self.assertEqual(session.scalars(select(StationLatest).where(StationLatest.source_id == "SNWR-test")).one().air_temperature, -5)

    def test_removed_access_hides_station_and_invalidates_cache(self):
        self.sync()
        settings = Settings(None, None, None, None)
        before = web._geojson_cache_dir(settings, web._active_snower_ids(self.factory))
        self.netatmo.fetch_stations.return_value = []
        with self.factory() as session:
            SyncService(session, self.frost, netatmo_client=self.netatmo).sync_metadata(100)
        after = web._geojson_cache_dir(settings, web._active_snower_ids(self.factory))
        self.assertNotEqual(before, after)
        self.assertEqual(web.build_latest_geojson(self.factory)["features"], [])
        with self.factory() as session:
            self.assertEqual(len(session.scalars(select(Observation)).all()), 2)

        # Restored sharing makes the stored station visible again.
        self.netatmo.fetch_stations.return_value = [self.monitor]
        with self.factory() as session:
            SyncService(session, self.frost, netatmo_client=self.netatmo).sync_metadata(100)
        self.assertEqual(len(web.build_latest_geojson(self.factory)["features"]), 1)

        # A transient discovery failure must not be interpreted as revoked access.
        self.netatmo.fetch_stations.side_effect = NetatmoAuthError("Temporary failure")
        with self.factory() as session, self.assertLogs("frost_sync.service", "ERROR"):
            result = SyncService(session, self.frost, netatmo_client=self.netatmo).sync_metadata(100)
        self.assertEqual(result.station_errors, 1)
        self.assertEqual(len(web.build_latest_geojson(self.factory)["features"]), 1)

    def test_unconfigured_netatmo_is_reported_in_both_timeseries_routes(self):
        self.sync()
        settings = Settings(None, None, None, None, proxy_api_key="test")
        with patch.object(web, "load_settings", return_value=settings), \
             patch.object(web, "create_session_factory", return_value=self.factory):
            client = web.create_app().test_client()
        args = f"parameters=air_temperature&from={web._isoformat(NOW)}&to={web._isoformat(NOW + timedelta(hours=1))}"
        headers = {"X-API-Key": "test"}
        self.assertEqual(client.get(f"/api/stations/{self.monitor.source_id}/timeseries?{args}", headers=headers).status_code, 502)
        multi = client.get(f"/api/timeseries?stations={self.monitor.source_id}&{args}", headers=headers)
        self.assertEqual(multi.status_code, 200)
        self.assertEqual(multi.json["stations"], [])
        self.assertEqual(multi.json["errors"][0]["source_id"], self.monitor.source_id)


if __name__ == "__main__":
    unittest.main()
