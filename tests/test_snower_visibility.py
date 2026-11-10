import gzip
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from frost_sync import web
from frost_sync.config import Settings
from frost_sync.db import Base
from frost_sync.models import Observation, Station, StationLatest
from frost_sync.service import SyncService
from frost_sync.snower_api import SnowerClient


class SnowerVisibilityTest(unittest.TestCase):
    def test_metadata_only_accepts_boolean_true(self):
        client = SnowerClient("https://example.test", "snk_test")
        statuses = {"yes": True, "no": False, "string": "false", "unknown": None}

        def post(path, body):
            if path == "/monitors_list":
                return [{"permanent_id": key} for key in statuses]
            if path == "/monitor_active_status":
                return {"active_status": statuses[body["monitor"]]}
            self.assertEqual(body["monitor"], "yes")
            return {"location": {"lat": 60, "lng": 10}}

        with patch.object(client, "_post", side_effect=post):
            self.assertEqual([monitor.source_id for monitor in client.fetch_stations()], ["SNWR-yes"])

    def test_deactivation_hides_station_and_bypasses_previous_cache(self):
        engine = create_engine("sqlite:///:memory:")
        self.addCleanup(engine.dispose)
        Base.metadata.create_all(engine)
        factory = sessionmaker(engine)
        now = datetime.now(timezone.utc)
        observation_time = now - timedelta(days=1)
        with factory.begin() as session:
            for source_id, provider, active in [
                ("SNWR-active", "snower", True),
                ("SNWR-unknown", "snower", None),
                ("SN123", "frost", None),
            ]:
                station = Station(source_id=source_id, provider=provider, snower_active=active,
                                  latitude=60, longitude=10, last_seen_at=now)
                session.add(station)
                session.flush()
                session.add(StationLatest(station_id=station.id, source_id=source_id, updated_at=now,
                                          observed_at=observation_time, snow_depth=10))
                session.add(Observation(station_id=station.id, reference_time=observation_time,
                                        element_id="snow_depth", value=10, fetched_at=now))

        with TemporaryDirectory(dir=Path(__file__).resolve().parent) as folder:
            settings = Settings(None, None, None, None, geojson_cache_dir=folder, proxy_api_key="test")
            with patch.object(web, "load_settings", return_value=settings), \
                 patch.object(web, "create_session_factory", return_value=factory):
                web.refresh_geojson_cache(settings)
                client = web.create_app().test_client()
            gzip_headers = {"Accept-Encoding": "gzip"}
            path = "/api/stations/latest.geojson"
            with client.get(path, headers=gzip_headers) as cached:
                self.assertEqual(cached.headers["X-GeoJSON-Cache"], "file-gzip")
                features = json.loads(gzip.decompress(cached.data))["features"]
                self.assertEqual({f["properties"]["source_id"] for f in features}, {"SN123", "SNWR-active"})

            with factory.begin() as session:
                SyncService(session, Mock())._upsert_snower_stations([])

            for suffix in ["latest.geojson", "latest.compact.geojson", "latest.fme.geojson", "latest.7d.geojson"]:
                with self.subTest(suffix=suffix):
                    response = client.get("/api/stations/" + suffix, headers=gzip_headers)
                    self.assertEqual(response.headers["X-GeoJSON-Cache"], "database-fallback")
                    self.assertEqual({f["properties"]["source_id"] for f in response.json["features"]}, {"SN123"})

            headers = {"X-API-Key": "test"}
            date = observation_time.date().isoformat()
            history = client.get(f"/api/stations/history.geojson?date={date}", headers=headers)
            self.assertEqual({f["properties"]["source_id"] for f in history.json["features"]}, {"SN123"})
            for suffix in ["", f"/timeseries?date={date}", f"/observations?date={date}"]:
                self.assertEqual(client.get("/api/stations/SNWR-active" + suffix, headers=headers).status_code, 404)
            with factory() as session:
                self.assertEqual(len(list(session.scalars(select(Observation)))), 3)
