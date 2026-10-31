from pathlib import Path
import runpy
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch

from flask import Flask

from frost_sync.auth import PUBLIC_ENDPOINTS, install_api_key_auth
from frost_sync.config import Settings, load_settings
from frost_sync import web


KEY = "test-proxy-secret-not-a-provider-key"


class ProxyAuthTest(unittest.TestCase):
    def settings(self, **kwargs):
        return Settings(None, None, None, None, proxy_api_key=KEY, **kwargs)

    def make_app(self, settings=None):
        with patch.object(web, "load_settings", return_value=settings or self.settings()):
            return web.create_app()

    def test_all_blueprint_data_routes_reject_missing_and_wrong_keys(self):
        app = self.make_app()
        client = app.test_client()
        for rule in app.url_map.iter_rules():
            if rule.endpoint == "static" or rule.endpoint.rsplit(".", 1)[-1] in PUBLIC_ENDPOINTS:
                continue
            path = rule.rule.replace("<source_id>", "12.215.0")
            for headers in [{}, {"X-API-Key": "wrong"}, {"X-API-Key": "æ"}]:
                with self.subTest(path=path, headers=bool(headers)):
                    response = client.get(path, headers=headers)
                    self.assertEqual(response.status_code, 401)
                    self.assertEqual(response.json, {"error": "Unauthorized"})
                    self.assertEqual(response.headers["Cache-Control"], "private, no-store")
                    self.assertNotIn(KEY, response.get_data(as_text=True))

    def test_correct_header_only_and_public_health(self):
        client = self.make_app().test_client()
        self.assertEqual(client.get("/api/parameters", headers={"X-API-Key": KEY}).status_code, 200)
        self.assertEqual(client.get("/api/parameters", headers={"x-api-key": KEY}).status_code, 200)
        self.assertEqual(client.get("/api/parameters?api_key=" + KEY).status_code, 401)
        self.assertEqual(client.get("/api/parameters", headers={"Authorization": "Bearer " + KEY}).status_code, 401)
        self.assertEqual(client.head("/api/parameters").status_code, 401)
        self.assertEqual(client.get("/health").json, {"status": "ok"})
        self.assertEqual(client.options("/api/parameters").get_data(), b"")

    def test_missing_configuration_fails_closed(self):
        with self.assertRaisesRegex(RuntimeError, "PROXY_API_KEY"):
            self.make_app(Settings(None, None, None, None))

    def test_env_configuration(self):
        with patch("frost_sync.config._load_dotenv"), patch.dict(
            "os.environ", {"PROXY_API_KEY": "  " + KEY + "  "}, clear=True
        ):
            self.assertEqual(load_settings().proxy_api_key, KEY)

    def test_prefixed_blueprint_cannot_bypass_auth(self):
        app = Flask(__name__)
        with patch.object(web, "load_settings", return_value=self.settings()):
            app.register_blueprint(web.create_blueprint("embedded"), url_prefix="/custom")
        client = app.test_client()
        self.assertEqual(client.get("/custom/api/parameters").status_code, 401)
        self.assertEqual(client.get("/custom/api/parameters", headers={"X-API-Key": KEY}).status_code, 200)

    def test_latest_gzip_file_and_conditional_requests_are_public(self):
        with TemporaryDirectory(dir=Path(__file__).resolve().parent) as folder:
            path = Path(folder) / "latest.geojson"
            path.write_text('{"type":"FeatureCollection","features":[]}', encoding="utf-8")
            path.with_suffix(".geojson.gz").write_bytes(b"test-compressed-payload")
            client = self.make_app(self.settings(geojson_cache_dir=folder)).test_client()
            headers = {"Accept-Encoding": "gzip"}
            response = client.get("/api/stations/latest.geojson", headers=headers)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.headers["Content-Encoding"], "gzip")
            self.assertNotIn("no-store", response.headers["Cache-Control"])
            self.assertNotIn("X-API-Key", response.headers.get("Vary", ""))
            etag = response.headers["ETag"]
            response.close()
            cached = client.get("/api/stations/latest.geojson", headers={
                "Accept-Encoding": "gzip", "If-None-Match": etag,
            })
            self.assertEqual(cached.status_code, 304)
            cached.close()

    def test_latest_variants_filters_and_fallback_are_public_with_any_prefix(self):
        payload = {"type": "FeatureCollection", "features": []}
        builders = ["build_latest_geojson", "build_latest_compact_geojson",
                    "build_latest_fme_geojson", "build_latest_7d_geojson"]
        from contextlib import ExitStack
        with ExitStack() as stack:
            for name in builders:
                stack.enter_context(patch.object(web, name, return_value=payload))
            stack.enter_context(patch.object(web, "_cached_geojson_response", return_value=None))
            standalone = self.make_app()
            embedded = Flask(__name__)
            # app.py has both app-wide and blueprint-wide guards.
            install_api_key_auth(embedded, KEY)
            with patch.object(web, "load_settings", return_value=self.settings()):
                embedded.register_blueprint(web.create_blueprint("embedded"), url_prefix="/weather")
            for app, prefix in [(standalone, ""), (embedded, "/weather")]:
                client = app.test_client()
                for suffix in ["latest.geojson", "latest.compact.geojson", "latest.fme.geojson",
                               "latest.7d.geojson", "latest.geojson?has=snow_depth"]:
                    for headers in [{}, {"X-API-Key": "wrong"}]:
                        response = client.get(prefix + "/api/stations/" + suffix, headers=headers)
                        self.assertEqual(response.status_code, 200)
                        self.assertEqual(response.json, payload)
                self.assertEqual(client.head(prefix + "/api/stations/latest.geojson").status_code, 200)
                # A suffix/query string must not bypass the route allowlist.
                self.assertEqual(client.get(prefix + "/api/stations/SN123/timeseries?format=latest.geojson").status_code, 401)
                self.assertEqual(client.get(prefix + "/api/stations/history.geojson").status_code, 401)
                self.assertEqual(client.get(prefix + "/api/stations/daily.geojson").status_code, 401)

    def test_old_key_is_rejected_after_rotation(self):
        app = Flask(__name__)
        install_api_key_auth(app, "rotated-key")
        app.add_url_rule("/data", "data", lambda: {"data": 1})
        client = app.test_client()
        self.assertEqual(client.get("/data", headers={"X-API-Key": KEY}).status_code, 401)
        self.assertEqual(client.get("/data", headers={"X-API-Key": "rotated-key"}).status_code, 200)

    def test_pythonanywhere_legacy_routes_cache_and_cors(self):
        settings = Settings("test-frost-id", None, None, None, proxy_api_key=KEY)
        upstream = Mock()
        upstream.get.return_value.status_code = 200
        upstream.get.return_value.json.return_value = {"data": []}
        with patch("frost_sync.config.load_settings", return_value=settings), \
             patch.object(web, "load_settings", return_value=settings), \
             patch("requests.Session", return_value=upstream):
            # app.py's global app is the entry point imported by the WSGI file.
            app = runpy.run_path(str(Path(__file__).resolve().parents[1] / "app.py"))["app"]
        client = app.test_client()
        for path in ["/frost", "/frost_available", "/sources", "/weather/api/parameters"]:
            self.assertEqual(client.get(path).status_code, 401)
        url = "/frost?sources=SN123&elements=air_temperature&referencetime=2026-10-28"
        self.assertEqual(client.get(url, headers={"X-API-Key": KEY}).status_code, 200)
        upstream.get.assert_called_once()
        self.assertEqual(client.get(url).status_code, 401)
        self.assertEqual(client.get(url, headers={"X-API-Key": "wrong"}).status_code, 401)
        upstream.get.assert_called_once()
        preflight = client.options("/weather/api/parameters", headers={
            "Origin": "https://respons.example", "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "X-API-Key",
        })
        self.assertEqual(preflight.status_code, 200)
        self.assertIn("X-API-Key", preflight.headers["Access-Control-Allow-Headers"])
        self.assertEqual(client.get("/health").status_code, 200)
        self.assertEqual(client.get("/weather/health").status_code, 200)


if __name__ == "__main__":
    unittest.main()
