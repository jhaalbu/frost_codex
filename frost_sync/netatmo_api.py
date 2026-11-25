"""Account weather stations (including shared/favourite stations), read_station.

API reference: https://dev.netatmo.com/apidocumentation/weather
OAuth reference: https://dev.netatmo.com/apidocumentation/oauth
Indoor modules are never treated as outdoor weather measurements.
"""
from __future__ import annotations

import hashlib
from contextlib import closing
import json
import logging
import math
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
import sqlite3
import time
from typing import Any, Iterable

import requests

from frost_sync.config import Settings

logger = logging.getLogger(__name__)

SOURCE_ID_PREFIX = "NETATMO-"
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_GET_ATTEMPTS = 3
MAX_RETRY_DELAY_SECONDS = 60
# Netatmo wind speeds are always km/h, independent of the user's display units.
MODULE_FIELDS = {
    "NAModule1": {"air_temperature": ("temperature", "Temperature", "degC", 1)},
    "NAModule2": {
        "wind_speed": ("windstrength", "WindStrength", "m/s", 1 / 3.6),
        "wind_from_direction": ("windangle", "WindAngle", "deg", 1),
        "wind_speed_of_gust": ("guststrength", "GustStrength", "m/s", 1 / 3.6),
    },
    "NAModule3": {"precipitation_1h": ("sum_rain", None, "mm", 1)},
}
TARGET_ELEMENTS = {element for fields in MODULE_FIELDS.values() for element in fields}


class NetatmoAuthError(RuntimeError):
    pass


@dataclass(frozen=True)
class NetatmoMonitor:
    source_id: str
    name: str
    longitude: float | None
    latitude: float | None
    provider_context: str
    masl: float | None = None
    country: str | None = None


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _timestamp(value: Any) -> datetime | None:
    number = _number(value)
    if number is None:
        return None
    try:
        return datetime.fromtimestamp(number, timezone.utc)
    except (ValueError, OverflowError, OSError):
        return None


def _row(source: str, stamp: datetime, observations: list[dict]) -> dict:
    return {"sourceId": source, "referenceTime": stamp.isoformat(), "observations": observations}


def _source_id(device_id: str) -> str:
    # Colons denote Frost sub-sources in the shared normalizer.
    return SOURCE_ID_PREFIX + device_id.replace(":", "")


def create_netatmo_client(settings: Settings) -> NetatmoClient | None:
    values = (settings.netatmo_client_id, settings.netatmo_client_secret,
              settings.netatmo_refresh_token, settings.netatmo_access_token)
    if not any(values):
        return None
    if not all(values[:3]):
        raise NetatmoAuthError("Netatmo krev NETATMO_CLIENT_ID, NETATMO_CLIENT_SECRET og NETATMO_REFRESH_TOKEN")
    return NetatmoClient(*values[:3], access_token=values[3],
                         token_file=settings.netatmo_token_file,
                         timeout_seconds=settings.request_timeout_seconds)


class NetatmoClient:
    def __init__(self, client_id: str, client_secret: str, refresh_token: str,
                 access_token: str | None = None, token_file: str = ".netatmo_tokens.db",
                 timeout_seconds: int = 60) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.access_token = access_token
        self.token_file = Path(token_file).resolve()
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json", "User-Agent": "frost-sync/1.0"})
        # Changing the configured refresh token explicitly starts a new grant.
        self._seed = hashlib.sha256(f"{client_id}\0{refresh_token}".encode()).hexdigest()

    def _http(self, method: str, path: str, **kwargs) -> requests.Response:
        # Only reads can be repeated safely. A token POST may already have
        # rotated the refresh token even if its response never reached us.
        attempts = MAX_GET_ATTEMPTS if method.upper() == "GET" else 1
        for attempt in range(attempts):
            try:
                response = self.session.request(method, f"https://api.netatmo.com{path}",
                                                timeout=self.timeout_seconds, allow_redirects=False, **kwargs)
            except requests.RequestException as exc:
                if isinstance(exc, (requests.ConnectionError, requests.Timeout)) and attempt + 1 < attempts:
                    delay = 2 ** (attempt + 1)
                    logger.warning("Netatmo %s: %s; retry %s/%s in %ss",
                                   path, type(exc).__name__, attempt + 1, attempts - 1, delay)
                    time.sleep(delay)
                    continue
                # Do not include headers, credentials or response bodies in logs/errors.
                raise RuntimeError(f"Netatmo network failure: {type(exc).__name__}") from None
            if response.status_code not in RETRYABLE_STATUS_CODES or attempt + 1 == attempts:
                return response
            delay = self._retry_delay(response, attempt)
            if delay is None:
                return response
            logger.warning("Netatmo %s: HTTP %s; retry %s/%s in %.1fs",
                           path, response.status_code, attempt + 1, attempts - 1, delay)
            response.close()
            time.sleep(delay)
        raise RuntimeError("Netatmo request attempts exhausted")

    @staticmethod
    def _retry_delay(response: requests.Response, attempt: int) -> float | None:
        delay = float(2 ** (attempt + 1))
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                seconds = float(retry_after)
            except (TypeError, ValueError):
                try:
                    seconds = (_utc(parsedate_to_datetime(retry_after)) - datetime.now(timezone.utc)).total_seconds()
                except (TypeError, ValueError, OverflowError):
                    seconds = 0
            if math.isfinite(seconds):
                delay = max(delay, seconds)
        # Do not retry early if the server requests a wait longer than this job
        # allows. The next sync can try again without tying up a web worker.
        return delay if delay <= MAX_RETRY_DELAY_SECONDS else None

    @staticmethod
    def _json(response: requests.Response) -> dict:
        try:
            payload = response.json()
        except ValueError:
            raise RuntimeError(f"Netatmo returned invalid JSON (HTTP {response.status_code})") from None
        if not isinstance(payload, dict):
            raise RuntimeError("Netatmo returned an unexpected response")
        return payload

    def _token(self, rejected_token: str | None = None) -> str:
        """Serialize token rotation across CLI jobs, web workers and threads.

        This separate SQLite file stores secrets, never weather data. Persist the
        returned refresh token before using the new access token. The env file is
        only a seed; writing it on every refresh would race with other workers.
        """
        self.token_file.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.token_file, os.O_CREAT | os.O_WRONLY, 0o600)
        os.close(fd)
        with closing(sqlite3.connect(self.token_file, timeout=self.timeout_seconds + 10)) as db, db:
            db.execute("CREATE TABLE IF NOT EXISTS oauth_token (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)")
            db.execute("BEGIN IMMEDIATE")
            saved = db.execute("SELECT payload FROM oauth_token WHERE id = 1").fetchone()
            state = json.loads(saved[0]) if saved else {}
            if state.get("seed") != self._seed:
                state = {"seed": self._seed, "access_token": self.access_token,
                         "refresh_token": self.refresh_token, "expires_at": None}
            token = state.get("access_token")
            expiry = state.get("expires_at")
            if (token and (expiry is None or expiry > time.time() + 60)
                    and token != rejected_token):
                return token
            response = self._http("POST", "/oauth2/token", data={
                "grant_type": "refresh_token", "client_id": self.client_id,
                "client_secret": self.client_secret, "refresh_token": state["refresh_token"],
            })
            payload = self._json(response)
            if not response.ok or payload.get("error"):
                raise NetatmoAuthError(f"Netatmo token refresh failed (HTTP {response.status_code}); kontroller OAuth-tilgang/read_station")
            if not payload.get("access_token") or not payload.get("refresh_token"):
                raise NetatmoAuthError("Netatmo token response is incomplete")
            state.update(access_token=payload["access_token"], refresh_token=payload["refresh_token"],
                         expires_at=time.time() + float(payload.get("expires_in", 10800)))
            db.execute("INSERT OR REPLACE INTO oauth_token VALUES (1, ?)", (json.dumps(state),))
            db.commit()
            return state["access_token"]

    def _get(self, endpoint: str, params: dict) -> dict:
        token = self._token()
        for attempt in range(2):
            response = self._http("GET", f"/api/{endpoint}", params=params,
                                  headers={"Authorization": f"Bearer {token}"})
            # Gateways may return HTML on 503. Keep the actual HTTP failure in
            # the error message instead of reporting it as a JSON parsing error.
            if not response.ok and response.status_code not in (401, 403):
                raise RuntimeError(f"Netatmo API failure (HTTP {response.status_code})")
            payload = self._json(response)
            error = payload.get("error") or {}
            code = error.get("code") if isinstance(error, dict) else None
            if attempt == 0 and (response.status_code == 401 or code in (1, 2, 3)):
                token = self._token(rejected_token=token)
                continue
            if response.status_code in (401, 403) or code in (1, 2, 3, 13):
                raise NetatmoAuthError(f"Netatmo access denied (HTTP {response.status_code}); kontroller read_station")
            if not response.ok or error:
                raise RuntimeError(f"Netatmo API failure (HTTP {response.status_code})")
            return payload
        raise NetatmoAuthError("Netatmo access denied")

    def _devices(self) -> list[dict]:
        payload = self._get("getstationsdata", {"get_favorites": "true"})
        devices = payload.get("body", {}).get("devices")
        if not isinstance(devices, list):
            raise RuntimeError("Netatmo response is missing devices")
        return devices

    @staticmethod
    def _monitor(device: dict) -> NetatmoMonitor | None:
        device_id = device.get("_id")
        modules = [{"module_id": m["_id"], "type": m["type"]}
                   for m in device.get("modules", [])
                   if m.get("_id") and m.get("type") in MODULE_FIELDS]
        if not device_id or not modules:
            return None
        place = device.get("place") or {}
        coordinates = place.get("location") or []
        lon = _number(coordinates[0]) if len(coordinates) >= 2 else None
        lat = _number(coordinates[1]) if len(coordinates) >= 2 else None
        if lon is not None and not -180 <= lon <= 180:
            lon = None
        if lat is not None and not -90 <= lat <= 90:
            lat = None
        return NetatmoMonitor(
            source_id=_source_id(str(device_id)),
            name=device.get("station_name") or device.get("module_name") or str(device_id),
            longitude=lon, latitude=lat, masl=_number(place.get("altitude")),
            country=place.get("country"),
            provider_context=json.dumps({"device_id": device_id, "modules": modules}),
        )

    def fetch_stations(self) -> list[NetatmoMonitor]:
        return [monitor for d in self._devices() if (monitor := self._monitor(d)) is not None]

    @staticmethod
    def _context(context: str | None) -> dict:
        try:
            result = json.loads(context or "{}")
        except (TypeError, ValueError):
            raise ValueError("Invalid Netatmo provider_context") from None
        if not isinstance(result, dict) or not result.get("device_id") or not isinstance(result.get("modules"), list):
            raise ValueError("Netatmo provider_context requires device_id and modules")
        return result

    @classmethod
    def available_elements(cls, context: str) -> set[str]:
        return {element for m in cls._context(context)["modules"]
                for element in MODULE_FIELDS.get(m.get("type"), {})}

    def fetch_latest_observations(self, monitors: Iterable[NetatmoMonitor]) -> list[dict]:
        allowed = {m.source_id for m in monitors}
        rows = []
        for device in self._devices():
            source = _source_id(str(device.get("_id")))
            if source not in allowed:
                continue
            for module in device.get("modules", []):
                fields = MODULE_FIELDS.get(module.get("type"), {})
                data = module.get("dashboard_data") or {}
                stamp = _timestamp(data.get("time_utc"))
                if stamp is None:
                    continue
                observations = []
                for element, (_, key, unit, factor) in fields.items():
                    value = _number(data.get(key)) if key else None
                    if value is not None:
                        observations.append({"elementId": element, "value": value * factor,
                                             "unit": unit, "qualityCode": None})
                if observations:
                    rows.append(_row(source, stamp, observations))
        return rows

    def fetch_observations_range(self, provider_context: str | None, from_dt: datetime,
                                 to_dt: datetime, logical_parameter_ids: Iterable[str]) -> list[dict]:
        context = self._context(provider_context)
        start, end = _utc(from_dt), _utc(to_dt)
        if end < start:
            raise ValueError("to_dt must be after from_dt")
        wanted = set(logical_parameter_ids)
        grouped: dict[datetime, dict] = {}
        for module in context["modules"]:
            fields = {k: v for k, v in MODULE_FIELDS.get(module.get("type"), {}).items() if k in wanted}
            if not fields:
                continue
            # Stable UTC hour boundaries: no moving/overlapping rain intervals.
            # 7-day chunks are far below the upstream limit of 1024 points.
            cursor = start.replace(minute=0, second=0, microsecond=0)
            if "precipitation_1h" in fields:
                cursor -= timedelta(hours=1)
            while cursor <= end:
                chunk_end = min(cursor + timedelta(days=7), end)
                params = {"device_id": context["device_id"], "module_id": module["module_id"],
                          "scale": "1hour", "type": ",".join(v[0] for v in fields.values()),
                          "date_begin": int(cursor.timestamp()), "date_end": int(chunk_end.timestamp()),
                          "limit": 1024, "optimize": "false", "real_time": "true"}
                body = self._get("getmeasure", params).get("body")
                if body == []:
                    body = {}
                if not isinstance(body, dict):
                    raise RuntimeError("Netatmo history response is not a timestamp map")
                for raw_stamp, values in body.items():
                    stamp = _timestamp(raw_stamp)
                    if stamp is None or not isinstance(values, list):
                        continue
                    # sum_rain describes the interval starting at this timestamp.
                    # Store at its end, matching the shared precipitation contract.
                    if "precipitation_1h" in fields:
                        stamp += timedelta(hours=1)
                    if not start <= stamp <= end:
                        continue
                    row = grouped.setdefault(stamp, _row(_source_id(context["device_id"]), stamp, []))
                    existing = {o["elementId"] for o in row["observations"]}
                    for (element, (_, _, unit, factor)), raw_value in zip(fields.items(), values):
                        value = _number(raw_value)
                        if value is not None and element not in existing:
                            row["observations"].append({"elementId": element, "value": value * factor,
                                                        "unit": unit, "qualityCode": None})
                if chunk_end >= end:
                    break
                cursor = chunk_end
        return [grouped[stamp] for stamp in sorted(grouped) if grouped[stamp]["observations"]]
