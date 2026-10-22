"""Wyssen WAC.3 time-series API client for frost_sync.

Requires requests. The API key is read by the app config and passed here;
this module does not load environment variables. Credentials are sent as the
`apikey` query parameter, as required by the documented Wyssen API.

Station coordinates are not supplied in the current Wyssen API documentation,
so they are deliberately left as None until known coordinates are supplied.
"""
from __future__ import annotations

import json
import logging
import math
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

import requests

logger = logging.getLogger(__name__)
WYSSEN_SOURCE_ID_PREFIX = "WYSSEN-"

# Confirmed from the Wyssen email. No coordinates were supplied.
STATIONS: tuple[tuple[int, str], ...] = (
    (9902, "Peparstein 1"),
    (11894, "Svandalsflona 1"),
    (11886, "Austmannalia 3"),
    (12707, "Skredene 1"),
)

WYSSEN_STATIONS = {
    9902: {
        "name": "Peparstein 1",
        "latitude": 59.8445,
        "longitude": 7.0897,
        "masl": 1309.34,
    },
    11894: {
        "name": "Svandalsflona 1",
        "latitude": 59.8526,
        "longitude": 6.9480,
        "masl": 1200.38,
    },
    11886: {
        "name": "Austmannalia 3",
        "latitude": 59.8591,
        "longitude": 6.8789,
        "masl":  886.1,
    },
    12707: {
        "name": "Skredene 1",
        "latitude": 61.0334,
        "longitude": 6.5440,
        "masl": 862.18,
    },
}

# Map directly comparable observations to existing frost_sync logical IDs.
# MPQ is NOT assumed to be an hourly sum, and MPI is NOT assumed to be
# equivalent to precipitation_1h. Wind gust period is unspecified.
MEASUREMENTS: dict[str, tuple[str, str]] = {
    "MTA": ("air_temperature", "degC"),
    "MWS": ("wind_speed", "m/s"),
    "MWD": ("wind_from_direction", "deg"),
    "MWG": ("wyssen_wind_gust", "m/s"),
    "MAP": ("wyssen_air_pressure", "unknown"),
    "MHA": ("wyssen_humidity", "unknown"),
    "MPI": ("wyssen_precipitation_intensity", "unknown"),
    "MPQ": ("wyssen_precipitation_quantity", "unknown"),
    "MSR": ("wyssen_solar_radiation", "unknown"),
}
DEFAULT_CODES = ("MTA", "MWS", "MWD")


class WyssenAuthError(RuntimeError):
    """API key was absent or rejected by Wyssen."""


@dataclass(frozen=True)
class WyssenMonitor:
    source_id: str
    name: str
    longitude: float | None
    latitude: float | None
    provider_context: str
    masl: float | None = None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return _utc(value).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (ValueError, TypeError):
        return None
    return number if math.isfinite(number) else None


def _context(value: str | None) -> dict[str, Any]:
    if not value:
        raise ValueError("Wyssen provider_context manglar")
    result = json.loads(value)
    if not isinstance(result, dict) or result.get("station_id") is None:
        raise ValueError("Ugyldig Wyssen provider_context: station_id manglar")
    return result


def _source_id(station_id: int) -> str:
    return f"{WYSSEN_SOURCE_ID_PREFIX}{station_id}"


def _is_tuple(value: Any) -> bool:
    return isinstance(value, list) and len(value) >= 2 and not isinstance(value[0], list)


class WyssenClient:
    def __init__(
        self,
        base_url: str = "https://control.wyssenavalanche.com/app/api/time-series/",
        api_key: str | None = None,
        timeout_seconds: int = 60,
        history_chunk_days: int = 3,
        measurement_codes: Iterable[str] = DEFAULT_CODES,
    ) -> None:
        if not api_key or not api_key.strip():
            raise WyssenAuthError("WYSSEN_API_KEY manglar")
        if history_chunk_days < 1:
            raise ValueError("history_chunk_days må vere minst 1")
        self.base_url = base_url
        self.api_key = api_key.strip()
        self.timeout_seconds = timeout_seconds
        self.history_chunk_days = history_chunk_days
        self.measurement_codes = tuple(dict.fromkeys(code.upper() for code in measurement_codes))
        invalid = set(self.measurement_codes) - set(MEASUREMENTS)
        if invalid:
            raise ValueError(f"Ukjende Wyssen målekodar: {sorted(invalid)}")
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json", "User-Agent": "frost-sync/1.0"})
        
    def fetch_stations(self) -> list[WyssenMonitor]:
        """Returnerer konfigurerte Wyssen-stasjonar med koordinatar."""
    
        return [
            WyssenMonitor(
                source_id=_source_id(station_id),
                name=info["name"],
                longitude=info.get("longitude"),
                latitude=info.get("latitude"),
                masl=info.get("masl"),
                provider_context=json.dumps({
                    "station_id": station_id,
                    "station_name": info["name"],
                }),
            )
            for station_id, info in WYSSEN_STATIONS.items()
        ]

    def _get(self, params: list[tuple[str, str]]) -> Any:
        # Never include full requests URLs or query strings in exceptions/logs,
        # since Wyssen requires the credential in the query string.
        safe_params = [("apikey", self.api_key), ("source", "wac.machine.measurement"),
                       ("representation", "json.default"), *params]
        for attempt in range(3):
            try:
                response = self.session.get(self.base_url, params=safe_params, timeout=self.timeout_seconds)
            except requests.RequestException as exc:
                if attempt == 2:
                    raise RuntimeError(f"Wyssen nettverksfeil: {type(exc).__name__}") from None
                time.sleep(2 ** attempt)
                continue
            if response.status_code in (401, 403):
                raise WyssenAuthError(f"Wyssen avviste API-nøkkelen (HTTP {response.status_code})")
            if response.status_code in (429, 500, 502, 503, 504) and attempt < 2:
                time.sleep(2 ** attempt + 1)
                continue
            if not response.ok:
                raise RuntimeError(f"Wyssen HTTP {response.status_code}")
            try:
                return response.json()
            except ValueError:
                raise RuntimeError("Wyssen returnerte ikkje gyldig JSON") from None
        raise RuntimeError("Wyssen API-kall feila")

    def _fetch_window(self, station_id: int, codes: list[str], start: datetime,
                      end: datetime) -> list[dict[str, Any]]:
        if not codes:
            return []
        params: list[tuple[str, str]] = [
            ("start", _iso(start)), ("end", _iso(end)),
        ]
        if len(codes) == 1:
            params.extend([("station", str(station_id)), ("measurement", codes[0])])
        else:
            for code in codes:
                params.extend([("station[]", str(station_id)), ("measurement[]", code)])
        payload = self._get(params)
        if len(codes) == 1:
            if not isinstance(payload, list) or (payload and not _is_tuple(payload[0])):
                raise RuntimeError("Uventa Wyssen JSON-format for ein parameter")
            groups = [payload]
        else:
            if not isinstance(payload, list) or len(payload) != len(codes) or any(
                not isinstance(group, list) or (group and not _is_tuple(group[0])) for group in payload
            ):
                raise RuntimeError("Uventa Wyssen JSON-format for fleire parametrar")
            groups = payload
        by_time: dict[str, dict[str, dict[str, Any]]] = {}
        for code, entries in zip(codes, groups):
            logical, unit = MEASUREMENTS[code]
            for item in entries:
                if not _is_tuple(item):
                    continue
                millis = _number(item[0])
                value = _number(item[1])
                if millis is None or value is None:
                    continue
                try:
                    observed = datetime.fromtimestamp(millis / 1000, tz=timezone.utc)
                except (OverflowError, OSError, ValueError):
                    continue
                if not start <= observed <= end:
                    continue
                stamp = _iso(observed)
                by_time.setdefault(stamp, {})[logical] = {
                    "elementId": logical,
                    "value": value,
                    "unit": unit,
                    "qualityCode": None,
                }
        return [
            {"sourceId": _source_id(station_id), "referenceTime": instant,
             "observations": list(by_time[instant].values())}
            for instant in sorted(by_time)
        ]

    def fetch_observations_range(
        self,
        provider_context: str,
        from_dt: datetime,
        to_dt: datetime,
        logical_parameter_ids: list[str],
    ) -> list[dict[str, Any]]:
        context = _context(provider_context)
        station_id = int(context["station_id"])
        start, end = _utc(from_dt), _utc(to_dt)
        if end < start:
            return []
        wanted = set(logical_parameter_ids)
        codes = [
            code for code in self.measurement_codes
            if MEASUREMENTS[code][0] in wanted
        ]
        if not codes:
            return []
        dedup: dict[str, dict[str, dict[str, Any]]] = {}
        cursor = start
        while cursor < end:
            window_end = min(cursor + timedelta(days=self.history_chunk_days), end)
            for row in self._fetch_window(station_id, codes, cursor, window_end):
                at = row["referenceTime"]
                for obs in row["observations"]:
                    dedup.setdefault(at, {})[obs["elementId"]] = obs
            cursor = window_end
        if cursor == start == end:
            for row in self._fetch_window(station_id, codes, start, end):
                at = row["referenceTime"]
                for obs in row["observations"]:
                    dedup.setdefault(at, {})[obs["elementId"]] = obs
        return [
            {"sourceId": _source_id(station_id), "referenceTime": at,
             "observations": list(dedup[at].values())}
            for at in sorted(dedup)
        ]

    def fetch_latest_observations(self, monitors: list[WyssenMonitor]) -> list[dict[str, Any]]:
        end = datetime.now(timezone.utc)
        start = end - timedelta(hours=48)
        rows: list[dict[str, Any]] = []
        wanted = [MEASUREMENTS[code][0] for code in self.measurement_codes]
        for monitor in monitors:
            try:
                history = self.fetch_observations_range(monitor.provider_context, start, end, wanted)
            except WyssenAuthError:
                raise
            except (RuntimeError, ValueError) as exc:
                logger.warning("Wyssen stasjon %s feila: %s", monitor.source_id, exc)
                continue
            latest: dict[str, tuple[str, dict[str, Any]]] = {}
            for item in history:
                for obs in item["observations"]:
                    latest[obs["elementId"]] = (item["referenceTime"], obs)
            by_time: dict[str, list[dict[str, Any]]] = {}
            for at, obs in latest.values():
                by_time.setdefault(at, []).append(obs)
            rows.extend(
                {"sourceId": monitor.source_id, "referenceTime": at,
                 "observations": observations}
                for at, observations in sorted(by_time.items())
            )
        return rows
