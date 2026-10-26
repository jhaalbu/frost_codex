"""Skred AS / InfluxDB v2 query client for frost_sync.

Source: Skred AS, API documentation for Vikafjell cs001, 2026-10-09.
Token is passed via config (SKRED_API_TOKEN). No DB/Flask dependency.

Implements the same contract as the Wyssen/Tilsig providers:
  fetch_stations()
  fetch_latest_observations(monitors)
  fetch_observations_range(provider_context, from_dt, to_dt, logical_parameter_ids)

Uses annotated CSV from /api/v2/query, not an Influx client dependency.
"""
from __future__ import annotations

import csv
import io
import json
import logging
import math
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

import requests

logger = logging.getLogger(__name__)

SOURCE_ID_PREFIX = "SKRED-"
DEFAULT_BASE_URL = "https://api.skredvarsling.no"
DEFAULT_ORG = "skred_as"
DEFAULT_BUCKET = "skred_api_vikafjell"
DEFAULT_MEASUREMENT = "aws_vikafjell"

# The first three map directly to elements already understood by frost_sync.
# The others keep their native identifiers until service/web explicitly support them.
FIELD_UNITS: dict[str, str] = {
    "wind_speed": "m/s",
    "wind_speed_of_gust": "m/s",
    "wind_from_direction": "deg",
    "air_temperature": "degC",
    "air_temperature_min": "degC",
    "air_temperature_max": "degC",
    "relative_humidity": "%",
    "dew_point_temperature": "degC",
    "wind_speed_fc_low": "m/s",
    "wind_speed_fc_high": "m/s",
    "snow_drift_low": "g/m2/s",
    "snow_drift_high": "g/m2/s",
    "snow_drift_low_cumsum": "g/m2/10min",
    "snow_drift_high_cumsum": "g/m2/10min",
}
DEFAULT_FIELDS = ("air_temperature", "wind_speed", "wind_from_direction")
STATIONS = (
    {"station_id": "cs001", "name": "Vikafjell", "latitude": 60.962746352,
     "longitude": 6.483825087, "masl": 971.5, "station_name": "cs001_Vikafjell"},
)


class SkredAuthError(RuntimeError):
    """Token missing or rejected by the API."""


@dataclass(frozen=True)
class SkredMonitor:
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


def _float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _flux_string(value: str) -> str:
    """Use JSON string quoting for safe Flux string literals."""
    return json.dumps(value, ensure_ascii=True)


def _parse_flux_csv(body: str) -> list[dict[str, str]]:
    """Read one or more Influx annotated CSV result tables.

    Tables have independent column headers; ignore #datatype, #group, #default.
    Avoid mixing up multiple query tables when the same header repeats.
    """
    parsed: list[dict[str, str]] = []
    headers: list[str] | None = None
    for row in csv.reader(io.StringIO(body)):
        if not row or not any(cell.strip() for cell in row):
            headers = None
            continue
        if row[0].startswith("#"):
            continue
        if "_time" in row and "_field" in row and "_value" in row:
            headers = row
            continue
        if headers is None:
            continue
        if len(row) != len(headers):
            logger.warning("Skipping malformed Influx CSV row (%s cols, expected %s)", len(row), len(headers))
            continue
        parsed.append(dict(zip(headers, row)))
    return parsed


class SkredClient:
    def __init__(
        self,
        api_token: str | None = None,
        base_url: str = DEFAULT_BASE_URL,
        org: str = DEFAULT_ORG,
        bucket: str = DEFAULT_BUCKET,
        measurement: str = DEFAULT_MEASUREMENT,
        timeout_seconds: int = 60,
        history_chunk_days: int = 3,
        latest_lookback_hours: int = 4,
        measurement_fields: Iterable[str] = DEFAULT_FIELDS,
    ) -> None:
        if not api_token or not api_token.strip():
            raise SkredAuthError("SKRED_API_TOKEN manglar")
        if history_chunk_days < 1 or latest_lookback_hours < 1:
            raise ValueError("history_chunk_days og latest_lookback_hours må vere minst 1")
        fields = tuple(dict.fromkeys(measurement_fields))
        unknown = set(fields) - set(FIELD_UNITS)
        if unknown:
            raise ValueError(f"Ukjende Skred AS-felt: {sorted(unknown)}")
        self.base_url = base_url.rstrip("/")
        self.org = org
        self.bucket = bucket
        self.measurement = measurement.strip()
        self.timeout_seconds = timeout_seconds
        self.history_chunk_days = history_chunk_days
        self.latest_lookback_hours = latest_lookback_hours
        self.measurement_fields = fields
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Token {api_token.strip()}",
            "Accept": "application/csv",
            "Content-Type": "application/vnd.flux",
            "User-Agent": "frost-sync/1.0",
        })

    def fetch_stations(self) -> list[SkredMonitor]:
        """One station is documented; static metadata is the safe initial fallback."""
        return [
            SkredMonitor(
                source_id=f"{SOURCE_ID_PREFIX}{spec['station_id']}",
                name=spec["name"],
                longitude=spec["longitude"],
                latitude=spec["latitude"],
                masl=spec["masl"],
                provider_context=json.dumps({
                    "station_id": spec["station_id"],
                    "station_name": spec["station_name"],
                    "measurement": self.measurement,
                    "bucket": self.bucket,
                }),
            )
            for spec in STATIONS
        ]

    def _query(self, start: datetime, end: datetime, fields: Iterable[str]) -> list[dict[str, str]]:
        selected = sorted(set(fields) & set(FIELD_UNITS))
        if not selected:
            return []
        if _utc(end) < _utc(start):
            raise ValueError("to_dt må vere seinare enn from_dt")
        fields_filter = " or ".join(f'r._field == {_flux_string(field)}' for field in selected)
        flux = (
            f'from(bucket: {_flux_string(self.bucket)})\n'
            f'  |> range(start: time(v: {_flux_string(_iso(start))}), '
            f'stop: time(v: {_flux_string(_iso(end))}))\n'
            f'  |> filter(fn: (r) => r._measurement == {_flux_string(self.measurement)})\n'
            f'  |> filter(fn: (r) => {fields_filter})\n'
            '  |> keep(columns: ["_time", "_field", "_value", "station_name", "location_name", "lat", "lon", "alt"])\n'
            '  |> sort(columns: ["_time"])'
        )
        for attempt in range(3):
            try:
                response = self.session.post(
                    f"{self.base_url}/api/v2/query",
                    params={"org": self.org},
                    data=flux.encode("utf-8"),
                    timeout=self.timeout_seconds,
                )
            except requests.RequestException as exc:
                if attempt == 2:
                    raise RuntimeError(f"Skred API network failure: {type(exc).__name__}") from None
                time.sleep(2 ** attempt)
                continue
            if response.status_code in (401, 403):
                raise SkredAuthError(f"Skred API rejected token (HTTP {response.status_code})")
            if response.status_code in (429, 500, 502, 503, 504) and attempt < 2:
                time.sleep(2 ** attempt + 1)
                continue
            if not response.ok:
                # Never log request headers/tokens or verbose responses.
                raise RuntimeError(f"Skred API HTTP {response.status_code}")
            return _parse_flux_csv(response.text)
        raise RuntimeError("Skred API query failed")

    def _range(self, source_id: str, start: datetime, end: datetime,
               fields: Iterable[str]) -> list[dict[str, Any]]:
        grouped: dict[str, dict[str, Any]] = {}
        cursor = _utc(start)
        end = _utc(end)
        while cursor < end:
            chunk_end = min(end, cursor + timedelta(days=self.history_chunk_days))
            for row in self._query(cursor, chunk_end, fields):
                raw_time = row.get("_time", "")
                field = row.get("_field", "")
                if field not in FIELD_UNITS or not raw_time:
                    continue
                value = _float(row.get("_value"))
                if value is None:
                    continue
                try:
                    timestamp = _utc(datetime.fromisoformat(raw_time.replace("Z", "+00:00")))
                except ValueError:
                    continue
                # Do not accept unexpectedly returned rows outside caller's range.
                if timestamp < _utc(start) or timestamp > end:
                    continue
                time_key = _iso(timestamp)
                record = grouped.setdefault(time_key, {"sourceId": source_id, "referenceTime": time_key,
                                                       "observations": []})
                record["observations"].append({
                    "elementId": field, "value": value,
                    "unit": FIELD_UNITS[field], "qualityCode": None,
                })
            cursor = chunk_end
        return [grouped[key] for key in sorted(grouped)]

    def fetch_latest_observations(self, monitors: Iterable[SkredMonitor]) -> list[dict[str, Any]]:
        now = datetime.now(timezone.utc)
        start = now - timedelta(hours=self.latest_lookback_hours)
        rows: list[dict[str, Any]] = []
        for monitor in monitors:
            ctx = self._context(monitor.provider_context)
            # Current documentation covers only the one documented measurement.
            if ctx.get("measurement") != self.measurement:
                logger.warning("Skipping unrecognised Skred measurement for %s", monitor.source_id)
                continue
            rows.extend(self._range(monitor.source_id, start, now, self.measurement_fields))
        return rows

    def fetch_observations_range(
        self,
        provider_context: str | None,
        from_dt: datetime,
        to_dt: datetime,
        logical_parameter_ids: Iterable[str],
    ) -> list[dict[str, Any]]:
        ctx = self._context(provider_context)
        if ctx.get("measurement") != self.measurement:
            raise ValueError("Unknown Skred measurement in provider_context")
        requested = set(logical_parameter_ids)
        fields = requested & set(FIELD_UNITS)
        return self._range(
            f"{SOURCE_ID_PREFIX}{ctx['station_id']}", from_dt, to_dt, sorted(fields)
        )

    @staticmethod
    def _context(provider_context: str | None) -> dict[str, Any]:
        if not provider_context:
            raise ValueError("Skred provider_context manglar")
        try:
            value = json.loads(provider_context)
        except (TypeError, ValueError):
            raise ValueError("Skred provider_context er ikkje gyldig JSON") from None
        if not isinstance(value, dict) or not value.get("station_id"):
            raise ValueError("Skred provider_context manglar station_id")
        return value
