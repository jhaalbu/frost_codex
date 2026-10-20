"""Tilsig API client matching the Snower provider interface.

Requires: requests. Pass TILSIG_API_KEY from your application's configuration.
No dependency on Flask, SQLAlchemy, or dotenv inside this module.
"""
from __future__ import annotations

import json
import logging
import math
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

logger = logging.getLogger(__name__)
TILSIG_SOURCE_ID_PREFIX = "TILSIG-"


class TilsigAuthError(RuntimeError):
    """API key missing or rejected."""


@dataclass(frozen=True)
class TilsigMonitor:
    source_id: str
    name: str
    longitude: float | None
    latitude: float | None
    provider_context: str


def _parse_time(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        # Application contract: naive datetimes interpreted as UTC.
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return _parse_time(value).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _number(value: Any) -> float | None:
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def _source_id(station_id: int | str) -> str:
    return f"{TILSIG_SOURCE_ID_PREFIX}{station_id}"


def _context(provider_context: str | None) -> dict[str, Any]:
    if not provider_context:
        raise ValueError("Tilsig provider_context manglar")
    value = json.loads(provider_context)
    if not isinstance(value, dict) or value.get("station_id") is None:
        raise ValueError("Ugyldig Tilsig provider_context (station_id manglar)")
    return value


def _type_for_series(serie: dict[str, Any]) -> tuple[str, float, str] | None:
    """Map ONLY defensible observed series to logical elements.

    The data series *name* is authoritative for Snow Depth Mode selection,
    because actual stations have incorrectly labelled type IDs for Avg/Max/Min.
    """
    name = str(serie.get("name") or serie.get("dataSeriesName") or "").strip().casefold()
    label = str(serie.get("measurementType") or serie.get("type") or "").casefold()
    unit = str(serie.get("measurementUnit") or serie.get("unit") or "").strip().casefold()
    mid = serie.get("measurementTypeId")

    is_snow_mode = ("snow depth" in name and "mode" in name) or name == "snø"
    if is_snow_mode and (mid == 176 or "snow depth" in label):
        if unit == "mm":
            return "snow_depth", 0.1, "cm"
        if unit == "cm":
            return "snow_depth", 1.0, "cm"
        logger.warning("Ukjend snødjupneeining %r for dataserie %s", unit, serie.get("id"))
    # Device Temperature is NOT implicitly mapped to air_temperature.
    return None


class TilsigClient:
    def __init__(
        self,
        base_url: str = "https://api.tilsig.com",
        api_key: str | None = None,
        timeout_seconds: int = 60,
        max_requests_per_minute: int = 50,
        history_chunk_days: int = 7,
    ) -> None:
        if not api_key or not api_key.strip():
            raise TilsigAuthError("TILSIG_API_KEY manglar")
        if history_chunk_days < 1:
            raise ValueError("history_chunk_days må vere minst 1")
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.history_chunk_days = history_chunk_days
        self.max_requests_per_minute = max_requests_per_minute
        self._minute = -1
        self._count = 0
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "X-W2O-Api-Key": api_key.strip(),
            "User-Agent": "frost-sync/1.0",
        })
        self._series_cache: dict[int, list[dict[str, Any]]] = {}

    def _rate_limit(self) -> None:
        now = time.time()
        minute = int(now // 60)
        if minute != self._minute:
            self._minute, self._count = minute, 0
        if self._count >= self.max_requests_per_minute:
            time.sleep(60 - (now % 60) + 0.5)
            self._minute, self._count = int(time.time() // 60), 0
        self._count += 1

    def _get(self, path: str, params: dict[str, Any] | None = None,
             allow_404: bool = False) -> Any:
        for attempt in range(3):
            self._rate_limit()
            try:
                response = self.session.get(
                    self.base_url + path, params=params, timeout=self.timeout_seconds
                )
            except requests.RequestException as exc:
                if attempt == 2:
                    raise RuntimeError(f"Tilsig transportfeil på {path}: {exc}") from exc
                time.sleep(2 ** attempt)
                continue
            if response.status_code == 404 and allow_404:
                return None
            if response.status_code in (401, 403):
                raise TilsigAuthError(f"Tilsig {response.status_code} på {path}: {response.text[:250]}")
            if response.status_code in (429, 500, 502, 503, 504) and attempt < 2:
                time.sleep(min(10, 2 ** attempt + 1))
                continue
            if not response.ok:
                raise RuntimeError(f"Tilsig HTTP {response.status_code} på {path}: {response.text[:250]}")
            try:
                return response.json()
            except ValueError as exc:
                raise RuntimeError(f"Tilsig svarte ikkje med JSON på {path}") from exc
        raise RuntimeError(f"Tilsig: kunne ikkje fullføre {path}")

    def _list_series(self, station_id: int, refresh: bool = False) -> list[dict[str, Any]]:
        if not refresh and station_id in self._series_cache:
            return self._series_cache[station_id]
        payload = self._get(f"/v1/dataseries/by-station-id/{station_id}", allow_404=True)
        if payload is None:
            logger.info("Tilsig stasjon %s: dataserie-endepunkt gav 404", station_id)
            payload = []
        if not isinstance(payload, list):
            raise RuntimeError(f"Uventa Tilsig dataserieformat for stasjon {station_id}")
        self._series_cache[station_id] = payload
        return payload

    def fetch_stations(self) -> list[TilsigMonitor]:
        """All stations, including stations without data series or coordinates."""
        payload = self._get("/v1/station/all")
        if not isinstance(payload, list):
            raise RuntimeError("Uventa format frå Tilsig /v1/station/all")
        monitors = []
        for item in payload:
            if not isinstance(item, dict) or item.get("stationId") is None:
                continue
            sid = int(item["stationId"])
            name = str(item.get("stationName") or sid)
            context = {
                "station_id": sid,
                "station_name": name,
            }
            monitors.append(TilsigMonitor(
                source_id=_source_id(sid),
                name=name,
                longitude=_number(item.get("long")),
                latitude=_number(item.get("lat")),
                provider_context=json.dumps(context, ensure_ascii=True),
            ))
        return monitors

    @staticmethod
    def _observation(logical_key: str, value: Any, factor: float, unit: str) -> dict[str, Any] | None:
        number = _number(value)
        if number is None:
            return None
        return {
            "elementId": logical_key,
            "value": number * factor,
            "unit": unit,
            "qualityCode": None,
        }

    def _fetch_series_measurements(self, data_series_id: int,
                                   from_dt: datetime, to_dt: datetime) -> list[dict[str, Any]]:
        """Fetch inclusive time windows; overlap is deduplicated by timestamp later."""
        result: list[dict[str, Any]] = []
        cursor = _parse_time(from_dt)
        end = _parse_time(to_dt)
        if cursor > end:
            return result
        # For a point query (latest, narrow test) still query the interval once.
        while cursor < end:
            window_end = min(cursor + timedelta(days=self.history_chunk_days), end)
            payload = self._get(
                f"/v1/measurement/dataseries/{data_series_id}",
                params={"from": _iso(cursor), "to": _iso(window_end)},
                allow_404=True,
            )
            if isinstance(payload, dict):
                measurements = payload.get("measurements") or []
                if isinstance(measurements, list):
                    result.extend(x for x in measurements if isinstance(x, dict))
            cursor = window_end
        return result

    def _read_station(self, monitor: TilsigMonitor, from_dt: datetime,
                      to_dt: datetime, wanted: set[str]) -> list[dict[str, Any]]:
        context = _context(monitor.provider_context)
        sid = int(context["station_id"])
        by_time: dict[str, dict[str, dict[str, Any]]] = {}
        for series in self._list_series(sid):
            mapping = _type_for_series(series)
            if mapping is None:
                continue
            logical, factor, unit = mapping
            if logical not in wanted:
                continue
            try:
                measurements = self._fetch_series_measurements(int(series["id"]), from_dt, to_dt)
            except TilsigAuthError:
                raise
            except (RuntimeError, KeyError, TypeError, ValueError) as exc:
                logger.warning("Tilsig dataserie %s feila: %s", series.get("id"), exc)
                continue
            for m in measurements:
                timestamp = m.get("timestamp")
                if not timestamp:
                    continue
                try:
                    reference = _iso(_parse_time(timestamp))
                except (ValueError, TypeError):
                    continue
                obs = self._observation(logical, m.get("value"), factor, unit)
                if obs is not None:
                    by_time.setdefault(reference, {})[logical] = obs
        return [
            {"sourceId": monitor.source_id, "referenceTime": instant,
             "observations": list(by_time[instant].values())}
            for instant in sorted(by_time)
        ]

    def fetch_latest_observations(self, monitors: list[TilsigMonitor]) -> list[dict[str, Any]]:
        """Recent readings from each supported data series (window: past 48 hours)."""
        end = datetime.now(timezone.utc)
        start = end - timedelta(hours=48)
        rows: list[dict[str, Any]] = []
        for monitor in monitors:
            try:
                history = self._read_station(monitor, start, end, {"snow_depth"})
                # Latest per logical parameter (not just latest shared timestamp).
                by_parameter: dict[str, tuple[str, dict[str, Any]]] = {}
                for row in history:
                    for obs in row["observations"]:
                        by_parameter[obs["elementId"]] = (row["referenceTime"], obs)
                grouped: dict[str, list[dict[str, Any]]] = {}
                for timestamp, observation in by_parameter.values():
                    grouped.setdefault(timestamp, []).append(observation)
                rows.extend({"sourceId": monitor.source_id, "referenceTime": ts,
                             "observations": observations}
                            for ts, observations in sorted(grouped.items()))
            except TilsigAuthError:
                raise
            except (RuntimeError, ValueError) as exc:
                logger.warning("Tilsig siste observasjon feila for %s: %s", monitor.source_id, exc)
        return rows

    def fetch_observations_range(self, provider_context: str, from_dt: datetime,
                                 to_dt: datetime, logical_parameter_ids: list[str]) -> list[dict[str, Any]]:
        try:
            context = _context(provider_context)
            sid = int(context["station_id"])
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            logger.warning("Ugyldig Tilsig provider_context: %s", exc)
            return []
        monitor = TilsigMonitor(
            source_id=_source_id(sid), name=str(context.get("station_name") or sid),
            longitude=None, latitude=None, provider_context=provider_context,
        )
        return self._read_station(monitor, from_dt, to_dt, set(logical_parameter_ids))
