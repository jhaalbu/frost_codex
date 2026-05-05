from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests
from requests import HTTPError


AIR_TEMPERATURE_ELEMENT = "air_temperature"
PRECIPITATION_1H_ELEMENT = "precipitation_1h"
SNOW_DEPTH_ELEMENT = "snow_depth"
WIND_SPEED_ELEMENT = "wind_speed"
WIND_DIRECTION_ELEMENT = "wind_from_direction"
DISCHARGE_ELEMENT = "discharge"
GROUNDWATER_LEVEL_ELEMENT = "groundwater_level"

SUPPORTED_LOGICAL_ELEMENTS = (
    AIR_TEMPERATURE_ELEMENT,
    PRECIPITATION_1H_ELEMENT,
    SNOW_DEPTH_ELEMENT,
    WIND_SPEED_ELEMENT,
    WIND_DIRECTION_ELEMENT,
    DISCHARGE_ELEMENT,
    GROUNDWATER_LEVEL_ELEMENT,
)

RESOLUTION_PREFERENCES = {
    AIR_TEMPERATURE_ELEMENT: (0, 60, 1440),
    PRECIPITATION_1H_ELEMENT: (60,),
    SNOW_DEPTH_ELEMENT: (1440, 60, 0),
    WIND_SPEED_ELEMENT: (0, 60, 1440),
    WIND_DIRECTION_ELEMENT: (0, 60, 1440),
    DISCHARGE_ELEMENT: (0, 60, 1440),
    GROUNDWATER_LEVEL_ELEMENT: (0, 60, 1440),
}


@dataclass(frozen=True)
class NveHydApiSeriesSpec:
    source_id: str
    parameter: str
    logical_element_id: str
    resolution_time: int
    unit: str | None


@dataclass(frozen=True)
class NveHydApiStation:
    source_id: str
    name: str | None
    country: str | None
    county: str | None
    municipality: str | None
    masl: float | None
    longitude: float | None
    latitude: float | None
    stationholder: str | None
    active: bool | None
    series_specs: tuple[NveHydApiSeriesSpec, ...] = ()


class NveHydApiClient:
    def __init__(self, base_url: str, api_key: str, timeout_seconds: int = 60) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self._last_observations_call_at = 0.0
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.headers.update(
            {
                "Accept": "application/json",
                "X-API-Key": api_key,
                "User-Agent": "frost-sync/1.0",
            }
        )

    def fetch_stations(self, only_active: bool = True) -> list[NveHydApiStation]:
        params: dict[str, Any] = {}
        if only_active:
            params["Active"] = "OnlyActive"

        payload = self._get("/Stations", params=params)
        stations: list[NveHydApiStation] = []
        for item in payload.get("data", []):
            source_id = _first_non_empty(item, "stationId", "StationId")
            if not source_id:
                continue
            stations.append(
                NveHydApiStation(
                    source_id=source_id,
                    name=_first_non_empty(item, "stationName", "StationName", "name", "Name"),
                    country="NO",
                    county=_first_non_empty(item, "countyName", "CountyName", "county", "County"),
                    municipality=_first_non_empty(item, "councilName", "CouncilName", "municipality", "Municipality"),
                    masl=_first_float(item, "altitude", "Altitude", "masl", "Masl"),
                    longitude=_first_float(item, "longitude", "Longitude", "lon", "Lon"),
                    latitude=_first_float(item, "latitude", "Latitude", "lat", "Lat"),
                    stationholder=_build_stationholder(item),
                    active=_first_bool(item, "active", "Active"),
                    series_specs=tuple(_extract_series_specs(source_id, item)),
                )
            )
        return stations

    def fetch_latest_observations(self, series_specs: list[NveHydApiSeriesSpec]) -> list[dict[str, Any]]:
        if not series_specs:
            return []

        spec_by_key = {
            (spec.source_id, str(spec.parameter)): spec
            for spec in series_specs
        }
        grouped_rows: dict[tuple[str, str], dict[str, Any]] = {}

        for batch in _chunked(series_specs, 10):
            response_data = self._post(
                "/Observations",
                [
                    {
                        "stationId": spec.source_id,
                        "parameter": spec.parameter,
                        "resolutionTime": str(spec.resolution_time),
                    }
                    for spec in batch
                ],
            ).get("data", [])

            for item in response_data:
                source_id = _first_non_empty(item, "stationId", "StationId")
                parameter = _first_non_empty(item, "parameter", "Parameter")
                if not source_id or parameter is None:
                    continue

                spec = spec_by_key.get((source_id, parameter))
                if spec is None:
                    continue

                unit = _first_non_empty(item, "unit", "Unit") or spec.unit
                for observation in item.get("observations", []):
                    reference_time = _first_non_empty(observation, "time", "Time")
                    if not reference_time:
                        continue
                    row_key = (source_id, reference_time)
                    row = grouped_rows.setdefault(
                        row_key,
                        {
                            "sourceId": source_id,
                            "referenceTime": reference_time,
                            "observations": [],
                        },
                    )
                    row["observations"].append(
                        {
                            "elementId": spec.logical_element_id,
                            "value": observation.get("value"),
                            "unit": unit,
                            "qualityCode": observation.get("quality"),
                        }
                    )

        return list(grouped_rows.values())

    def fetch_series_specs(self) -> list[NveHydApiSeriesSpec]:
        payload = self._get("/Series", params={})
        return _select_series_specs(payload.get("data", []))

    def fetch_series_specs_for_station(self, source_id: str) -> list[NveHydApiSeriesSpec]:
        payload = self._get("/Series", params={"StationId": source_id})
        return _select_series_specs(payload.get("data", []))

    def fetch_observations_range(
        self,
        series_specs: list[NveHydApiSeriesSpec],
        from_dt: datetime,
        to_dt: datetime,
    ) -> list[dict[str, Any]]:
        if not series_specs:
            return []

        spec_by_key = {(spec.source_id, str(spec.parameter)): spec for spec in series_specs}
        grouped_rows: dict[tuple[str, str], dict[str, Any]] = {}
        reference_time = f"{_isoformat_utc(from_dt)}/{_isoformat_utc(to_dt)}"

        for batch in _chunked(series_specs, 10):
            response_data = self._post(
                "/Observations",
                [
                    {
                        "stationId": spec.source_id,
                        "parameter": spec.parameter,
                        "resolutionTime": str(spec.resolution_time),
                        "referenceTime": reference_time,
                    }
                    for spec in batch
                ],
            ).get("data", [])

            for item in response_data:
                source_id = _first_non_empty(item, "stationId", "StationId")
                parameter = _first_non_empty(item, "parameter", "Parameter")
                if not source_id or parameter is None:
                    continue
                spec = spec_by_key.get((source_id, parameter))
                if spec is None:
                    continue
                unit = _first_non_empty(item, "unit", "Unit") or spec.unit
                for observation in item.get("observations", []):
                    reference = _first_non_empty(observation, "time", "Time")
                    if not reference:
                        continue
                    row = grouped_rows.setdefault(
                        (source_id, reference),
                        {
                            "sourceId": source_id,
                            "referenceTime": reference,
                            "observations": [],
                        },
                    )
                    row["observations"].append(
                        {
                            "elementId": spec.logical_element_id,
                            "value": observation.get("value"),
                            "unit": unit,
                            "qualityCode": observation.get("quality"),
                        }
                    )

        return list(grouped_rows.values())

    def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        response = self.session.get(
            f"{self.base_url}{path}",
            params=params,
            timeout=self.timeout_seconds,
        )
        try:
            response.raise_for_status()
        except HTTPError as exc:
            raise RuntimeError(f"NVE HydAPI request failed for {path}: {response.text}") from exc
        return response.json()

    def _post(self, path: str, body: list[dict[str, str]]) -> dict[str, Any]:
        if path == "/Observations":
            return self._post_with_rate_limit(path, body)

        response = self.session.post(
            f"{self.base_url}{path}",
            json=body,
            timeout=self.timeout_seconds,
        )
        try:
            response.raise_for_status()
        except HTTPError as exc:
            raise RuntimeError(f"NVE HydAPI request failed for {path}: {response.text}") from exc
        return response.json()

    def _post_with_rate_limit(self, path: str, body: list[dict[str, str]]) -> dict[str, Any]:
        for attempt in range(4):
            now = time.monotonic()
            wait_seconds = 0.22 - (now - self._last_observations_call_at)
            if wait_seconds > 0:
                time.sleep(wait_seconds)

            response = self.session.post(
                f"{self.base_url}{path}",
                json=body,
                timeout=self.timeout_seconds,
            )
            self._last_observations_call_at = time.monotonic()

            if response.status_code != 429:
                try:
                    response.raise_for_status()
                except HTTPError as exc:
                    raise RuntimeError(f"NVE HydAPI request failed for {path}: {response.text}") from exc
                return response.json()

            time.sleep(1.0 + attempt * 0.5)

        raise RuntimeError(f"NVE HydAPI request failed for {path}: {response.text}")


def _extract_series_specs(source_id: str, station_item: dict[str, Any]) -> list[NveHydApiSeriesSpec]:
    raw_series = _first_list(station_item, "seriesList", "SeriesList", "series", "Series")
    if not raw_series:
        return []

    selected: dict[str, NveHydApiSeriesSpec] = {}
    for raw_item in raw_series:
        if not isinstance(raw_item, dict):
            continue

        logical_element_id = _logical_element_id(raw_item)
        if logical_element_id is None:
            continue

        parameter = _first_non_empty(raw_item, "parameter", "Parameter", "parameterId", "ParameterId")
        if not parameter:
            continue

        resolution_time = _preferred_resolution_time(logical_element_id, raw_item)
        if resolution_time is None:
            continue

        candidate = NveHydApiSeriesSpec(
            source_id=source_id,
            parameter=parameter,
            logical_element_id=logical_element_id,
            resolution_time=resolution_time,
            unit=_first_non_empty(raw_item, "unit", "Unit"),
        )

        existing = selected.get(logical_element_id)
        if existing is None or _is_better_resolution(
            logical_element_id=logical_element_id,
            candidate_resolution=resolution_time,
            existing_resolution=existing.resolution_time,
        ):
            selected[logical_element_id] = candidate

    return list(selected.values())


def _preferred_resolution_time(logical_element_id: str, series_item: dict[str, Any]) -> int | None:
    available = {
        resolution
        for resolution in _extract_resolution_times(series_item)
        if resolution is not None
    }
    if not available:
        fallback = _first_int(series_item, "resolutionTime", "ResolutionTime")
        if fallback is not None:
            available.add(fallback)

    if not available:
        return None

    for preferred in RESOLUTION_PREFERENCES.get(logical_element_id, ()):
        if preferred in available:
            return preferred

    return sorted(available)[0]


def _extract_resolution_times(series_item: dict[str, Any]) -> list[int | None]:
    resolution_items = _first_list(
        series_item,
        "resolutionList",
        "ResolutionList",
        "resolutions",
        "Resolutions",
    )
    if not resolution_items:
        return []

    return [
        _parse_resolution_value(
            _first_non_empty(item, "resTime", "ResTime", "resolutionTime", "ResolutionTime", "timeResolution", "TimeResolution")
        )
        for item in resolution_items
        if isinstance(item, dict)
    ]


def _logical_element_id(item: dict[str, Any]) -> str | None:
    parameter_id = _first_int(item, "parameter", "Parameter", "parameterId", "ParameterId")
    searchable = " ".join(
        filter(
            None,
            [
                _first_non_empty(item, "parameterName", "ParameterName"),
                _first_non_empty(item, "parameterNameEng", "ParameterNameEng"),
                _first_non_empty(item, "name", "Name"),
                _first_non_empty(item, "nameEng", "NameEng"),
            ],
        )
    )
    normalized = _normalize_text(searchable)

    if parameter_id == 17 or "air temperature" in normalized or "lufttemperatur" in normalized:
        return AIR_TEMPERATURE_ELEMENT
    if parameter_id == 1001 or "discharge" in normalized or "vannforing" in normalized:
        return DISCHARGE_ELEMENT
    if (
        ("groundwater" in normalized or "grunnvann" in normalized or "grunnvass" in normalized)
        and (
            "level" in normalized
            or "stand" in normalized
            or "niva" in normalized
        )
    ):
        return GROUNDWATER_LEVEL_ELEMENT
    if "precipitation" in normalized or "nedbor" in normalized:
        return PRECIPITATION_1H_ELEMENT
    if "snow depth" in normalized or "snodybde" in normalized:
        return SNOW_DEPTH_ELEMENT
    if "wind speed" in normalized or "vindhastighet" in normalized:
        return WIND_SPEED_ELEMENT
    if "wind direction" in normalized or "vindretning" in normalized:
        return WIND_DIRECTION_ELEMENT
    return None


def _is_better_resolution(logical_element_id: str, candidate_resolution: int, existing_resolution: int) -> bool:
    preference_order = RESOLUTION_PREFERENCES.get(logical_element_id, ())
    try:
        return preference_order.index(candidate_resolution) < preference_order.index(existing_resolution)
    except ValueError:
        return candidate_resolution < existing_resolution


def _build_stationholder(item: dict[str, Any]) -> str:
    owner = _first_non_empty(
        item,
        "stationOwner",
        "StationOwner",
        "ownerName",
        "OwnerName",
        "stationHolder",
        "StationHolder",
    )
    if owner:
        return f"NVE HydAPI / {owner}"
    return "NVE HydAPI"


def _first_list(item: dict[str, Any], *keys: str) -> list[Any] | None:
    for key in keys:
        value = item.get(key)
        if isinstance(value, list):
            return value
    return None


def _first_non_empty(item: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = item.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _first_float(item: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = item.get(key)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _first_int(item: dict[str, Any], *keys: str) -> int | None:
    for key in keys:
        value = item.get(key)
        if value is None:
            continue
        parsed = _parse_resolution_value(value)
        if parsed is not None:
            return parsed
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _first_bool(item: dict[str, Any], *keys: str) -> bool | None:
    for key in keys:
        value = item.get(key)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            normalized = value.strip().casefold()
            if normalized in {"true", "1", "yes"}:
                return True
            if normalized in {"false", "0", "no"}:
                return False
    return None


def _parse_resolution_value(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)

    normalized = str(value).strip().casefold()
    named = {
        "inst": 0,
        "instant": 0,
        "instantaneous": 0,
        "hour": 60,
        "hourly": 60,
        "day": 1440,
        "daily": 1440,
    }
    if normalized in named:
        return named[normalized]
    try:
        return int(normalized)
    except ValueError:
        return None


def _normalize_text(value: str) -> str:
    return (
        value.casefold()
        .replace("ø", "o")
        .replace("å", "a")
        .replace("æ", "ae")
    )


def _chunked(items: list[NveHydApiSeriesSpec], chunk_size: int) -> list[list[NveHydApiSeriesSpec]]:
    return [items[index:index + chunk_size] for index in range(0, len(items), chunk_size)]


def _select_series_specs(items: list[dict[str, Any]]) -> list[NveHydApiSeriesSpec]:
    selected: dict[tuple[str, str], NveHydApiSeriesSpec] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        source_id = _first_non_empty(item, "stationId", "StationId")
        if not source_id:
            continue
        logical_element_id = _logical_element_id(item)
        if logical_element_id is None:
            continue
        parameter = _first_non_empty(item, "parameter", "Parameter", "parameterId", "ParameterId")
        if not parameter:
            continue
        resolution_time = _preferred_resolution_time(logical_element_id, item)
        if resolution_time is None:
            continue
        candidate = NveHydApiSeriesSpec(
            source_id=source_id,
            parameter=parameter,
            logical_element_id=logical_element_id,
            resolution_time=resolution_time,
            unit=_first_non_empty(item, "unit", "Unit"),
        )
        key = (source_id, logical_element_id)
        existing = selected.get(key)
        if existing is None or _is_better_resolution(
            logical_element_id=logical_element_id,
            candidate_resolution=resolution_time,
            existing_resolution=existing.resolution_time,
        ):
            selected[key] = candidate
    return list(selected.values())


def _isoformat_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
