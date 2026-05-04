from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests
from requests import HTTPError


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


class NveHydApiClient:
    def __init__(self, base_url: str, api_key: str, timeout_seconds: int = 60) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
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
                    stationholder="NVE HydAPI",
                    active=_first_bool(item, "active", "Active"),
                )
            )
        return stations

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
