from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from requests import HTTPError


TARGET_ELEMENTS = [
    "air_temperature",
    "sum(precipitation_amount PT1H)",
    "surface_snow_thickness",
    "snow_depth",
    "wind_from_direction",
    "wind_speed",
]


@dataclass(frozen=True)
class FrostSource:
    source_id: str
    name: str | None
    stationholder: str | None
    country: str | None
    county: str | None
    municipality: str | None
    masl: float | None
    longitude: float | None
    latitude: float | None
    valid_from: datetime | None
    valid_to: datetime | None


class FrostClient:
    def __init__(
        self,
        base_url: str,
        client_id: str,
        timeout_seconds: int = 60,
        acceptable_quality_codes: str = "0,1,2,3,4",
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.acceptable_quality_codes = acceptable_quality_codes
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.auth = (client_id, "")

    def _get(self, path: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        response = self.session.get(
            f"{self.base_url}{path}",
            params=params,
            timeout=self.timeout_seconds,
        )
        try:
            response.raise_for_status()
        except HTTPError as exc:
            if response.status_code == 404:
                return []
            raise RuntimeError(f"Frost API request failed for {path}: {response.text}") from exc
        payload = response.json()

        if payload.get("error"):
            raise RuntimeError(f"Frost API error: {payload['error']}")

        return payload.get("data", [])

    def fetch_sources(self, page_limit: int = 1000) -> list[FrostSource]:
        del page_limit

        data = self._get("/sources/v0.jsonld", self._source_query_params())

        sources: list[FrostSource] = []
        for item in data:
            geometry = item.get("geometry") or {}
            coordinates = geometry.get("coordinates") or []
            sources.append(
                FrostSource(
                    source_id=item["id"],
                    name=item.get("name"),
                    stationholder=_format_stationholders(item.get("stationHolders")),
                    country=item.get("country"),
                    county=item.get("county"),
                    municipality=item.get("municipality"),
                    masl=item.get("masl"),
                    longitude=_get_coordinate(coordinates, 0),
                    latitude=_get_coordinate(coordinates, 1),
                    valid_from=_parse_datetime(item.get("validFrom")),
                    valid_to=_parse_datetime(item.get("validTo")),
                )
            )

        return sources

    def fetch_capability_source_ids(self, element_id: str) -> set[str]:
        data = self._get(
            "/sources/v0.jsonld",
            self._source_query_params(element_id=element_id),
        )
        return {item["id"] for item in data if item.get("id")}

    def fetch_latest_observations(self, source_ids: str | list[str]) -> list[dict[str, Any]]:
        if isinstance(source_ids, list):
            source_value = ",".join(source_ids)
        else:
            source_value = source_ids

        data = self._get(
            "/observations/v0.jsonld",
            {
                "sources": source_value,
                "referencetime": "latest",
                "elements": ",".join(TARGET_ELEMENTS),
                "qualities": self.acceptable_quality_codes,
                "timeoffsets": "default",
                "levels": "default",
            },
        )
        return data

    def fetch_observations_range(
        self,
        source_ids: str | list[str],
        elements: list[str],
        from_dt: datetime,
        to_dt: datetime,
    ) -> list[dict[str, Any]]:
        if isinstance(source_ids, list):
            source_value = ",".join(source_ids)
        else:
            source_value = source_ids

        return self._get(
            "/observations/v0.jsonld",
            {
                "sources": source_value,
                "referencetime": f"{_isoformat_utc(from_dt)}/{_isoformat_utc(to_dt)}",
                "elements": ",".join(elements),
                "qualities": self.acceptable_quality_codes,
                "timeoffsets": "default",
                "levels": "default",
            },
        )

    def fetch_snow_series_ids(self, source_ids: list[str], lookback_days: int) -> list[str]:
        referencetime = _recent_range(lookback_days)
        data = self._get(
            "/observations/availableTimeSeries/v0.jsonld",
            {
                "sources": ",".join(source_ids),
                "referencetime": referencetime,
                "elements": "surface_snow_thickness",
                "timeresolutions": "P1D",
                "timeoffsets": "PT6H",
                "levels": "default",
            },
        )
        return sorted({item["sourceId"] for item in data if item.get("sourceId")})

    def fetch_recent_snow_observations(self, series_ids: list[str], lookback_days: int) -> list[dict[str, Any]]:
        if not series_ids:
            return []
        return self._get(
            "/observations/v0.jsonld",
            {
                "sources": ",".join(series_ids),
                "referencetime": _recent_range(lookback_days),
                "elements": "surface_snow_thickness",
                "timeresolutions": "P1D",
                "timeoffsets": "PT6H",
                "qualities": self.acceptable_quality_codes,
            },
        )

    def _source_query_params(self, element_id: str | None = None) -> dict[str, Any]:
        params: dict[str, Any] = {
            "country": "NO",
            "types": "SensorSystem",
            "fields": ",".join(
                [
                    "id",
                    "name",
                    "stationHolders",
                    "country",
                    "county",
                    "municipality",
                    "masl",
                    "geometry",
                    "validFrom",
                    "validTo",
                ]
            ),
        }
        if element_id:
            params["elements"] = element_id
        return params


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _get_coordinate(coordinates: list[Any], index: int) -> float | None:
    try:
        return float(coordinates[index])
    except (IndexError, TypeError, ValueError):
        return None


def _format_stationholders(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, list):
        holders = [str(item).strip() for item in value if str(item).strip()]
        return ", ".join(holders) if holders else None
    text = str(value).strip()
    return text or None


def _recent_range(days: int) -> str:
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    return f"{_isoformat_utc(start)}/{_isoformat_utc(end)}"


def _isoformat_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
