from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import requests
from requests import HTTPError


TARGET_ELEMENTS = [
    "air_temperature",
    "sum(precipitation_amount PT1H)",
    "snow_depth",
    "wind_from_direction",
    "wind_speed",
]


@dataclass(frozen=True)
class FrostSource:
    source_id: str
    name: str | None
    country: str | None
    county: str | None
    municipality: str | None
    masl: float | None
    longitude: float | None
    latitude: float | None
    valid_from: datetime | None
    valid_to: datetime | None


class FrostClient:
    def __init__(self, base_url: str, client_id: str, timeout_seconds: int = 60) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
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
                "timeoffsets": "default",
                "levels": "default",
            },
        )
        return data

    def _source_query_params(self, element_id: str | None = None) -> dict[str, Any]:
        params: dict[str, Any] = {
            "country": "NO",
            "types": "SensorSystem",
            "fields": ",".join(
                [
                    "id",
                    "name",
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
