from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests
from requests import HTTPError


SNOWER_SNOW_DEPTH_ELEMENT = "snow_depth"
SNOWER_PROBE_TEMPERATURE_ELEMENT = "probe_temperature"
SNOWER_TO_LOGICAL_ELEMENT = {
    SNOWER_SNOW_DEPTH_ELEMENT: "snow_depth",
    SNOWER_PROBE_TEMPERATURE_ELEMENT: "air_temperature",
}


@dataclass(frozen=True)
class SnowerMonitor:
    source_id: str
    name: str
    longitude: float | None
    latitude: float | None
    provider_context: str


class SnowerClient:
    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        domain_id: str,
        timeout_seconds: int = 60,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.domain_id = domain_id
        self.timeout_seconds = timeout_seconds
        self._authentication_key: str | None = None
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": "frost-sync/1.0",
            }
        )

    def fetch_stations(self) -> list[SnowerMonitor]:
        regions = self._post("/regions_list", {"domain": self.domain_id})
        monitors: list[SnowerMonitor] = []
        for region in regions:
            areas = self._post("/areas_list", {"region": region})
            for area in areas:
                area_monitors = self._post("/monitors_list", {"area": area})
                for monitor_name in area_monitors:
                    location = self._post("/monitor_location", {"area": area, "monitor": monitor_name}).get("location", {})
                    context = {
                        "domain_id": self.domain_id,
                        "area": area,
                        "monitor": monitor_name,
                    }
                    monitors.append(
                        SnowerMonitor(
                            source_id=_monitor_source_id(area, monitor_name),
                            name=str(monitor_name),
                            longitude=_as_float(location.get("lng")),
                            latitude=_as_float(location.get("lat")),
                            provider_context=json.dumps(context, ensure_ascii=True),
                        )
                    )
        return monitors

    def fetch_latest_observations(self, monitors: list[SnowerMonitor]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for monitor in monitors:
            context = parse_provider_context(monitor.provider_context)
            payload = self._post(
                "/last_reading",
                {
                    "area": context["area"],
                    "monitor": context["monitor"],
                    "readings_types": sorted(SNOWER_TO_LOGICAL_ELEMENT.keys()),
                },
            )
            rows.extend(self._normalize_last_reading_payload(monitor.source_id, payload))
        return rows

    def fetch_observations_range(
        self,
        provider_context: str,
        from_dt: datetime,
        to_dt: datetime,
        logical_parameter_ids: list[str],
    ) -> list[dict[str, Any]]:
        requested_types = [
            source_key
            for source_key, logical_key in SNOWER_TO_LOGICAL_ELEMENT.items()
            if logical_key in logical_parameter_ids
        ]
        if not requested_types:
            return []

        context = parse_provider_context(provider_context)
        payload = self._post(
            "/readings_list",
            {
                "area": context["area"],
                "monitor": context["monitor"],
                "start_date": _snower_datetime(from_dt),
                "end_date": _snower_datetime(to_dt),
                "readings_types": requested_types,
            },
        )
        return self._normalize_readings_payload(
            source_id=_monitor_source_id(context["area"], context["monitor"]),
            payload=payload,
        )

    def _normalize_last_reading_payload(self, source_id: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
        grouped_rows: dict[str, dict[str, Any]] = {}
        for source_key, logical_key in SNOWER_TO_LOGICAL_ELEMENT.items():
            reading = payload.get(source_key)
            if not isinstance(reading, dict):
                continue
            reference_time = reading.get("date_time")
            if not reference_time:
                continue
            row = grouped_rows.setdefault(
                reference_time,
                {
                    "sourceId": source_id,
                    "referenceTime": reference_time,
                    "observations": [],
                },
            )
            row["observations"].append(
                {
                    "elementId": logical_key,
                    "value": reading.get("value"),
                    "unit": _normalize_unit(reading.get("unit")),
                    "qualityCode": None,
                }
            )
        return list(grouped_rows.values())

    def _normalize_readings_payload(self, source_id: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for reference_time, values in payload.items():
            if not isinstance(values, dict):
                continue
            observations: list[dict[str, Any]] = []
            for source_key, reading in values.items():
                logical_key = SNOWER_TO_LOGICAL_ELEMENT.get(source_key)
                if logical_key is None or not isinstance(reading, dict):
                    continue
                observations.append(
                    {
                        "elementId": logical_key,
                        "value": reading.get("value"),
                        "unit": _normalize_unit(reading.get("unit")),
                        "qualityCode": None,
                    }
                )
            if observations:
                rows.append(
                    {
                        "sourceId": source_id,
                        "referenceTime": _normalize_reference_time(reference_time),
                        "observations": observations,
                    }
                )
        return rows

    def _authenticate(self) -> None:
        if self._authentication_key:
            return

        response = self.session.post(
            f"{self.base_url}/login",
            json={"username": self.username, "password": self.password},
            timeout=self.timeout_seconds,
        )
        try:
            response.raise_for_status()
        except HTTPError as exc:
            raise RuntimeError(f"Snower request failed for /login: {response.text}") from exc

        payload = response.json()
        authentication_key = payload.get("authentication_key")
        if not authentication_key:
            raise RuntimeError("Snower login response did not include authentication_key")
        self._authentication_key = authentication_key

    def _post(self, path: str, body: dict[str, Any]) -> Any:
        self._authenticate()
        response = self.session.post(
            f"{self.base_url}{path}",
            json=body,
            headers={
                "authentication-key": self._authentication_key or "",
                "domain-id": self.domain_id,
            },
            timeout=self.timeout_seconds,
        )
        try:
            response.raise_for_status()
        except HTTPError as exc:
            raise RuntimeError(f"Snower request failed for {path}: {response.text}") from exc
        return response.json()


def parse_provider_context(provider_context: str | None) -> dict[str, str]:
    if not provider_context:
        raise RuntimeError("Missing provider_context for Snower station")
    parsed = json.loads(provider_context)
    if not isinstance(parsed, dict):
        raise RuntimeError("Invalid provider_context for Snower station")
    return {key: str(value) for key, value in parsed.items()}


def _monitor_source_id(area: str, monitor: str) -> str:
    digest = hashlib.sha1(f"{area}|{monitor}".encode("utf-8")).hexdigest()[:16].upper()
    return f"SNWR-{digest}"


def _normalize_reference_time(value: str) -> str:
    parsed = _parse_datetime(value)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_datetime(value: str) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        return datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    if "T" in normalized and ("+" in normalized[10:] or normalized.endswith("00:00")):
        parsed = datetime.fromisoformat(normalized)
    elif "+" in normalized[10:] or normalized.endswith("00:00"):
        parsed = datetime.fromisoformat(normalized.replace(" ", "T"))
    else:
        parsed = datetime.fromisoformat(normalized.replace(" ", "T"))
        parsed = parsed.replace(tzinfo=timezone.utc)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _snower_datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f+00:00")


def _normalize_unit(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.casefold() == "celcius":
        return "degC"
    return text


def _as_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
