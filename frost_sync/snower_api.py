from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

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
        domain: str | None,
        domain_id: str,
        timeout_seconds: int = 60,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.domain = (domain or domain_id.capitalize()).strip()
        self.domain_id = domain_id.strip().lower()
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
        regions = self._post("/regions_list", {"domain": self.domain})
        monitors: list[SnowerMonitor] = []
        for region in regions:
            areas = self._post("/areas_list", {"region": region})
            for area in areas:
                area_monitors = self._post("/monitors_list", {"area": area})
                for monitor_name in area_monitors:
                    monitor_body = {"area": area, "monitor": monitor_name}
                    status = self._post("/monitor_active_status", monitor_body)
                    if not bool(status.get("active_status")):
                        continue
                    location = self._post("/monitor_location", monitor_body).get("location", {})
                    context = {
                        "domain": self.domain,
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
        now = datetime.now(ZoneInfo("Europe/Oslo"))
        start = now - timedelta(days=1)
        for monitor in monitors:
            context = parse_provider_context(monitor.provider_context)
            payload = self._post(
                "/readings_list",
                {
                    "area": context["area"],
                    "monitor": context["monitor"],
                    "start_date": _snower_datetime(start),
                    "end_date": _snower_datetime(now),
                },
            )
            latest_rows = self._latest_rows_from_readings_payload(
                source_id=monitor.source_id,
                payload=payload,
            )
            rows.extend(latest_rows)
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
            },
        )
        rows = self._normalize_readings_payload(
            source_id=_monitor_source_id(context["area"], context["monitor"]),
            payload=payload,
        )
        if requested_types:
            allowed = set(logical_parameter_ids)
            for row in rows:
                row["observations"] = [
                    item for item in row.get("observations", [])
                    if item.get("elementId") in allowed
                ]
            rows = [row for row in rows if row.get("observations")]
        return rows

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

    def _latest_rows_from_readings_payload(self, source_id: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
        rows = self._normalize_readings_payload(source_id=source_id, payload=payload)
        latest_by_parameter: dict[str, dict[str, Any]] = {}
        for row in rows:
            reference_time = row.get("referenceTime")
            if not reference_time:
                continue
            reference_dt = _parse_datetime(reference_time)
            for item in row.get("observations", []):
                element_id = item.get("elementId")
                if not element_id:
                    continue
                existing = latest_by_parameter.get(element_id)
                if existing is None or reference_dt > existing["reference_dt"]:
                    latest_by_parameter[element_id] = {
                        "reference_dt": reference_dt,
                        "reference_time": reference_time,
                        "item": item,
                    }

        grouped: dict[str, dict[str, Any]] = {}
        for entry in latest_by_parameter.values():
            reference_time = entry["reference_time"]
            row = grouped.setdefault(
                reference_time,
                {
                    "sourceId": source_id,
                    "referenceTime": reference_time,
                    "observations": [],
                },
            )
            row["observations"].append(entry["item"])
        return list(grouped.values())

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
                "Content-Type": "application/json",
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
    return value.astimezone(ZoneInfo("Europe/Oslo")).strftime("%Y-%m-%d %H:%M:%S.%f%z")


def _normalize_unit(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.casefold() in {"celcius", "celsius"}:
        return "degC"
    return text


def _as_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
