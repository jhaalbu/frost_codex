from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import requests
from requests import HTTPError

from frost_sync.models import Station, StationLatest


NVDB_OBJECT_TYPE_AVALANCHE = 445
NVDB_BASE_URL = "https://nvdbapiles.atlas.vegvesen.no"
NVE_GTS_BASE_URL = "https://gts.nve.no/api"
DEFAULT_GTS_THEMES = [
    "rr3h",
    "tm3h",
    "sd",
    "fsw",
    "sdfsw3d",
    "additional_snow_depth",
    "windSpeed10m3h",
    "windDirection10m3h",
    "qswenergy3h",
]


@dataclass(frozen=True)
class AvalancheEvent:
    nvdb_id: int | None
    event_type: str | None
    event_date: str | None
    vegsystemreferanse: str | None
    municipality: list[int]
    county: list[int]
    properties: dict[str, Any]


class NvdbClient:
    def __init__(self, timeout_seconds: int = 60, session: requests.Session | None = None) -> None:
        self.timeout_seconds = timeout_seconds
        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": "frost-sync-avalanche/1.0",
                "X-Client": "frost-sync-avalanche",
            }
        )

    def fetch_snow_avalanche_events(self, road: str, segment: str, max_pages: int = 10) -> list[AvalancheEvent]:
        url, params = self._avalanche_request(road, segment)

        events: list[AvalancheEvent] = []
        next_url: str | None = url
        next_params: dict[str, Any] | None = params

        for _ in range(max_pages):
            if not next_url:
                break
            response = self.session.get(
                next_url,
                params=next_params,
                timeout=self.timeout_seconds,
                headers={"X-Request-ID": str(uuid4())},
            )
            self._raise_for_status(response)
            payload = response.json()

            for item in payload.get("objekter", []):
                detail = self._fetch_object_detail(item)
                properties = _extract_property_map(detail.get("egenskaper"))
                event_type = _match_property(properties, "Type skred")
                if not _is_snow_avalanche(event_type):
                    continue

                location = detail.get("lokasjon") or {}
                events.append(
                    AvalancheEvent(
                        nvdb_id=detail.get("id") or item.get("id"),
                        event_type=event_type,
                        event_date=_resolve_event_date(detail, properties),
                        vegsystemreferanse=_resolve_vegsystemreferanse(location),
                        municipality=location.get("kommuner") or [],
                        county=location.get("fylker") or [],
                        properties=properties,
                    )
                )

            next_link = ((payload.get("metadata") or {}).get("neste") or {}).get("href")
            next_url = next_link
            next_params = None

        return events

    def fetch_raw_avalanche_objects(self, road: str, segment: str, max_pages: int = 2) -> dict[str, Any]:
        url, params = self._avalanche_request(road, segment)
        pages: list[dict[str, Any]] = []
        next_url: str | None = url
        next_params: dict[str, Any] | None = params

        for _ in range(max_pages):
            if not next_url:
                break
            response = self.session.get(
                next_url,
                params=next_params,
                timeout=self.timeout_seconds,
                headers={"X-Request-ID": str(uuid4())},
            )
            self._raise_for_status(response)
            payload = response.json()
            payload["_detailed_objects"] = [
                self._fetch_object_detail(item) for item in payload.get("objekter", [])
            ]
            pages.append(payload)
            next_url = ((payload.get("metadata") or {}).get("neste") or {}).get("href")
            next_params = None

        return {
            "road": road,
            "segment": segment,
            "request": {
                "url": url,
                "params": params,
            },
            "pages": pages,
        }

    def _avalanche_request(self, road: str, segment: str) -> tuple[str, dict[str, Any]]:
        vegsystemreferanse = _build_vegsystemreferanse(road, segment)
        return (
            f"{NVDB_BASE_URL}/vegobjekter/api/v4/vegobjekter/{NVDB_OBJECT_TYPE_AVALANCHE}",
            {
                "vegsystemreferanse": vegsystemreferanse,
                "antall": 200,
            },
        )

    def _fetch_object_detail(self, item: dict[str, Any]) -> dict[str, Any]:
        href = item.get("href")
        if not href:
            return item
        response = self.session.get(
            href,
            timeout=self.timeout_seconds,
            headers={"X-Request-ID": str(uuid4())},
        )
        self._raise_for_status(response)
        return response.json()

    def _raise_for_status(self, response: requests.Response) -> None:
        try:
            response.raise_for_status()
        except HTTPError as exc:
            detail = response.text.strip() or "<empty response body>"
            raise RuntimeError(
                f"NVDB request failed with {response.status_code}: {detail}"
            ) from exc


class NveGtsClient:
    def __init__(self, timeout_seconds: int = 60, session: requests.Session | None = None) -> None:
        self.timeout_seconds = timeout_seconds
        self.session = session or requests.Session()

    def fetch_latest_values(
        self,
        x: int,
        y: int,
        start_date: date,
        end_date: date,
        themes: list[str] | None = None,
    ) -> dict[str, dict[str, Any]]:
        results: dict[str, dict[str, Any]] = {}
        for theme in themes or DEFAULT_GTS_THEMES:
            url = f"{NVE_GTS_BASE_URL}/GridTimeSeries/{x}/{y}/{start_date.isoformat()}/{end_date.isoformat()}/{theme}.json"
            response = self.session.get(url, timeout=self.timeout_seconds)
            response.raise_for_status()
            payload = response.json()
            latest_value = _latest_gts_value(payload)
            if latest_value is None:
                continue
            results[theme] = {
                "value": latest_value,
                "unit": payload.get("Unit"),
                "time_resolution": payload.get("TimeResolution"),
                "full_name": payload.get("FullName"),
            }
        return results


def build_avalanche_risk_payload(
    station: Station,
    latest: StationLatest | None,
    events: list[AvalancheEvent],
    road: str,
    segment: str,
    gts_data: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    risk_score = 0
    drivers: list[str] = []
    components: dict[str, int] = {}

    recent_events = _count_recent_events(events, years=10)
    if recent_events:
        points = min(20, recent_events * 4)
        risk_score += points
        components["historiske_skred"] = points
        drivers.append(f"{recent_events} registrerte snoskredhendingar siste 10 ar")

    if latest is not None:
        risk_score += _add_station_component(
            latest.precipitation_24h,
            thresholds=((30.0, 22), (15.0, 14), (8.0, 8)),
            component_name="nedbor_24h",
            label="betydelig nedbor siste 24 timar",
            components=components,
            drivers=drivers,
        )
        risk_score += _add_station_component(
            latest.precipitation_3h,
            thresholds=((10.0, 16), (5.0, 10)),
            component_name="nedbor_3h",
            label="hoy nedbor siste 3 timar",
            components=components,
            drivers=drivers,
        )
        risk_score += _add_station_component(
            latest.wind_speed_max,
            thresholds=((15.0, 16), (10.0, 10), (8.0, 6)),
            component_name="vind",
            label="kraftig vind siste dogn",
            components=components,
            drivers=drivers,
        )
        risk_score += _add_station_component(
            latest.snow_depth_change,
            thresholds=((15.0, 18), (8.0, 12), (3.0, 6)),
            component_name="snodybde_endring",
            label="okande snodybde siste dogn",
            components=components,
            drivers=drivers,
        )
        if latest.air_temperature is not None and -5.0 <= latest.air_temperature <= 1.0:
            risk_score += 8
            components["temperatur_nar_null"] = 8
            drivers.append("temperatur i kritisk omrade for fokksno/vatn i snodekket")
        if latest.air_temperature is not None and latest.air_temperature > 0.0 and (latest.precipitation_24h or 0.0) > 5.0:
            risk_score += 10
            components["mildver_med_nedbor"] = 10
            drivers.append("mildver kombinert med nedbor")

    if gts_data:
        risk_score += _add_gts_component(
            gts_data,
            theme="fsw",
            thresholds=((20.0, 18), (10.0, 12), (5.0, 6)),
            component_name="gts_nysno_1d",
            label="nysno siste dogn i griddata",
            components=components,
            drivers=drivers,
        )
        risk_score += _add_gts_component(
            gts_data,
            theme="sdfsw3d",
            thresholds=((30.0, 20), (15.0, 12), (8.0, 6)),
            component_name="gts_nysnodybde_3d",
            label="nysnodybde siste 3 dogn i griddata",
            components=components,
            drivers=drivers,
        )
        risk_score += _add_gts_component(
            gts_data,
            theme="additional_snow_depth",
            thresholds=((10.0, 18), (5.0, 10)),
            component_name="gts_fokksno",
            label="forhoya fokksnoindeks i griddata",
            components=components,
            drivers=drivers,
        )
        risk_score += _add_gts_component(
            gts_data,
            theme="qswenergy3h",
            thresholds=((10.0, 14), (5.0, 8)),
            component_name="gts_snosmelting",
            label="snosmelting siste 3 timar i griddata",
            components=components,
            drivers=drivers,
        )

    risk_score = min(100, risk_score)
    return {
        "road": road,
        "segment": segment,
        "station": station.source_id,
        "station_name": station.name,
        "assessment_time": _isoformat_utc(datetime.now(timezone.utc)),
        "risk_score": risk_score,
        "risk_level": _risk_level(risk_score),
        "drivers": drivers,
        "components": components,
        "history": {
            "snow_avalanche_event_count": len(events),
            "recent_event_count_10y": recent_events,
            "last_event_date": _latest_event_date(events),
            "events": [
                {
                    "nvdb_id": event.nvdb_id,
                    "event_date": event.event_date,
                    "event_type": event.event_type,
                    "vegsystemreferanse": event.vegsystemreferanse,
                    "properties": event.properties,
                }
                for event in events[:25]
            ],
        },
        "latest_weather": _latest_station_payload(station, latest),
        "gts_weather": gts_data or {},
    }


def _latest_station_payload(station: Station, latest: StationLatest | None) -> dict[str, Any]:
    payload = {
        "source_id": station.source_id,
        "name": station.name,
        "stationholder": station.stationholder,
        "latitude": station.latitude,
        "longitude": station.longitude,
    }
    if latest is None:
        payload["latest"] = None
        return payload

    payload["latest"] = {
        "observed_at": _isoformat_utc(latest.observed_at),
        "air_temperature": latest.air_temperature,
        "precipitation_3h": latest.precipitation_3h,
        "precipitation_24h": latest.precipitation_24h,
        "snow_depth": latest.snow_depth,
        "snow_depth_change": latest.snow_depth_change,
        "wind_speed": latest.wind_speed,
        "wind_speed_max": latest.wind_speed_max,
        "wind_from_direction": latest.wind_from_direction,
        "wind_from_direction_max": latest.wind_from_direction_max,
    }
    return payload


def _add_station_component(
    value: float | None,
    thresholds: tuple[tuple[float, int], ...],
    component_name: str,
    label: str,
    components: dict[str, int],
    drivers: list[str],
) -> int:
    if value is None:
        return 0
    for threshold, points in thresholds:
        if value >= threshold:
            components[component_name] = points
            drivers.append(label)
            return points
    return 0


def _add_gts_component(
    gts_data: dict[str, dict[str, Any]],
    theme: str,
    thresholds: tuple[tuple[float, int], ...],
    component_name: str,
    label: str,
    components: dict[str, int],
    drivers: list[str],
) -> int:
    value = ((gts_data.get(theme) or {}).get("value"))
    if not isinstance(value, (int, float)):
        return 0
    for threshold, points in thresholds:
        if float(value) >= threshold:
            components[component_name] = points
            drivers.append(label)
            return points
    return 0


def _risk_level(score: int) -> str:
    if score >= 75:
        return "very_high"
    if score >= 50:
        return "high"
    if score >= 25:
        return "medium"
    return "low"


def _count_recent_events(events: list[AvalancheEvent], years: int) -> int:
    cutoff = datetime.now(timezone.utc).date() - timedelta(days=365 * years)
    count = 0
    for event in events:
        event_date = _parse_date(event.event_date)
        if event_date and event_date >= cutoff:
            count += 1
    return count


def _latest_event_date(events: list[AvalancheEvent]) -> str | None:
    dated_events = [
        (parsed, event.event_date)
        for event in events
        if (parsed := _parse_date(event.event_date)) is not None and event.event_date is not None
    ]
    if not dated_events:
        return None
    dated_events.sort(key=lambda item: item[0])
    return dated_events[-1][1]


def _extract_property_map(raw_properties: Any) -> dict[str, Any]:
    if isinstance(raw_properties, dict):
        result: dict[str, Any] = {}
        for key, value in raw_properties.items():
            if isinstance(value, dict):
                name = str(value.get("navn") or key)
                result[name] = value.get("verdi")
            else:
                result[str(key)] = value
        return result

    if isinstance(raw_properties, list):
        result = {}
        for item in raw_properties:
            if not isinstance(item, dict):
                continue
            name = item.get("navn") or item.get("name") or item.get("id")
            if name is None:
                continue
            value = item.get("verdi")
            if value is None and "enumverdi" in item and isinstance(item["enumverdi"], dict):
                value = item["enumverdi"].get("navn") or item["enumverdi"].get("id")
            if value is None:
                value = item.get("verdiTekst") or item.get("tekst")
            result[str(name)] = value
        return result

    return {}


def _match_property(properties: dict[str, Any], target_name: str) -> str | None:
    target = target_name.casefold()
    for key, value in properties.items():
        if str(key).casefold() == target:
            return None if value is None else str(value)
    for key, value in properties.items():
        if target in str(key).casefold():
            return None if value is None else str(value)
    return None


def _is_snow_avalanche(value: str | None) -> bool:
    if not value:
        return False
    normalized = value.casefold()
    return "snø" in normalized or "sno" in normalized


def _resolve_event_date(item: dict[str, Any], properties: dict[str, Any]) -> str | None:
    for key in ("Dato", "Skreddato", "Hendelsesdato", "Registrert dato", "Tidspunkt"):
        value = _match_property(properties, key)
        if value:
            return value

    metadata = item.get("metadata") or {}
    return metadata.get("startdato") or metadata.get("sist_modifisert")


def _resolve_vegsystemreferanse(location: dict[str, Any]) -> str | None:
    refs = location.get("vegsystemreferanser") or []
    if not refs:
        return None
    first = refs[0] or {}
    return first.get("kortform")


def _build_vegsystemreferanse(road: str, segment: str) -> str:
    road_normalized = (
        road.strip()
        .upper()
        .replace(" ", "")
        .replace(".", "")
        .replace("-", "")
    )
    segment_normalized = (
        segment.strip()
        .upper()
        .replace(" ", "")
        .replace(".", "")
        .replace("-", "")
    )
    if not segment_normalized:
        return road_normalized
    return f"{road_normalized}{segment_normalized}"


def _latest_gts_value(payload: dict[str, Any]) -> float | None:
    nodata_value = payload.get("NoDataValue")
    for raw_value in reversed(payload.get("Data") or []):
        if raw_value is None or raw_value == nodata_value:
            continue
        try:
            return float(raw_value)
        except (TypeError, ValueError):
            continue
    return None


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    candidate = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(candidate).date()
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d.%m.%Y %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def _isoformat_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
