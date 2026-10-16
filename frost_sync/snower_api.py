from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

logger = logging.getLogger(__name__)

SNOWER_SNOW_DEPTH_ELEMENT = "snow_depth"
SNOWER_PROBE_TEMPERATURE_ELEMENT = "probe_temperature"
SNOWER_TO_LOGICAL_ELEMENT = {
    SNOWER_SNOW_DEPTH_ELEMENT: "snow_depth",
    SNOWER_PROBE_TEMPERATURE_ELEMENT: "air_temperature",
}
# NB: "snow_on_road" er IKKJE snødjupn lenger (sidan 1.10.2026: snø etter brøyting, cm, berre vegpar).

SNOWER_SOURCE_ID_PREFIX = "SNWR-"


class SnowerAuthError(RuntimeError):
    """401/410 – nøkkelen manglar, er ugyldig/rotert, eller koden prøver login."""


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
        api_key: str,
        domain_id: str = "frost-sync",
        timeout_seconds: int = 60,
        max_requests_per_minute: int = 55,
    ) -> None:
        if not api_key or not api_key.startswith("snk_"):
            raise SnowerAuthError("Snower API-nøkkel manglar eller startar ikkje med 'snk_'")
        self.base_url = base_url.rstrip("/")
        self.domain_id = (domain_id or "frost-sync").strip()
        self.timeout_seconds = timeout_seconds
        self._max_per_minute = max_requests_per_minute
        self._rate_minute = -1
        self._rate_count = 0
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": "frost-sync/1.0",
                "authentication-key": api_key,
                # Påkravd header, men verdien blir ikkje sjekka i 4.0
                "domain-id": self.domain_id,
            }
        )

    # ------------------------------------------------------------------ stasjonar
    def fetch_stations(self) -> list[SnowerMonitor]:
        """monitors_list direkte (ingen regions/areas i 4.0).

        Kostar 1 + 2N kall (active_status + location per monitor).
        Rate limiter held det under 60/min, men køyr dette i sync-metadata, ikkje kvar time.
        """
        items = self._post("/monitors_list", {"format": "extended"})
        monitors: list[SnowerMonitor] = []
        for item in items or []:
            if not isinstance(item, dict):
                continue
            permanent_id = str(item.get("permanent_id") or "").strip()
            if not permanent_id:
                continue
            name = str(item.get("name") or permanent_id)
            monitor_body = {"monitor": permanent_id}
            try:
                status = self._post("/monitor_active_status", monitor_body)
                if not bool(status.get("active_status")):
                    continue
                location = self._post("/monitor_location", monitor_body).get("location", {}) or {}
            except SnowerAuthError:
                raise
            except RuntimeError as exc:
                logger.warning("Hoppar over Snower-monitor %s (%s): %s", permanent_id, name, exc)
                continue
            context = {
                "domain_id": self.domain_id,
                "permanent_id": permanent_id,
                "name": name,
            }
            monitors.append(
                SnowerMonitor(
                    source_id=_monitor_source_id(permanent_id),
                    name=name,
                    longitude=_as_float(location.get("lng")),
                    latitude=_as_float(location.get("lat")),
                    provider_context=json.dumps(context, ensure_ascii=True),
                )
            )
        return monitors

    # ------------------------------------------------------------- siste verdiar
    def fetch_latest_observations(self, monitors: list[SnowerMonitor]) -> list[dict[str, Any]]:
        """Eitt last_reading-kall per monitor. Feil på éin monitor stoppar ikkje dei andre."""
        rows: list[dict[str, Any]] = []
        for monitor in monitors:
            permanent_id = _permanent_id_from_context(monitor.provider_context)
            if permanent_id is None:
                logger.warning(
                    "Snower-stasjon %s har gammal provider_context utan permanent_id – køyr sync-metadata",
                    monitor.source_id,
                )
                continue
            try:
                payload = self._post(
                    "/last_reading",
                    {"monitor": permanent_id, "readings_types": list(SNOWER_TO_LOGICAL_ELEMENT)},
                )
            except SnowerAuthError:
                raise
            except RuntimeError as exc:
                logger.warning("Snower last_reading feila for %s: %s", permanent_id, exc)
                continue
            rows.extend(self._normalize_last_reading_payload(monitor.source_id, payload or {}))
        return rows

    # ------------------------------------------------------------------ historikk
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

        permanent_id = _permanent_id_from_context(provider_context)
        if permanent_id is None:
            logger.warning("Gammal Snower provider_context utan permanent_id: %s", provider_context)
            return []
        source_id = _monitor_source_id(permanent_id)

        rows: list[dict[str, Any]] = []
        cursor = from_dt
        for _ in range(20):  # vern mot evig løkke
            response = self._post_response(
                "/readings_list",
                {
                    "monitor": permanent_id,
                    "start_date": _snower_datetime(cursor),
                    "end_date": _snower_datetime(to_dt),
                    "readings_types": requested_types,
                },
            )
            headers = response.headers
            if headers.get("X-Snower-History-Truncated") == "true":
                logger.info(
                    "Snower-historikk avkorta for %s (maks %s dagar), data frå %s",
                    permanent_id,
                    headers.get("X-Snower-History-Limit-Days"),
                    headers.get("X-Snower-Data-Available-From"),
                )
            rows.extend(self._normalize_readings_payload(source_id, response.json() or {}))

            if headers.get("X-Snower-Points-Truncated") != "true":
                break
            complete_until = headers.get("X-Snower-Data-Complete-Until")
            next_cursor = _parse_datetime(complete_until) if complete_until else None
            if next_cursor is None or next_cursor <= cursor:
                break
            cursor = next_cursor

        return _dedupe_rows(rows)

    # ---------------------------------------------------------------- normalisering
    def _normalize_last_reading_payload(self, source_id: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
        grouped_rows: dict[str, dict[str, Any]] = {}
        for source_key, logical_key in SNOWER_TO_LOGICAL_ELEMENT.items():
            reading = payload.get(source_key)
            if not isinstance(reading, dict):
                continue  # fråverande = ingen sensor/lisens/data, ikkje 0
            raw_time = reading.get("date_time")
            observation = _observation(logical_key, reading)
            if not raw_time or observation is None:
                continue
            reference_time = _normalize_reference_time(raw_time)
            row = grouped_rows.setdefault(
                reference_time,
                {"sourceId": source_id, "referenceTime": reference_time, "observations": []},
            )
            row["observations"].append(observation)
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
                observation = _observation(logical_key, reading)
                if observation is not None:
                    observations.append(observation)
            if observations:
                rows.append(
                    {
                        "sourceId": source_id,
                        "referenceTime": _normalize_reference_time(reference_time),
                        "observations": observations,
                    }
                )
        return rows

    # ------------------------------------------------------------------------ HTTP
    def _wait_for_rate_limit(self) -> None:
        now = time.time()
        minute = int(now // 60)
        if minute != self._rate_minute:
            self._rate_minute, self._rate_count = minute, 0
        if self._rate_count >= self._max_per_minute:
            sleep_s = 60 - (now % 60) + 0.5
            logger.info("Snower rate limit: ventar %.1f s", sleep_s)
            time.sleep(sleep_s)
            self._rate_minute, self._rate_count = int(time.time() // 60), 0
        self._rate_count += 1

    def _post_response(self, path: str, body: dict[str, Any]) -> requests.Response:
        response: requests.Response | None = None
        for attempt in range(2):
            self._wait_for_rate_limit()
            response = self.session.post(f"{self.base_url}{path}", json=body, timeout=self.timeout_seconds)
            if response.status_code == 429 and attempt == 0:
                sleep_s = 60 - (time.time() % 60) + 1
                logger.warning("Snower 429 på %s – ventar %.0f s", path, sleep_s)
                time.sleep(sleep_s)
                continue
            break
        assert response is not None
        if response.status_code in (401, 410):
            raise SnowerAuthError(f"Snower {response.status_code} for {path}: {_detail(response)}")
        if not response.ok:
            raise RuntimeError(f"Snower request failed for {path} ({response.status_code}): {_detail(response)}")
        return response

    def _post(self, path: str, body: dict[str, Any]) -> Any:
        return self._post_response(path, body).json()


# ------------------------------------------------------------------------ hjelparar
def parse_provider_context(provider_context: str | None) -> dict[str, str]:
    if not provider_context:
        raise RuntimeError("Missing provider_context for Snower station")
    parsed = json.loads(provider_context)
    if not isinstance(parsed, dict):
        raise RuntimeError("Invalid provider_context for Snower station")
    return {key: str(value) for key, value in parsed.items()}


def _permanent_id_from_context(provider_context: str | None) -> str | None:
    try:
        return parse_provider_context(provider_context).get("permanent_id") or None
    except (RuntimeError, ValueError):
        return None


def _monitor_source_id(permanent_id: str) -> str:
    # permanent_id er stabil – ingen grunn til å hashe
    return f"{SNOWER_SOURCE_ID_PREFIX}{permanent_id}"


def _observation(logical_key: str, reading: dict[str, Any]) -> dict[str, Any] | None:
    # snow_depth.value er avrunda; non_approximate_value har full presisjon
    value = _as_float(reading.get("non_approximate_value"))
    if value is None:
        value = _as_float(reading.get("value"))
    if value is None:
        return None
    return {
        "elementId": logical_key,
        "value": value,
        "unit": _normalize_unit(reading.get("unit")),
        "qualityCode": None,
    }


def _dedupe_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_time: dict[str, dict[str, Any]] = {}
    for row in rows:
        by_time[row["referenceTime"]] = row
    return [by_time[key] for key in sorted(by_time)]


def _normalize_reference_time(value: str) -> str:
    return _parse_datetime(value).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_datetime(value: str) -> datetime:
    normalized = value.strip().replace(" ", "T")
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:  # readings_list-nøklar er UTC utan sone
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _snower_datetime(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f%z")


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


def _detail(response: requests.Response) -> str:
    try:
        return str(response.json().get("detail", response.text))
    except ValueError:
        return response.text[:300]
