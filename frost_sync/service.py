from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable

from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from frost_sync.frost_api import FrostClient, FrostSource, TARGET_ELEMENTS
from frost_sync.models import Observation, Station, StationCapability, StationLatest


logger = logging.getLogger(__name__)

PRECIPITATION_ELEMENT = "sum(precipitation_amount PT1H)"
AIR_TEMPERATURE_ELEMENT = "air_temperature"
SNOW_DEPTH_ELEMENT = "snow_depth"
SURFACE_SNOW_THICKNESS_ELEMENT = "surface_snow_thickness"
SNOW_DEPTH_ELEMENT_IDS = {SNOW_DEPTH_ELEMENT, SURFACE_SNOW_THICKNESS_ELEMENT}
WIND_SPEED_ELEMENT = "wind_speed"
WIND_DIRECTION_ELEMENT = "wind_from_direction"


ELEMENT_FIELD_MAP = {
    AIR_TEMPERATURE_ELEMENT: ("air_temperature", "air_temperature_unit"),
    PRECIPITATION_ELEMENT: ("precipitation_1h", "precipitation_1h_unit"),
    SNOW_DEPTH_ELEMENT: ("snow_depth", "snow_depth_unit"),
    SURFACE_SNOW_THICKNESS_ELEMENT: ("snow_depth", "snow_depth_unit"),
    WIND_DIRECTION_ELEMENT: ("wind_from_direction", "wind_from_direction_unit"),
    WIND_SPEED_ELEMENT: ("wind_speed", "wind_speed_unit"),
}


@dataclass(frozen=True)
class SyncSummary:
    stations_seen: int
    capabilities_updated: int
    observations_written: int
    latest_updated: int
    observations_deleted: int
    station_errors: int


class SyncService:
    def __init__(self, session: Session, frost_client: FrostClient) -> None:
        self.session = session
        self.frost_client = frost_client

    def run_hourly_sync(
        self,
        page_limit: int,
        source_batch_size: int = 100,
        retention_days: int = 14,
    ) -> SyncSummary:
        sources = self.frost_client.fetch_sources(page_limit=page_limit)
        stations_by_source = self._upsert_stations(sources)
        capability_source_ids = {
            element_id: self.frost_client.fetch_capability_source_ids(element_id)
            for element_id in TARGET_ELEMENTS
        }

        capabilities_updated = 0
        observations_written = 0
        latest_updated = 0
        station_errors = 0

        for source in sources:
            station = stations_by_source[source.source_id]
            available_elements = {
                element_id
                for element_id, source_ids in capability_source_ids.items()
                if source.source_id in source_ids and element_id not in SNOW_DEPTH_ELEMENT_IDS
            }
            capabilities_updated += self._upsert_capabilities(station, available_elements)

        self.session.commit()

        observable_source_ids = [
            source.source_id
            for source in sources
            if any(source.source_id in source_ids for source_ids in capability_source_ids.values())
        ]

        snow_capable_source_ids: set[str] = set()

        for source_id_batch in _chunked(observable_source_ids, source_batch_size):
            batch_written, batch_latest, batch_errors = self._sync_observation_batch(
                stations_by_source=stations_by_source,
                source_id_batch=source_id_batch,
            )
            observations_written += batch_written
            latest_updated += batch_latest
            station_errors += batch_errors

        snow_written, snow_latest, snow_errors = self._sync_snow_observations(
            stations_by_source=stations_by_source,
            source_ids=observable_source_ids,
            source_batch_size=source_batch_size,
            lookback_days=retention_days,
            snow_capable_source_ids=snow_capable_source_ids,
        )
        observations_written += snow_written
        latest_updated += snow_latest
        station_errors += snow_errors

        for source in sources:
            if source.source_id not in snow_capable_source_ids:
                continue
            station = stations_by_source[source.source_id]
            capabilities_updated += self._upsert_capabilities(
                station,
                {SURFACE_SNOW_THICKNESS_ELEMENT},
            )

        observations_deleted = self._prune_old_observations(retention_days)
        self.session.commit()

        return SyncSummary(
            stations_seen=len(sources),
            capabilities_updated=capabilities_updated,
            observations_written=observations_written,
            latest_updated=latest_updated,
            observations_deleted=observations_deleted,
            station_errors=station_errors,
        )

    def _sync_snow_observations(
        self,
        stations_by_source: dict[str, Station],
        source_ids: list[str],
        source_batch_size: int,
        lookback_days: int,
        snow_capable_source_ids: set[str],
    ) -> tuple[int, int, int]:
        written = 0
        latest = 0
        errors = 0

        for source_batch in _chunked(source_ids, source_batch_size):
            try:
                snow_series_ids = self.frost_client.fetch_snow_series_ids(source_batch, lookback_days=lookback_days)
                if not snow_series_ids:
                    continue
                snow_capable_source_ids.update(_normalize_source_id(series_id) for series_id in snow_series_ids)
                snow_rows = self.frost_client.fetch_recent_snow_observations(snow_series_ids, lookback_days=lookback_days)
                batch_written, batch_latest = self._store_observations_batch(stations_by_source, snow_rows)
                self.session.commit()
                written += batch_written
                latest += batch_latest
            except Exception as exc:
                self.session.rollback()
                logger.warning(
                    "Snow batch failed for %s sources. First source=%s. Error=%s",
                    len(source_batch),
                    source_batch[0] if source_batch else None,
                    exc,
                )
                errors += len(source_batch)

        return written, latest, errors

    def _sync_observation_batch(
        self,
        stations_by_source: dict[str, Station],
        source_id_batch: list[str],
    ) -> tuple[int, int, int]:
        try:
            observation_rows = self.frost_client.fetch_latest_observations(source_id_batch)
            batch_written, batch_latest = self._store_observations_batch(stations_by_source, observation_rows)
            self.session.commit()
            return batch_written, batch_latest, 0
        except Exception as exc:
            self.session.rollback()
            if len(source_id_batch) == 1:
                logger.exception(
                    "Failed to sync observations for source %s: %s",
                    source_id_batch[0],
                    exc,
                )
                return 0, 0, 1

            midpoint = len(source_id_batch) // 2
            logger.warning(
                "Observation batch failed for %s sources; retrying as smaller batches. First source=%s. Error=%s",
                len(source_id_batch),
                source_id_batch[0],
                exc,
            )
            left_written, left_latest, left_errors = self._sync_observation_batch(
                stations_by_source=stations_by_source,
                source_id_batch=source_id_batch[:midpoint],
            )
            right_written, right_latest, right_errors = self._sync_observation_batch(
                stations_by_source=stations_by_source,
                source_id_batch=source_id_batch[midpoint:],
            )
            return (
                left_written + right_written,
                left_latest + right_latest,
                left_errors + right_errors,
            )

    def _upsert_stations(self, sources: Iterable[FrostSource]) -> dict[str, Station]:
        now = datetime.now(timezone.utc)
        source_ids = [source.source_id for source in sources]
        existing = self.session.execute(
            select(Station).where(Station.source_id.in_(source_ids))
        ).scalars().all()
        stations = {station.source_id: station for station in existing}

        for source in sources:
            station = stations.get(source.source_id)
            if station is None:
                station = Station(source_id=source.source_id, last_seen_at=now)
                self.session.add(station)
                stations[source.source_id] = station

            station.name = source.name
            station.country = source.country
            station.county = source.county
            station.municipality = source.municipality
            station.masl = source.masl
            station.longitude = source.longitude
            station.latitude = source.latitude
            station.valid_from = source.valid_from
            station.valid_to = source.valid_to
            station.last_seen_at = now

        self.session.flush()
        return stations

    def _upsert_capabilities(self, station: Station, available_elements: set[str]) -> int:
        now = datetime.now(timezone.utc)
        existing = {
            capability.element_id: capability
            for capability in self.session.execute(
                select(StationCapability).where(StationCapability.station_id == station.id)
            ).scalars()
        }

        updated = 0
        for element_id in TARGET_ELEMENTS:
            available = element_id in available_elements
            capability = existing.get(element_id)
            if capability is None:
                capability = StationCapability(
                    station_id=station.id,
                    element_id=element_id,
                    available=available,
                    first_seen_at=now,
                    last_seen_at=now,
                )
                self.session.add(capability)
                updated += 1
                continue

            if capability.available != available:
                capability.available = available
                updated += 1

            capability.last_seen_at = now

        return updated

    def _store_observations_batch(self, stations_by_source: dict[str, Station], observation_rows: list[dict]) -> tuple[int, int]:
        now = datetime.now(timezone.utc)
        written = 0
        latest_updates = 0

        sorted_rows = sorted(
            observation_rows,
            key=lambda row: _ensure_utc(_parse_reference_time(row.get("referenceTime")))
            or datetime.min.replace(tzinfo=timezone.utc),
        )

        for row in sorted_rows:
            source_id = _normalize_source_id(row.get("sourceId"))
            station = stations_by_source.get(source_id)
            if station is None:
                continue

            reference_time = _ensure_utc(_parse_reference_time(row.get("referenceTime")))
            if reference_time is None:
                continue

            latest = station.latest
            if latest is None:
                latest = StationLatest(
                    station_id=station.id,
                    source_id=station.source_id,
                    updated_at=now,
                )
                self.session.add(latest)
                station.latest = latest

            has_latest_change = False

            for item in row.get("observations", []):
                element_id = item.get("elementId")
                if not element_id:
                    continue

                quality_code = _extract_quality_code(item.get("qualityCode"))
                if quality_code is not None and quality_code >= 5:
                    continue

                normalized_value = _normalize_observation_value(element_id, item.get("value"))
                if normalized_value is None and element_id in SNOW_DEPTH_ELEMENT_IDS:
                    continue

                existing = self.session.execute(
                    select(Observation).where(
                        Observation.station_id == station.id,
                        Observation.reference_time == reference_time,
                        Observation.element_id == element_id,
                    )
                ).scalar_one_or_none()

                if existing is None:
                    existing = Observation(
                        station_id=station.id,
                        reference_time=reference_time,
                        element_id=element_id,
                        fetched_at=now,
                    )
                    self.session.add(existing)
                    written += 1

                existing.value = normalized_value
                existing.unit = item.get("unit")
                existing.time_offset = item.get("timeOffset")
                existing.level = _format_level(item.get("level"))
                existing.quality_code = quality_code
                existing.fetched_at = now

                normalized_item = dict(item)
                normalized_item["value"] = normalized_value
                normalized_item["qualityCode"] = quality_code

                if _apply_latest_observation(latest, reference_time, normalized_item):
                    has_latest_change = True

            if has_latest_change:
                self._refresh_latest_window_metrics(latest, station.id)
                latest.updated_at = now
                station.last_observation_time = latest.observed_at
                latest_updates += 1

        return written, latest_updates

    def _refresh_latest_window_metrics(self, latest: StationLatest, station_id: int) -> None:
        observed_at = _ensure_utc(latest.observed_at)
        if observed_at is None:
            _reset_window_metrics(latest)
            return

        window_start = observed_at - timedelta(hours=24)
        rows = (
            self.session.execute(
                select(Observation).where(
                    Observation.station_id == station_id,
                    Observation.reference_time > window_start,
                    Observation.reference_time <= observed_at,
                    Observation.element_id.in_(
                        [
                            PRECIPITATION_ELEMENT,
                            AIR_TEMPERATURE_ELEMENT,
                            *sorted(SNOW_DEPTH_ELEMENT_IDS),
                            WIND_SPEED_ELEMENT,
                            WIND_DIRECTION_ELEMENT,
                        ]
                    ),
                )
            )
            .scalars()
            .all()
        )

        precipitation_rows = sorted(
            [row for row in rows if row.element_id == PRECIPITATION_ELEMENT and row.value is not None],
            key=lambda row: _ensure_utc(row.reference_time) or datetime.min.replace(tzinfo=timezone.utc),
        )
        temperature_rows = [row for row in rows if row.element_id == AIR_TEMPERATURE_ELEMENT and row.value is not None]
        snow_depth_rows = sorted(
            [row for row in rows if row.element_id in SNOW_DEPTH_ELEMENT_IDS and row.value is not None],
            key=lambda row: _ensure_utc(row.reference_time) or datetime.min.replace(tzinfo=timezone.utc),
        )
        wind_speed_rows = [row for row in rows if row.element_id == WIND_SPEED_ELEMENT and row.value is not None]
        wind_direction_rows = {
            _ensure_utc(row.reference_time): row
            for row in rows
            if row.element_id == WIND_DIRECTION_ELEMENT and row.value is not None
        }

        precipitation_total = self.session.execute(
            select(func.sum(Observation.value)).where(
                Observation.station_id == station_id,
                Observation.element_id == PRECIPITATION_ELEMENT,
                Observation.reference_time > window_start,
                Observation.reference_time <= observed_at,
            )
        ).scalar_one()

        latest.precipitation_24h = float(precipitation_total) if precipitation_total is not None else None
        latest.precipitation_24h_unit = _first_unit(precipitation_rows)
        latest.precipitation_1h_max = _max_value(precipitation_rows)
        latest.precipitation_1h_max_unit = _first_unit(precipitation_rows)
        latest.precipitation_3h = _rolling_sum_for_window(precipitation_rows, observed_at, hours=3)
        latest.precipitation_3h_unit = _first_unit(precipitation_rows) if latest.precipitation_3h is not None else None
        latest.precipitation_3h_max = _max_rolling_sum(precipitation_rows, hours=3)
        latest.precipitation_3h_max_unit = _first_unit(precipitation_rows) if latest.precipitation_3h_max is not None else None

        latest.air_temperature_min = _min_value(temperature_rows)
        latest.air_temperature_min_unit = _first_unit(temperature_rows)
        latest.air_temperature_max = _max_value(temperature_rows)
        latest.air_temperature_max_unit = _first_unit(temperature_rows)

        latest.snow_depth_change = _snow_depth_change(snow_depth_rows)
        latest.snow_depth_change_unit = _first_unit(snow_depth_rows) if latest.snow_depth_change is not None else None

        wind_speed_max_row = _max_row(wind_speed_rows)
        if wind_speed_max_row is None:
            latest.wind_speed_max = None
            latest.wind_speed_max_unit = None
            latest.wind_from_direction_max = None
            latest.wind_from_direction_max_unit = None
        else:
            latest.wind_speed_max = wind_speed_max_row.value
            latest.wind_speed_max_unit = wind_speed_max_row.unit
            direction_row = wind_direction_rows.get(_ensure_utc(wind_speed_max_row.reference_time))
            latest.wind_from_direction_max = direction_row.value if direction_row else None
            latest.wind_from_direction_max_unit = direction_row.unit if direction_row else None

    def _prune_old_observations(self, retention_days: int) -> int:
        if retention_days <= 0:
            deleted_bad_quality = self.session.execute(
                delete(Observation).where(Observation.quality_code >= 5)
            )
            return deleted_bad_quality.rowcount or 0

        cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
        result_old = self.session.execute(
            delete(Observation).where(Observation.reference_time < cutoff)
        )
        result_bad_quality = self.session.execute(
            delete(Observation).where(Observation.quality_code >= 5)
        )
        return (result_old.rowcount or 0) + (result_bad_quality.rowcount or 0)


def _apply_latest_observation(latest: StationLatest, reference_time: datetime, item: dict) -> bool:
    element_id = item.get("elementId")
    mapped_fields = ELEMENT_FIELD_MAP.get(element_id)
    if mapped_fields is None:
        return False

    value_field, unit_field = mapped_fields
    current_time = _ensure_utc(latest.observed_at)
    should_update = current_time is None or reference_time >= current_time

    if not should_update:
        return False

    setattr(latest, value_field, item.get("value"))
    setattr(latest, unit_field, item.get("unit"))
    latest.observed_at = reference_time
    return True


def _chunked(items: list[str], chunk_size: int) -> Iterable[list[str]]:
    for index in range(0, len(items), chunk_size):
        yield items[index:index + chunk_size]


def _normalize_source_id(value: str | None) -> str:
    if not value:
        return ""
    return value.split(":", 1)[0]


def _parse_reference_time(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _format_level(level: dict | None) -> str | None:
    if not level:
        return None
    return ",".join(f"{key}={value}" for key, value in sorted(level.items()))


def _extract_quality_code(value: int | str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _reset_window_metrics(latest: StationLatest) -> None:
    latest.precipitation_1h_max = None
    latest.precipitation_1h_max_unit = None
    latest.precipitation_3h = None
    latest.precipitation_3h_unit = None
    latest.precipitation_3h_max = None
    latest.precipitation_3h_max_unit = None
    latest.precipitation_24h = None
    latest.precipitation_24h_unit = None
    latest.air_temperature_min = None
    latest.air_temperature_min_unit = None
    latest.air_temperature_max = None
    latest.air_temperature_max_unit = None
    latest.snow_depth_change = None
    latest.snow_depth_change_unit = None
    latest.wind_speed_max = None
    latest.wind_speed_max_unit = None
    latest.wind_from_direction_max = None
    latest.wind_from_direction_max_unit = None


def _first_unit(rows: list[Observation]) -> str | None:
    for row in rows:
        if row.unit:
            return row.unit
    return None


def _max_value(rows: list[Observation]) -> float | None:
    values = [row.value for row in rows if row.value is not None]
    return max(values) if values else None


def _min_value(rows: list[Observation]) -> float | None:
    values = [row.value for row in rows if row.value is not None]
    return min(values) if values else None


def _max_row(rows: list[Observation]) -> Observation | None:
    filtered = [row for row in rows if row.value is not None]
    if not filtered:
        return None
    return max(
        filtered,
        key=lambda row: (
            row.value,
            _ensure_utc(row.reference_time) or datetime.min.replace(tzinfo=timezone.utc),
        ),
    )


def _rolling_sum_for_window(rows: list[Observation], window_end: datetime, hours: int) -> float | None:
    window_start = window_end - timedelta(hours=hours)
    values = [
        row.value
        for row in rows
        if row.value is not None
        and (_ensure_utc(row.reference_time) or window_end) > window_start
        and (_ensure_utc(row.reference_time) or window_end) <= window_end
    ]
    return float(sum(values)) if values else None


def _max_rolling_sum(rows: list[Observation], hours: int) -> float | None:
    if not rows:
        return None
    max_sum: float | None = None
    for row in rows:
        row_time = _ensure_utc(row.reference_time)
        if row_time is None:
            continue
        rolling_sum = _rolling_sum_for_window(rows, row_time, hours)
        if rolling_sum is None:
            continue
        if max_sum is None or rolling_sum > max_sum:
            max_sum = rolling_sum
    return max_sum


def _snow_depth_change(rows: list[Observation]) -> float | None:
    if len(rows) < 2:
        return None
    first_row = rows[0]
    last_row = rows[-1]
    if first_row.value is None or last_row.value is None:
        return None
    return last_row.value - first_row.value


def _normalize_observation_value(element_id: str, value: float | int | None) -> float | None:
    if value is None:
        return None
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return None

    if element_id in SNOW_DEPTH_ELEMENT_IDS:
        if numeric_value == -1:
            return 0.0
        if numeric_value <= -3:
            return None

    return numeric_value
