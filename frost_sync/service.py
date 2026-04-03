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


ELEMENT_FIELD_MAP = {
    "air_temperature": ("air_temperature", "air_temperature_unit"),
    "sum(precipitation_amount PT1H)": ("precipitation_1h", "precipitation_1h_unit"),
    "snow_depth": ("snow_depth", "snow_depth_unit"),
    "wind_from_direction": ("wind_from_direction", "wind_from_direction_unit"),
    "wind_speed": ("wind_speed", "wind_speed_unit"),
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
                if source.source_id in source_ids
            }
            capabilities_updated += self._upsert_capabilities(station, available_elements)

        self.session.commit()

        observable_source_ids = [
            source.source_id
            for source in sources
            if any(source.source_id in source_ids for source_ids in capability_source_ids.values())
        ]

        for source_id_batch in _chunked(observable_source_ids, source_batch_size):
            batch_written, batch_latest, batch_errors = self._sync_observation_batch(
                stations_by_source=stations_by_source,
                source_id_batch=source_id_batch,
            )
            observations_written += batch_written
            latest_updated += batch_latest
            station_errors += batch_errors

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

        for row in observation_rows:
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

                existing.value = item.get("value")
                existing.unit = item.get("unit")
                existing.time_offset = item.get("timeOffset")
                existing.level = _format_level(item.get("level"))
                existing.quality_code = _extract_quality_code(item.get("qualityCode"))
                existing.fetched_at = now

                if _apply_latest_observation(latest, reference_time, item):
                    has_latest_change = True

            if has_latest_change:
                self._refresh_precipitation_24h(latest, station.id)
                latest.updated_at = now
                station.last_observation_time = latest.observed_at
                latest_updates += 1

        return written, latest_updates

    def _refresh_precipitation_24h(self, latest: StationLatest, station_id: int) -> None:
        observed_at = _ensure_utc(latest.observed_at)
        if observed_at is None:
            latest.precipitation_24h = None
            latest.precipitation_24h_unit = None
            return

        window_start = observed_at - timedelta(hours=24)
        total = self.session.execute(
            select(func.sum(Observation.value)).where(
                Observation.station_id == station_id,
                Observation.element_id == "sum(precipitation_amount PT1H)",
                Observation.reference_time > window_start,
                Observation.reference_time <= observed_at,
            )
        ).scalar_one()

        latest.precipitation_24h = float(total) if total is not None else None
        latest.precipitation_24h_unit = latest.precipitation_1h_unit if total is not None else None

    def _prune_old_observations(self, retention_days: int) -> int:
        if retention_days <= 0:
            return 0

        cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
        result = self.session.execute(
            delete(Observation).where(Observation.reference_time < cutoff)
        )
        return result.rowcount or 0


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
