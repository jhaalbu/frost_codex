from __future__ import annotations

from datetime import date, datetime, time, timezone
from typing import Any

from flask import Blueprint, Flask, abort, jsonify, request
from sqlalchemy import and_, select

from frost_sync.config import load_settings
from frost_sync.db import create_session_factory
from frost_sync.models import Observation, Station, StationCapability, StationLatest


CAPABILITY_FLAG_MAP = {
    "air_temperature": "has_air_temperature",
    "sum(precipitation_amount PT1H)": "has_precipitation_1h",
    "precipitation_1h": "has_precipitation_1h",
    "snow_depth": "has_snow_depth",
    "surface_snow_thickness": "has_snow_depth",
    "wind_from_direction": "has_wind_from_direction",
    "wind_speed": "has_wind_speed",
    "discharge": "has_discharge",
    "groundwater_level": "has_groundwater_level",
}

def create_app() -> Flask:
    app = Flask(__name__)
    app.register_blueprint(create_blueprint())
    return app


def create_blueprint(name: str = "frost_sync") -> Blueprint:
    settings = load_settings()
    session_factory = create_session_factory(settings.database_url)
    blueprint = Blueprint(name, __name__)

    @blueprint.get("/health")
    def health() -> Any:
        return {"status": "ok"}

    @blueprint.get("/api/stations/latest.geojson")
    def latest_geojson() -> Any:
        has_filter = request.args.get("has")

        with session_factory() as session:
            rows = (
                session.execute(
                    select(Station, StationLatest)
                    .join(StationLatest, StationLatest.station_id == Station.id)
                    .order_by(Station.source_id)
                )
                .all()
            )
            capabilities = _load_capabilities(session)

        features = []
        for station, latest in rows:
            if station.longitude is None or station.latitude is None:
                continue

            capability_flags = capabilities.get(station.id, {})
            if has_filter and not _matches_has_filter(has_filter, capability_flags, latest):
                continue

            features.append(
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [station.longitude, station.latitude],
                    },
                    "properties": {
                        **_station_properties(station),
                        **capability_flags,
                        **_parameter_profile_properties(capability_flags),
                        **_latest_properties(latest),
                    },
                }
            )

        return jsonify({"type": "FeatureCollection", "features": features})

    @blueprint.get("/api/stations/history.geojson")
    def history_geojson() -> Any:
        from_dt, to_dt = _resolve_time_range()

        with session_factory() as session:
            rows = (
                session.execute(
                    select(Observation, Station)
                    .join(Station, Station.id == Observation.station_id)
                    .where(
                        and_(
                            Observation.reference_time >= from_dt,
                            Observation.reference_time <= to_dt,
                        )
                    )
                    .order_by(Observation.reference_time, Station.source_id, Observation.element_id)
                )
                .all()
            )
            capabilities = _load_capabilities(session)

        features = []
        for observation, station in rows:
            if station.longitude is None or station.latitude is None:
                continue

            features.append(
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [station.longitude, station.latitude],
                    },
                    "properties": {
                        **_station_properties(station),
                        **capabilities.get(station.id, {}),
                        "reference_time": _isoformat(observation.reference_time),
                        "element_id": observation.element_id,
                        "value": observation.value,
                        "unit": observation.unit,
                        "time_offset": observation.time_offset,
                        "level": observation.level,
                        "quality_code": observation.quality_code,
                    },
                }
            )

        return jsonify({"type": "FeatureCollection", "features": features})

    @blueprint.get("/api/stations/<source_id>")
    def station_detail(source_id: str) -> Any:
        with session_factory() as session:
            row = (
                session.execute(
                    select(Station, StationLatest)
                    .outerjoin(StationLatest, StationLatest.station_id == Station.id)
                    .where(Station.source_id == source_id)
                )
                .one_or_none()
            )
            if row is None:
                abort(404)

            station, latest = row
            capability_rows = (
                session.execute(
                    select(StationCapability).where(StationCapability.station_id == station.id)
                )
                .scalars()
                .all()
            )

        capabilities = {
            capability.element_id: capability.available
            for capability in capability_rows
        }
        payload = {
            **_station_properties(station),
            "capabilities": capabilities,
            **_parameter_profile_properties(_capability_flags_from_capabilities(capabilities)),
            "latest": _latest_properties(latest) if latest else None,
        }
        return jsonify(payload)

    @blueprint.get("/api/stations/<source_id>/observations")
    def station_observations(source_id: str) -> Any:
        from_dt, to_dt = _resolve_time_range()

        with session_factory() as session:
            station = (
                session.execute(select(Station).where(Station.source_id == source_id))
                .scalar_one_or_none()
            )
            if station is None:
                abort(404)

            observations = (
                session.execute(
                    select(Observation)
                    .where(
                        and_(
                            Observation.station_id == station.id,
                            Observation.reference_time >= from_dt,
                            Observation.reference_time <= to_dt,
                        )
                    )
                    .order_by(Observation.reference_time, Observation.element_id)
                )
                .scalars()
                .all()
            )

        return jsonify(
            {
                "station": _station_properties(station),
                "from": _isoformat(from_dt),
                "to": _isoformat(to_dt),
                "observations": [
                    {
                        "reference_time": _isoformat(observation.reference_time),
                        "element_id": observation.element_id,
                        "value": observation.value,
                        "unit": observation.unit,
                        "time_offset": observation.time_offset,
                        "level": observation.level,
                        "quality_code": observation.quality_code,
                    }
                    for observation in observations
                ],
            }
        )

    return blueprint


def _resolve_time_range() -> tuple[datetime, datetime]:
    date_arg = request.args.get("date")
    from_arg = request.args.get("from")
    to_arg = request.args.get("to")

    if date_arg:
        selected_date = date.fromisoformat(date_arg)
        return (
            datetime.combine(selected_date, time.min, tzinfo=timezone.utc),
            datetime.combine(selected_date, time.max, tzinfo=timezone.utc),
        )

    if from_arg and to_arg:
        return _parse_timestamp(from_arg), _parse_timestamp(to_arg)

    abort(400, description="Use either ?date=YYYY-MM-DD or both ?from=...&to=...")


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)

def _load_capabilities(session) -> dict[int, dict[str, bool]]:
    rows = session.execute(select(StationCapability)).scalars().all()
    capabilities: dict[int, dict[str, bool]] = {}
    for row in rows:
        flags = capabilities.setdefault(row.station_id, _empty_capability_flags())
        flag_name = CAPABILITY_FLAG_MAP.get(row.element_id)
        if flag_name:
            flags[flag_name] = row.available
    return capabilities


def _capability_flags_from_capabilities(capabilities: dict[str, bool]) -> dict[str, bool]:
    flags = _empty_capability_flags()
    for element_id, available in capabilities.items():
        flag_name = CAPABILITY_FLAG_MAP.get(element_id)
        if flag_name:
            flags[flag_name] = bool(available)
    return flags

def _empty_capability_flags() -> dict[str, bool]:
    return {flag: False for flag in CAPABILITY_FLAG_MAP.values()}


def _capability_flag_name(element_id: str) -> str:
    if element_id in CAPABILITY_FLAG_MAP:
        return CAPABILITY_FLAG_MAP[element_id]

    alias_map = {
        "precipitation_1h": "has_precipitation_1h",
        "air_temperature": "has_air_temperature",
        "snow_depth": "has_snow_depth",
        "wind_from_direction": "has_wind_from_direction",
        "wind_speed": "has_wind_speed",
        "discharge": "has_discharge",
        "groundwater_level": "has_groundwater_level",
    }
    return alias_map.get(element_id, "")


def _matches_has_filter(has_filter: str, capability_flags: dict[str, bool], latest: StationLatest) -> bool:
    flag_name = _capability_flag_name(has_filter)
    if not flag_name:
        return False

    if flag_name == "has_snow_depth":
        return capability_flags.get(flag_name, False) and latest.snow_depth is not None

    return capability_flags.get(flag_name, False)


def _parameter_profile_properties(capability_flags: dict[str, bool]) -> dict[str, Any]:
    count = sum(1 for value in capability_flags.values() if value)
    return {
        "available_parameter_count": count,
        "parameter_profile": _parameter_profile_name(capability_flags),
    }


def _parameter_profile_name(capability_flags: dict[str, bool]) -> str:
    weather_groups = {
        "temperature": capability_flags.get("has_air_temperature", False),
        "precipitation": capability_flags.get("has_precipitation_1h", False),
        "wind": capability_flags.get("has_wind_speed", False) or capability_flags.get("has_wind_from_direction", False),
        "snow": capability_flags.get("has_snow_depth", False),
    }
    hydrology_groups = {
        "discharge": capability_flags.get("has_discharge", False),
        "groundwater": capability_flags.get("has_groundwater_level", False),
    }

    weather_present = [name for name, present in weather_groups.items() if present]
    hydrology_present = [name for name, present in hydrology_groups.items() if present]

    if len(weather_present) == 4 and not hydrology_present:
        return "complete"
    if len(weather_present) == 4 and hydrology_present:
        return "complete_plus_hydrology"
    if set(weather_present) == {"temperature", "precipitation", "wind"}:
        return "weather"
    if set(weather_present) == {"temperature", "wind", "snow"}:
        return "snow_weather"
    if set(hydrology_present) == {"discharge", "groundwater"} and not weather_present:
        return "hydrology_complete"
    if len(weather_present) == 1 and not hydrology_present:
        return weather_present[0]
    if len(hydrology_present) == 1 and not weather_present:
        return hydrology_present[0]
    if not weather_present and not hydrology_present:
        return "metadata_only"
    if weather_present or hydrology_present:
        return "_".join(weather_present + hydrology_present)
    return "other"


def _station_properties(station: Station) -> dict[str, Any]:
    recent_status = _recent_status(station.last_observation_time)
    return {
        "source_id": station.source_id,
        "provider": station.provider,
        "name": station.name,
        "stationholder": station.stationholder,
        "country": station.country,
        "county": station.county,
        "municipality": station.municipality,
        "masl": station.masl,
        "longitude": station.longitude,
        "latitude": station.latitude,
        "valid_from": _isoformat(station.valid_from),
        "valid_to": _isoformat(station.valid_to),
        "last_seen_at": _isoformat(station.last_seen_at),
        "last_observation_time": _isoformat(station.last_observation_time),
        "has_recent_data": recent_status["has_recent_data"],
        "minutes_since_observation": recent_status["minutes_since_observation"],
    }


def _latest_properties(latest: StationLatest) -> dict[str, Any]:
    return {
        "observed_at": _isoformat(latest.observed_at),
        "air_temperature": latest.air_temperature,
        "air_temperature_unit": latest.air_temperature_unit,
        "air_temperature_min": latest.air_temperature_min,
        "air_temperature_min_unit": latest.air_temperature_min_unit,
        "air_temperature_max": latest.air_temperature_max,
        "air_temperature_max_unit": latest.air_temperature_max_unit,
        "air_temperature_max_time": latest.air_temperature_max_time,
        "precipitation_1h": latest.precipitation_1h,
        "precipitation_1h_unit": latest.precipitation_1h_unit,
        "is_precipitation_suspect": latest.is_precipitation_suspect,
        "precipitation_1h_max": latest.precipitation_1h_max,
        "precipitation_1h_max_unit": latest.precipitation_1h_max_unit,
        "precipitation_1h_max_period": latest.precipitation_1h_max_period,
        "precipitation_3h": latest.precipitation_3h,
        "precipitation_3h_unit": latest.precipitation_3h_unit,
        "precipitation_3h_max": latest.precipitation_3h_max,
        "precipitation_3h_max_unit": latest.precipitation_3h_max_unit,
        "precipitation_3h_max_period": latest.precipitation_3h_max_period,
        "precipitation_24h": latest.precipitation_24h,
        "precipitation_24h_unit": latest.precipitation_24h_unit,
        "snow_depth": latest.snow_depth,
        "snow_depth_unit": latest.snow_depth_unit,
        "snow_depth_change": latest.snow_depth_change,
        "snow_depth_change_unit": latest.snow_depth_change_unit,
        "wind_from_direction": latest.wind_from_direction,
        "wind_from_direction_unit": latest.wind_from_direction_unit,
        "wind_from_direction_max": latest.wind_from_direction_max,
        "wind_from_direction_max_unit": latest.wind_from_direction_max_unit,
        "wind_speed": latest.wind_speed,
        "wind_speed_unit": latest.wind_speed_unit,
        "wind_speed_max": latest.wind_speed_max,
        "wind_speed_max_unit": latest.wind_speed_max_unit,
        "wind_speed_max_time": latest.wind_speed_max_time,
        "discharge": latest.discharge,
        "discharge_unit": latest.discharge_unit,
        "groundwater_level": latest.groundwater_level,
        "groundwater_level_unit": latest.groundwater_level_unit,
        "updated_at": _isoformat(latest.updated_at),
    }


def _isoformat(value: datetime | None) -> str | None:
    if value is None:
        return None
    value = _ensure_utc(value)
    return value.isoformat().replace("+00:00", "Z") if value else None


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _recent_status(last_observation_time: datetime | None, recent_minutes: int = 120) -> dict[str, Any]:
    observed_at = _ensure_utc(last_observation_time)
    if observed_at is None:
        return {
            "has_recent_data": False,
            "minutes_since_observation": None,
        }

    minutes_since = int((datetime.now(timezone.utc) - observed_at).total_seconds() // 60)
    return {
        "has_recent_data": minutes_since <= recent_minutes,
        "minutes_since_observation": minutes_since,
    }
