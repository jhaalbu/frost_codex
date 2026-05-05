from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
import math
from typing import Any

from flask import Blueprint, Flask, abort, jsonify, request
from sqlalchemy import and_, select

from frost_sync.config import load_settings
from frost_sync.db import create_session_factory
from frost_sync.frost_api import FrostClient
from frost_sync.models import Observation, Station, StationCapability, StationLatest
from frost_sync.nve_hydapi import NveHydApiClient


PARAMETER_DEFINITIONS = {
    "air_temperature": {
        "label": "Lufttemperatur",
        "unit": "degC",
        "element_ids": ["mean(air_temperature PT1H)"],
        "fallback_element_ids": ["air_temperature"],
        "fallback_aggregation": "mean",
        "style": "line",
    },
    "precipitation_1h": {"label": "Nedbor siste time", "unit": "mm", "element_ids": ["sum(precipitation_amount PT1H)", "precipitation_1h"], "style": "column"},
    "precipitation_24h_rolling": {"label": "Rullande nedbor siste 24 timer", "unit": "mm", "element_ids": [], "style": "line"},
    "snow_depth": {"label": "Snodybde", "unit": "cm", "element_ids": ["snow_depth", "surface_snow_thickness"], "style": "line"},
    "wind_from_direction": {
        "label": "Vindretning",
        "unit": "deg",
        "element_ids": ["mean(wind_from_direction PT1H)"],
        "fallback_element_ids": ["wind_from_direction"],
        "fallback_aggregation": "direction_mean",
        "style": "line",
    },
    "wind_speed": {
        "label": "Vindhastighet",
        "unit": "m/s",
        "element_ids": ["mean(wind_speed PT1H)"],
        "fallback_element_ids": ["wind_speed"],
        "fallback_aggregation": "mean",
        "style": "line",
    },
    "wind_gust_10m": {
        "label": "Maks vindkast siste time",
        "unit": "m/s",
        "element_ids": ["max(wind_speed_of_gust PT1H)"],
        "fallback_element_ids": ["max(wind_speed_of_gust PT10M)"],
        "fallback_aggregation": "max",
        "style": "line",
    },
    "discharge": {"label": "Vannforing", "unit": "m3/s", "element_ids": ["discharge"], "style": "line"},
    "groundwater_level": {"label": "Grunnvannsniva", "unit": "m", "element_ids": ["groundwater_level"], "style": "line"},
}

CAPABILITY_FLAG_MAP = {
    "air_temperature": "has_air_temperature",
    "sum(precipitation_amount PT1H)": "has_precipitation_1h",
    "precipitation_1h": "has_precipitation_1h",
    "snow_depth": "has_snow_depth",
    "surface_snow_thickness": "has_snow_depth",
    "wind_from_direction": "has_wind_from_direction",
    "wind_speed": "has_wind_speed",
    "max(wind_speed_of_gust PT10M)": "has_wind_gust_10m",
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
    frost_client = FrostClient(
        base_url=settings.frost_base_url,
        client_id=settings.frost_client_id or "",
        timeout_seconds=settings.request_timeout_seconds,
        acceptable_quality_codes=settings.acceptable_quality_codes,
    ) if settings.frost_client_id else None
    nve_hydapi_client = NveHydApiClient(
        base_url=settings.nve_hydapi_base_url,
        api_key=settings.nve_hydapi_key,
        timeout_seconds=settings.request_timeout_seconds,
    ) if settings.nve_hydapi_key else None
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

    @blueprint.get("/api/parameters")
    def parameters() -> Any:
        return jsonify(
            [
                {
                    "id": parameter_id,
                    "label": definition["label"],
                    "unit": definition["unit"],
                    "style": definition["style"],
                    "element_ids": definition["element_ids"],
                }
                for parameter_id, definition in PARAMETER_DEFINITIONS.items()
            ]
        )

    @blueprint.get("/api/stations/<source_id>/timeseries")
    def station_timeseries(source_id: str) -> Any:
        parameter_ids = _resolve_parameter_ids(request.args.get("parameters"))
        from_dt, to_dt = _resolve_time_range()

        with session_factory() as session:
            station = (
                session.execute(select(Station).where(Station.source_id == source_id))
                .scalar_one_or_none()
            )
            if station is None:
                abort(404)
            station_provider = station.provider

        if station_provider == "frost":
            if frost_client is None:
                abort(503, description="Frost client is not configured")
            try:
                normalized_rows = _fetch_frost_timeseries_rows(
                    frost_client=frost_client,
                    source_id=source_id,
                    parameter_ids=parameter_ids,
                    from_dt=from_dt,
                    to_dt=to_dt,
                )
            except RuntimeError as exc:
                abort(502, description=str(exc))
        elif station_provider == "nve_hydapi":
            if nve_hydapi_client is None:
                abort(503, description="NVE HydAPI client is not configured")
            try:
                normalized_rows = _fetch_nve_timeseries_rows(
                    hydapi_client=nve_hydapi_client,
                    source_id=source_id,
                    parameter_ids=parameter_ids,
                    from_dt=from_dt,
                    to_dt=to_dt,
                )
            except RuntimeError as exc:
                abort(502, description=str(exc))
        else:
            abort(400, description=f"Unsupported provider: {station_provider}")

        series = {
            parameter_id: _build_direct_series_payload(
                parameter_id=parameter_id,
                rows=normalized_rows,
                from_dt=from_dt,
                to_dt=to_dt,
                provider=station_provider,
            )
            for parameter_id in parameter_ids
        }

        return jsonify(
            {
                "station": _timeseries_station_properties(station),
                "from": _isoformat(from_dt),
                "to": _isoformat(to_dt),
                "series": series,
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
        "wind_gust_10m": "has_wind_gust_10m",
        "discharge": "has_discharge",
        "groundwater_level": "has_groundwater_level",
    }
    return alias_map.get(element_id, "")


def _resolve_parameter_ids(raw_value: str | None) -> list[str]:
    if not raw_value:
        return list(PARAMETER_DEFINITIONS.keys())

    parameter_ids = [item.strip() for item in raw_value.split(",") if item.strip()]
    invalid = [parameter_id for parameter_id in parameter_ids if parameter_id not in PARAMETER_DEFINITIONS]
    if invalid:
        abort(400, description=f"Unknown parameters: {', '.join(invalid)}")
    return parameter_ids


def _fetch_frost_timeseries_rows(
    frost_client: FrostClient,
    source_id: str,
    parameter_ids: list[str],
    from_dt: datetime,
    to_dt: datetime,
) -> list[dict[str, Any]]:
    element_ids = _direct_observation_element_ids(parameter_ids)
    if not element_ids:
        return []
    return frost_client.fetch_observations_range(
        source_ids=source_id,
        elements=sorted(element_ids),
        from_dt=_lookback_start(from_dt, parameter_ids),
        to_dt=to_dt,
    )


def _fetch_nve_timeseries_rows(
    hydapi_client: NveHydApiClient,
    source_id: str,
    parameter_ids: list[str],
    from_dt: datetime,
    to_dt: datetime,
) -> list[dict[str, Any]]:
    series_specs = hydapi_client.fetch_series_specs_for_station(source_id)
    if not series_specs:
        return []
    needed = set(parameter_ids)
    if "precipitation_24h_rolling" in needed:
        needed.add("precipitation_1h")
    filtered_specs = [spec for spec in series_specs if spec.logical_element_id in needed]
    return hydapi_client.fetch_observations_range(
        filtered_specs,
        from_dt=_lookback_start(from_dt, parameter_ids),
        to_dt=to_dt,
    )


def _direct_observation_element_ids(parameter_ids: list[str]) -> set[str]:
    element_ids: set[str] = set()
    for parameter_id in parameter_ids:
        if parameter_id == "precipitation_24h_rolling":
            element_ids.update(PARAMETER_DEFINITIONS["precipitation_1h"]["element_ids"])
            continue
        definition = PARAMETER_DEFINITIONS[parameter_id]
        element_ids.update(definition["element_ids"])
        element_ids.update(definition.get("fallback_element_ids", []))
    return element_ids


def _lookback_start(from_dt: datetime, parameter_ids: list[str]) -> datetime:
    if "precipitation_24h_rolling" in parameter_ids:
        return from_dt - timedelta(hours=24)
    return from_dt


def _build_direct_series_payload(
    parameter_id: str,
    rows: list[dict[str, Any]],
    from_dt: datetime,
    to_dt: datetime,
    provider: str,
) -> dict[str, Any]:
    definition = PARAMETER_DEFINITIONS[parameter_id]
    if parameter_id == "precipitation_24h_rolling":
        points = _build_precipitation_rolling_points(rows, from_dt, to_dt)
    else:
        points = _build_series_points(parameter_id, definition, rows, from_dt, to_dt, provider)
    return {
        "parameter": parameter_id,
        "unit": _series_unit(parameter_id, rows) or definition["unit"],
        "data": [_format_timeseries_point(point) for point in points],
    }


def _build_series_points(
    parameter_id: str,
    definition: dict[str, Any],
    rows: list[dict[str, Any]],
    from_dt: datetime,
    to_dt: datetime,
    provider: str,
) -> list[dict[str, Any]]:
    preferred_points = _build_direct_points(parameter_id, definition["element_ids"], rows, from_dt, to_dt)
    if provider != "frost" or preferred_points or not definition.get("fallback_element_ids"):
        return preferred_points

    fallback_points = _build_direct_points(
        parameter_id,
        definition["fallback_element_ids"],
        rows,
        from_dt,
        to_dt,
    )
    return _aggregate_hourly_points(
        fallback_points,
        mode=definition.get("fallback_aggregation", "mean"),
        element_id=parameter_id,
    )


def _build_direct_points(
    parameter_id: str,
    element_ids: list[str],
    rows: list[dict[str, Any]],
    from_dt: datetime,
    to_dt: datetime,
) -> list[dict[str, Any]]:
    allowed = set(element_ids)
    points_by_time: dict[str, dict[str, Any]] = {}
    for row in sorted(rows, key=lambda item: item.get("referenceTime") or ""):
        reference_time = _parse_timestamp(row["referenceTime"])
        if reference_time < from_dt or reference_time > to_dt:
            continue
        for observation in row.get("observations", []):
            if observation.get("elementId") not in allowed:
                continue
            key = _isoformat(reference_time)
            points_by_time[key] = {
                "time": key,
                "value": observation.get("value"),
                "quality_code": observation.get("qualityCode"),
                "element_id": parameter_id,
            }
    return list(points_by_time.values())


def _build_precipitation_rolling_points(
    rows: list[dict[str, Any]],
    from_dt: datetime,
    to_dt: datetime,
) -> list[dict[str, Any]]:
    points = _build_direct_points(
        parameter_id="precipitation_1h",
        element_ids=PARAMETER_DEFINITIONS["precipitation_1h"]["element_ids"],
        rows=rows,
        from_dt=from_dt - timedelta(hours=24),
        to_dt=to_dt,
    )
    normalized = [
        {"time": _parse_timestamp(point["time"]), "value": point["value"]}
        for point in points
        if point["value"] is not None
    ]
    rolling_points: list[dict[str, Any]] = []
    for point in normalized:
        point_time = point["time"]
        if point_time < from_dt or point_time > to_dt:
            continue
        value = float(
            sum(
                candidate["value"]
                for candidate in normalized
                if candidate["time"] > point_time - timedelta(hours=24)
                and candidate["time"] <= point_time
            )
        )
        rolling_points.append(
            {
                "time": _isoformat(point_time),
                "value": value,
                "quality_code": None,
                "element_id": "precipitation_24h_rolling",
            }
        )
    return rolling_points


def _format_timeseries_point(point: dict[str, Any]) -> dict[str, Any]:
    point_time = _parse_timestamp(point["time"])
    return {
        "time": _isoformat(point_time),
        "timestamp": int(point_time.timestamp() * 1000),
        "value": point.get("value"),
        "quality_code": point.get("quality_code"),
    }


def _aggregate_hourly_points(
    points: list[dict[str, Any]],
    mode: str,
    element_id: str,
) -> list[dict[str, Any]]:
    buckets: dict[datetime, list[dict[str, Any]]] = {}
    for point in points:
        point_time = _parse_timestamp(point["time"])
        bucket_time = point_time.replace(minute=0, second=0, microsecond=0)
        buckets.setdefault(bucket_time, []).append(point)

    aggregated: list[dict[str, Any]] = []
    for bucket_time in sorted(buckets):
        bucket = buckets[bucket_time]
        values = [float(point["value"]) for point in bucket if point.get("value") is not None]
        if not values:
            continue

        if mode == "max":
            value = max(values)
        elif mode == "direction_mean":
            value = _circular_mean_degrees(values)
        else:
            value = sum(values) / len(values)

        aggregated.append(
            {
                "time": _isoformat(bucket_time),
                "value": value,
                "quality_code": None,
                "element_id": element_id,
            }
        )
    return aggregated


def _circular_mean_degrees(values: list[float]) -> float:
    sin_sum = sum(math.sin(math.radians(value)) for value in values)
    cos_sum = sum(math.cos(math.radians(value)) for value in values)
    if sin_sum == 0 and cos_sum == 0:
        return values[-1]
    angle = math.degrees(math.atan2(sin_sum, cos_sum))
    return angle % 360.0


def _series_unit(parameter_id: str, rows: list[dict[str, Any]]) -> str | None:
    if parameter_id == "precipitation_24h_rolling":
        parameter_id = "precipitation_1h"
    allowed = set(PARAMETER_DEFINITIONS[parameter_id]["element_ids"])
    for row in rows:
        for observation in row.get("observations", []):
            if observation.get("elementId") in allowed and observation.get("unit"):
                return _normalize_unit(str(observation["unit"]))
    return None


def _normalize_unit(unit: str) -> str:
    normalized = unit.strip()
    replacements = {
        "mł/s": "m3/s",
        "m³/s": "m3/s",
        "°C": "degC",
    }
    return replacements.get(normalized, normalized)


def _build_timeseries_series(
    parameter_id: str,
    observation_rows: list[Observation],
    latest: StationLatest | None,
    from_dt: datetime,
    to_dt: datetime,
) -> dict[str, Any]:
    definition = PARAMETER_DEFINITIONS[parameter_id]
    if definition["element_ids"]:
        return _build_observation_series(parameter_id, definition, observation_rows)
    return _build_derived_series(parameter_id, definition, observation_rows, latest, from_dt, to_dt)


def _build_observation_series(parameter_id: str, definition: dict[str, Any], observation_rows: list[Observation]) -> dict[str, Any]:
    element_ids = set(definition["element_ids"])
    unit = definition["unit"]
    points = []
    for row in observation_rows:
        if row.element_id not in element_ids:
            continue
        if row.unit and not unit:
            unit = row.unit
        points.append(
            {
                "time": _isoformat(row.reference_time),
                "value": row.value,
                "quality_code": row.quality_code,
                "element_id": row.element_id,
            }
        )
    return {
        "parameter": parameter_id,
        "label": definition["label"],
        "unit": unit,
        "points": points,
    }


def _build_derived_series(
    parameter_id: str,
    definition: dict[str, Any],
    observation_rows: list[Observation],
    latest: StationLatest | None,
    from_dt: datetime,
    to_dt: datetime,
) -> dict[str, Any]:
    points = _derived_points(parameter_id, observation_rows, from_dt, to_dt)
    if not points and latest is not None:
        observed_at = _ensure_utc(latest.observed_at)
        if observed_at is not None and observed_at >= from_dt and observed_at <= to_dt:
            value, unit = _derived_parameter_value(parameter_id, latest, definition["unit"])
            if value is not None:
                points = [{"time": _isoformat(observed_at), "value": value, "quality_code": None, "element_id": parameter_id}]
                return {
                    "parameter": parameter_id,
                    "label": definition["label"],
                    "unit": unit,
                    "points": points,
                }
    return {
        "parameter": parameter_id,
        "label": definition["label"],
        "unit": definition["unit"],
        "points": points,
    }


def _derived_parameter_value(parameter_id: str, latest: StationLatest, fallback_unit: str | None) -> tuple[float | None, str | None]:
    mapping = {
        "air_temperature_min": (latest.air_temperature_min, latest.air_temperature_min_unit),
        "air_temperature_max": (latest.air_temperature_max, latest.air_temperature_max_unit),
        "precipitation_1h_max": (latest.precipitation_1h_max, latest.precipitation_1h_max_unit),
        "precipitation_3h": (latest.precipitation_3h, latest.precipitation_3h_unit),
        "precipitation_3h_max": (latest.precipitation_3h_max, latest.precipitation_3h_max_unit),
        "precipitation_24h": (latest.precipitation_24h, latest.precipitation_24h_unit),
        "snow_depth_change": (latest.snow_depth_change, latest.snow_depth_change_unit),
        "wind_speed_max": (latest.wind_speed_max, latest.wind_speed_max_unit),
        "wind_from_direction_max": (latest.wind_from_direction_max, latest.wind_from_direction_max_unit),
    }
    value, unit = mapping.get(parameter_id, (None, None))
    return value, unit or fallback_unit


def _observation_element_ids_for_parameters(parameter_ids: list[str]) -> set[str]:
    element_ids: set[str] = set()
    for parameter_id in parameter_ids:
        element_ids.update(PARAMETER_DEFINITIONS[parameter_id]["element_ids"])
        element_ids.update(_derived_dependency_element_ids(parameter_id))
    return element_ids


def _derived_dependency_element_ids(parameter_id: str) -> set[str]:
    mapping = {
        "air_temperature_min": {"air_temperature"},
        "air_temperature_max": {"air_temperature"},
        "precipitation_1h_max": {"sum(precipitation_amount PT1H)", "precipitation_1h"},
        "precipitation_3h": {"sum(precipitation_amount PT1H)", "precipitation_1h"},
        "precipitation_3h_max": {"sum(precipitation_amount PT1H)", "precipitation_1h"},
        "precipitation_24h": {"sum(precipitation_amount PT1H)", "precipitation_1h"},
        "snow_depth_change": {"snow_depth", "surface_snow_thickness"},
        "wind_speed_max": {"wind_speed"},
        "wind_from_direction_max": {"wind_speed", "wind_from_direction"},
    }
    return mapping.get(parameter_id, set())


def _max_lookback(parameter_ids: list[str]) -> timedelta:
    derived_with_lookback = {
        "air_temperature_min",
        "air_temperature_max",
        "precipitation_1h_max",
        "precipitation_3h",
        "precipitation_3h_max",
        "precipitation_24h",
        "snow_depth_change",
        "wind_speed_max",
        "wind_from_direction_max",
    }
    if any(parameter_id in derived_with_lookback for parameter_id in parameter_ids):
        return timedelta(hours=24)
    return timedelta(0)


def _derived_points(parameter_id: str, observation_rows: list[Observation], from_dt: datetime, to_dt: datetime) -> list[dict[str, Any]]:
    if parameter_id in {"air_temperature_min", "air_temperature_max"}:
        rows = _rows_for_elements(observation_rows, {"air_temperature"})
        return _window_extreme_points(rows, from_dt, to_dt, hours=24, mode="min" if parameter_id.endswith("_min") else "max", element_id=parameter_id)
    if parameter_id in {"precipitation_1h_max", "precipitation_3h", "precipitation_3h_max", "precipitation_24h"}:
        rows = _rows_for_elements(observation_rows, {"sum(precipitation_amount PT1H)", "precipitation_1h"})
        if parameter_id == "precipitation_1h_max":
            return _window_extreme_points(rows, from_dt, to_dt, hours=24, mode="max", element_id=parameter_id)
        if parameter_id == "precipitation_3h":
            return _rolling_value_points(rows, from_dt, to_dt, hours=3, element_id=parameter_id)
        if parameter_id == "precipitation_3h_max":
            return _window_rolling_extreme_points(rows, from_dt, to_dt, base_hours=3, window_hours=24, element_id=parameter_id)
        return _rolling_value_points(rows, from_dt, to_dt, hours=24, element_id=parameter_id)
    if parameter_id == "snow_depth_change":
        rows = _rows_for_elements(observation_rows, {"snow_depth", "surface_snow_thickness"})
        return _rolling_change_points(rows, from_dt, to_dt, hours=24, element_id=parameter_id)
    if parameter_id == "wind_speed_max":
        rows = _rows_for_elements(observation_rows, {"wind_speed"})
        return _window_extreme_points(rows, from_dt, to_dt, hours=24, mode="max", element_id=parameter_id)
    if parameter_id == "wind_from_direction_max":
        wind_rows = _rows_for_elements(observation_rows, {"wind_speed"})
        direction_rows = _rows_for_elements(observation_rows, {"wind_from_direction"})
        return _wind_direction_at_max_points(wind_rows, direction_rows, from_dt, to_dt, hours=24, element_id=parameter_id)
    return []


def _rows_for_elements(observation_rows: list[Observation], element_ids: set[str]) -> list[Observation]:
    return [row for row in observation_rows if row.element_id in element_ids and row.value is not None]


def _window_extreme_points(rows: list[Observation], from_dt: datetime, to_dt: datetime, hours: int, mode: str, element_id: str) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    for row in rows:
        row_time = _ensure_utc(row.reference_time)
        if row_time is None or row_time < from_dt or row_time > to_dt:
            continue
        window_rows = [
            candidate
            for candidate in rows
            if (candidate_time := _ensure_utc(candidate.reference_time)) is not None
            and candidate_time > row_time - timedelta(hours=hours)
            and candidate_time <= row_time
        ]
        if not window_rows:
            continue
        values = [candidate.value for candidate in window_rows if candidate.value is not None]
        if not values:
            continue
        value = min(values) if mode == "min" else max(values)
        points.append({"time": _isoformat(row_time), "value": value, "quality_code": None, "element_id": element_id})
    return points


def _rolling_value_points(rows: list[Observation], from_dt: datetime, to_dt: datetime, hours: int, element_id: str) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    for row in rows:
        row_time = _ensure_utc(row.reference_time)
        if row_time is None or row_time < from_dt or row_time > to_dt:
            continue
        value = _sum_rows_in_window(rows, row_time, hours)
        if value is None:
            continue
        points.append({"time": _isoformat(row_time), "value": value, "quality_code": None, "element_id": element_id})
    return points


def _window_rolling_extreme_points(rows: list[Observation], from_dt: datetime, to_dt: datetime, base_hours: int, window_hours: int, element_id: str) -> list[dict[str, Any]]:
    rolling_points = _rolling_value_points(rows, from_dt - timedelta(hours=window_hours), to_dt, hours=base_hours, element_id=element_id)
    points: list[dict[str, Any]] = []
    for point in rolling_points:
        point_time = _parse_timestamp(point["time"])
        if point_time < from_dt or point_time > to_dt:
            continue
        window_values = [
            candidate["value"]
            for candidate in rolling_points
            if (candidate_time := _parse_timestamp(candidate["time"])) > point_time - timedelta(hours=window_hours)
            and candidate_time <= point_time
            and candidate["value"] is not None
        ]
        if not window_values:
            continue
        points.append({"time": point["time"], "value": max(window_values), "quality_code": None, "element_id": element_id})
    return points


def _rolling_change_points(rows: list[Observation], from_dt: datetime, to_dt: datetime, hours: int, element_id: str) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    for row in rows:
        row_time = _ensure_utc(row.reference_time)
        if row_time is None or row_time < from_dt or row_time > to_dt or row.value is None:
            continue
        window_rows = [
            candidate
            for candidate in rows
            if (candidate_time := _ensure_utc(candidate.reference_time)) is not None
            and candidate_time > row_time - timedelta(hours=hours)
            and candidate_time <= row_time
            and candidate.value is not None
        ]
        if len(window_rows) < 2:
            continue
        change = row.value - window_rows[0].value
        points.append({"time": _isoformat(row_time), "value": change, "quality_code": None, "element_id": element_id})
    return points


def _wind_direction_at_max_points(
    wind_rows: list[Observation],
    direction_rows: list[Observation],
    from_dt: datetime,
    to_dt: datetime,
    hours: int,
    element_id: str,
) -> list[dict[str, Any]]:
    direction_by_time = {
        _isoformat(_ensure_utc(row.reference_time)): row.value
        for row in direction_rows
        if row.value is not None
    }
    points: list[dict[str, Any]] = []
    for row in wind_rows:
        row_time = _ensure_utc(row.reference_time)
        if row_time is None or row_time < from_dt or row_time > to_dt:
            continue
        window_rows = [
            candidate
            for candidate in wind_rows
            if (candidate_time := _ensure_utc(candidate.reference_time)) is not None
            and candidate_time > row_time - timedelta(hours=hours)
            and candidate_time <= row_time
            and candidate.value is not None
        ]
        if not window_rows:
            continue
        max_row = max(window_rows, key=lambda candidate: candidate.value if candidate.value is not None else float("-inf"))
        direction_value = direction_by_time.get(_isoformat(_ensure_utc(max_row.reference_time)))
        if direction_value is None:
            continue
        points.append({"time": _isoformat(row_time), "value": direction_value, "quality_code": None, "element_id": element_id})
    return points


def _sum_rows_in_window(rows: list[Observation], row_time: datetime, hours: int) -> float | None:
    values = [
        candidate.value
        for candidate in rows
        if (candidate_time := _ensure_utc(candidate.reference_time)) is not None
        and candidate_time > row_time - timedelta(hours=hours)
        and candidate_time <= row_time
        and candidate.value is not None
    ]
    return float(sum(values)) if values else None


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


def _timeseries_station_properties(station: Station) -> dict[str, Any]:
    return {
        "provider": station.provider,
        "source_id": station.source_id,
        "stationholder": station.stationholder,
        "name": station.name,
        "masl": station.masl,
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
