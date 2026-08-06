from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
import gzip
import json
import math
from pathlib import Path
from typing import Any

from flask import Blueprint, Flask, abort, jsonify, request, send_file
from sqlalchemy import and_, select

from frost_sync.config import Settings, load_settings
from frost_sync.db import create_session_factory
from frost_sync.frost_api import FrostClient
from frost_sync.models import (
    NveDischargeFloodThreshold,
    NveDischargePercentile,
    Observation,
    Station,
    StationCapability,
    StationLatest,
)
from frost_sync.nve_hydapi import NveHydApiClient
from frost_sync.snower_api import SnowerClient


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
    "precipitation_accumulated": {"label": "Akkumulert nedbor", "unit": "mm", "element_ids": [], "style": "line"},
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

SEVEN_DAY_AGGREGATE_ELEMENT_MAP = {
    "air_temperature": ["air_temperature", "mean(air_temperature PT1H)"],
    "precipitation_1h": ["sum(precipitation_amount PT1H)", "precipitation_1h"],
    "snow_depth": ["snow_depth", "surface_snow_thickness"],
    "wind_speed": ["wind_speed", "mean(wind_speed PT1H)"],
    "wind_gust": ["max(wind_speed_of_gust PT10M)", "max(wind_speed_of_gust PT1H)"],
    "discharge": ["discharge"],
    "groundwater_level": ["groundwater_level"],
}

SEVEN_DAY_AGGREGATE_ELEMENT_IDS = {
    element_id
    for element_ids in SEVEN_DAY_AGGREGATE_ELEMENT_MAP.values()
    for element_id in element_ids
}

EXCLUDED_PRECIPITATION_SOURCE_IDS = {"2.36.0", "SN11000"}

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
    snower_client = SnowerClient(
        base_url=settings.snower_base_url,
        username=settings.snower_username,
        password=settings.snower_password,
        domain=settings.snower_domain,
        domain_id=settings.snower_domain_id,
        timeout_seconds=settings.request_timeout_seconds,
    ) if settings.snower_username and settings.snower_password and settings.snower_domain_id else None
    blueprint = Blueprint(name, __name__)

    @blueprint.get("/health")
    def health() -> Any:
        return {"status": "ok"}

    @blueprint.get("/api/stations/latest.geojson")
    def latest_geojson() -> Any:
        has_filter = request.args.get("has")
        if not has_filter:
            cached_response = _cached_geojson_response(settings, "latest.geojson")
            if cached_response is not None:
                return cached_response
        response = jsonify(build_latest_geojson(session_factory, has_filter=has_filter))
        response.headers["X-GeoJSON-Cache"] = "database-fallback"
        return response

    @blueprint.get("/api/stations/latest.compact.geojson")
    def latest_compact_geojson() -> Any:
        cached_response = _cached_geojson_response(settings, "latest.compact.geojson")
        if cached_response is not None:
            return cached_response
        response = jsonify(build_latest_compact_geojson(session_factory))
        response.headers["X-GeoJSON-Cache"] = "database-fallback"
        return response

    @blueprint.get("/api/stations/latest.fme.geojson")
    def latest_fme_geojson() -> Any:
        cached_response = _cached_geojson_response(settings, "latest.fme.geojson")
        if cached_response is not None:
            return cached_response
        response = jsonify(build_latest_fme_geojson(session_factory))
        response.headers["X-GeoJSON-Cache"] = "database-fallback"
        return response

    @blueprint.get("/api/stations/latest.7d.geojson")
    def latest_7d_geojson() -> Any:
        cached_response = _cached_geojson_response(settings, "latest.7d.geojson")
        if cached_response is not None:
            return cached_response
        response = jsonify(build_latest_7d_geojson(session_factory))
        response.headers["X-GeoJSON-Cache"] = "database-fallback"
        return response

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
            "latest": _latest_properties_for_station(station, latest) if latest else None,
        }
        return jsonify(payload)

    @blueprint.get("/api/stations/<source_id>/discharge-thresholds")
    def station_discharge_thresholds(source_id: str) -> Any:
        with session_factory() as session:
            row = (
                session.execute(
                    select(Station, NveDischargeFloodThreshold)
                    .outerjoin(
                        NveDischargeFloodThreshold,
                        NveDischargeFloodThreshold.station_id == Station.id,
                    )
                    .where(Station.source_id == source_id)
                )
                .one_or_none()
            )
        if row is None:
            abort(404, description=f"Station {source_id} was not found")
        station, threshold = row
        if station.provider != "nve_hydapi" or threshold is None:
            abort(404, description=f"No discharge flood thresholds are available for {source_id}")
        return jsonify(
            {
                "source_id": station.source_id,
                "name": station.name,
                "discharge_flood_qm": threshold.discharge_qm,
                "discharge_flood_q5": threshold.discharge_q5,
                "discharge_flood_q50": threshold.discharge_q50,
                "discharge_flood_unit": threshold.unit,
                "discharge_flood_series_version": threshold.series_version,
                "discharge_flood_updated_at": _isoformat(threshold.updated_at),
            }
        )

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

    @blueprint.get("/api/timeseries")
    def stations_timeseries() -> Any:
        source_ids = _resolve_source_ids(
            request.args.get("stations") or request.args.get("source_ids")
        )
        parameter_ids = _resolve_parameter_ids(request.args.get("parameters"))
        from_dt, to_dt = _resolve_time_range()

        with session_factory() as session:
            stations = (
                session.execute(select(Station).where(Station.source_id.in_(source_ids)))
                .scalars()
                .all()
            )
            stations_by_source = {station.source_id: station for station in stations}

        station_payloads, errors = _build_timeseries_payloads_for_stations(
            stations=[station for source_id in source_ids if (station := stations_by_source.get(source_id)) is not None],
            parameter_ids=parameter_ids,
            from_dt=from_dt,
            to_dt=to_dt,
            frost_client=frost_client,
            nve_hydapi_client=nve_hydapi_client,
            snower_client=snower_client,
        )

        return jsonify(
            {
                "from": _isoformat(from_dt),
                "to": _isoformat(to_dt),
                "parameters": parameter_ids,
                "stations": station_payloads,
                "missing": [source_id for source_id in source_ids if source_id not in stations_by_source],
                "errors": errors,
            }
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

        try:
            payload = _build_timeseries_payload_for_station(
                station=station,
                parameter_ids=parameter_ids,
                from_dt=from_dt,
                to_dt=to_dt,
                frost_client=frost_client,
                nve_hydapi_client=nve_hydapi_client,
                snower_client=snower_client,
            )
        except RuntimeError as exc:
            abort(502, description=str(exc))

        return jsonify(
            {
                "station": payload["station"],
                "from": _isoformat(from_dt),
                "to": _isoformat(to_dt),
                "series": payload["series"],
            }
        )

    return blueprint


def refresh_geojson_cache(settings: Settings | None = None) -> dict[str, Path]:
    settings = settings or load_settings()
    session_factory = create_session_factory(settings.database_url)
    payloads = {
        "latest.geojson": build_latest_geojson(session_factory),
        "latest.fme.geojson": build_latest_fme_geojson(session_factory),
        "latest.compact.geojson": build_latest_compact_geojson(session_factory),
        "latest.7d.geojson": build_latest_7d_geojson(session_factory),
    }

    cache_dir = _geojson_cache_dir(settings)
    cache_dir.mkdir(parents=True, exist_ok=True)
    written: dict[str, Path] = {}
    for filename, payload in payloads.items():
        path = cache_dir / filename
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        content = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        tmp_path.write_bytes(content)
        tmp_path.replace(path)
        gzip_path = path.with_suffix(path.suffix + ".gz")
        tmp_gzip_path = gzip_path.with_suffix(gzip_path.suffix + ".tmp")
        tmp_gzip_path.write_bytes(gzip.compress(content, compresslevel=6, mtime=0))
        tmp_gzip_path.replace(gzip_path)
        written[filename] = path
    return written


def build_latest_geojson(session_factory, has_filter: str | None = None) -> dict[str, Any]:
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
        discharge_percentiles = _load_discharge_percentiles(session, rows)
        flood_thresholds = _load_discharge_flood_thresholds(session, rows)

    features = []
    for station, latest in rows:
        if station.longitude is None or station.latitude is None:
            continue
        if _is_suspect_nve_feature(station, latest):
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
                    **_latest_properties_for_station(station, latest),
                    **_discharge_classification_properties(
                        station,
                        latest,
                        discharge_percentiles.get(_discharge_percentile_key(station, latest)),
                        flood_thresholds.get(station.id),
                    ),
                },
            }
        )

    return {"type": "FeatureCollection", "features": features}


def build_latest_compact_geojson(session_factory) -> dict[str, Any]:
    with session_factory() as session:
        rows = (
            session.execute(
                select(Station, StationLatest)
                .join(StationLatest, StationLatest.station_id == Station.id)
                .order_by(Station.source_id)
            )
            .all()
        )
        discharge_percentiles = _load_discharge_percentiles(session, rows)
        flood_thresholds = _load_discharge_flood_thresholds(session, rows)

    features = []
    for station, latest in rows:
        if station.longitude is None or station.latitude is None:
            continue
        if _is_suspect_nve_feature(station, latest):
            continue

        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [station.longitude, station.latitude],
                },
                "properties": _compact_latest_properties(
                    station,
                    latest,
                    discharge_percentiles.get(_discharge_percentile_key(station, latest)),
                    flood_thresholds.get(station.id),
                ),
            }
        )

    return {"type": "FeatureCollection", "features": features}


def build_latest_fme_geojson(session_factory) -> dict[str, Any]:
    payload = build_latest_geojson(session_factory)
    for feature in payload["features"]:
        properties = feature["properties"]
        precipitation_1h_max_time = _period_end_time(
            properties.pop("precipitation_1h_max_period", None)
        )
        precipitation_3h_max_time = _period_end_time(
            properties.pop("precipitation_3h_max_period", None)
        )
        feature["properties"] = {
            key: _fme_property_value(value)
            for key, value in properties.items()
            if _keep_fme_latest_property(key)
        }
        feature["properties"]["precipitation_1h_max_time"] = precipitation_1h_max_time
        feature["properties"]["precipitation_3h_max_time"] = precipitation_3h_max_time
        feature["properties"].setdefault("discharge", None)
        feature["properties"].setdefault("discharge_class", None)
    return payload


def _period_end_time(period: str | None) -> str | None:
    if not period:
        return None
    return period.rsplit("/", 1)[-1] or None


def _fme_property_value(value: Any) -> Any:
    return int(value) if isinstance(value, bool) else value


def _keep_fme_latest_property(key: str) -> bool:
    if key.endswith("_unit"):
        return False
    if key in {
        "latitude",
        "longitude",
        "country",
        "county",
        "municipality",
        "last_seen_at",
        "valid_to",
        "parameter_profile",
        "available_parameter_count",
        "has_wind_gust_10m",
    }:
        return False
    if key.startswith("discharge_") and key not in {
        "discharge_class",
        "discharge_observed_at",
    }:
        return False
    return True


def build_latest_7d_geojson(session_factory) -> dict[str, Any]:
    window_to = datetime.now(timezone.utc)
    window_from = window_to - timedelta(days=7)

    with session_factory() as session:
        rows = (
            session.execute(
                select(Station, StationLatest)
                .join(StationLatest, StationLatest.station_id == Station.id)
                .order_by(Station.source_id)
            )
            .all()
        )
        observation_rows = (
            session.execute(
                select(Observation)
                .where(
                    and_(
                        Observation.reference_time >= window_from,
                        Observation.reference_time <= window_to,
                        Observation.element_id.in_(SEVEN_DAY_AGGREGATE_ELEMENT_IDS),
                    )
                )
                .order_by(Observation.station_id, Observation.reference_time)
            )
            .scalars()
            .all()
        )
        flood_thresholds = _load_discharge_flood_thresholds(session, rows)

    stations_by_id = {station.id: station for station, _latest in rows}
    aggregates_by_station = _seven_day_aggregates_by_station(observation_rows, stations_by_id)
    discharge_percentile_keys = {
        key
        for station, _latest in rows
        if (
            key := _discharge_7d_percentile_key(
                station,
                aggregates_by_station.get(station.id, {}),
            )
        ) is not None
    }
    with session_factory() as session:
        discharge_percentiles = _load_discharge_percentiles_for_keys(
            session,
            discharge_percentile_keys,
        )

    features = []
    for station, latest in rows:
        if station.longitude is None or station.latitude is None:
            continue
        if _is_suspect_nve_feature(station, latest):
            continue

        aggregates = aggregates_by_station.get(station.id)
        if not aggregates:
            continue

        discharge_max = aggregates.get("discharge_max_7d")
        discharge_max_time = aggregates.get("discharge_max_7d_time")
        discharge_key = _discharge_7d_percentile_key(station, aggregates)
        discharge_classification = _discharge_classification_for_value(
            station=station,
            discharge=discharge_max,
            observed_at=_parse_timestamp(discharge_max_time) if discharge_max_time else None,
            percentile=discharge_percentiles.get(discharge_key) if discharge_key else None,
            flood_threshold=flood_thresholds.get(station.id),
        )
        properties = {
            **_compact_latest_base_properties(station, latest),
            **aggregates,
            "discharge_class": discharge_classification.get("discharge_class"),
        }
        properties.pop("has_recent_data", None)

        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [station.longitude, station.latitude],
                },
                "properties": _without_null_values(properties),
            }
        )

    return {"type": "FeatureCollection", "features": features}


def _cached_geojson_response(settings: Settings, filename: str) -> Any | None:
    path = _geojson_cache_dir(settings) / filename
    if not path.exists():
        return None

    gzip_path = path.with_suffix(path.suffix + ".gz")
    if gzip_path.exists() and "gzip" in request.headers.get("Accept-Encoding", ""):
        response = send_file(gzip_path, mimetype="application/geo+json", conditional=True, max_age=300)
        response.headers["Content-Encoding"] = "gzip"
        response.headers["Vary"] = "Accept-Encoding"
        response.headers["X-GeoJSON-Cache"] = "file-gzip"
        return response

    response = send_file(path, mimetype="application/geo+json", conditional=True, max_age=300)
    response.headers["X-GeoJSON-Cache"] = "file"
    return response


def _geojson_cache_dir(settings: Settings) -> Path:
    path = Path(settings.geojson_cache_dir)
    if path.is_absolute():
        return path
    return Path(__file__).resolve().parent.parent / path


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
            flags[flag_name] = flags.get(flag_name, False) or bool(row.available)
    return capabilities


def _capability_flags_from_capabilities(capabilities: dict[str, bool]) -> dict[str, bool]:
    flags = _empty_capability_flags()
    for element_id, available in capabilities.items():
        flag_name = CAPABILITY_FLAG_MAP.get(element_id)
        if flag_name:
            flags[flag_name] = flags.get(flag_name, False) or bool(available)
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


def _resolve_source_ids(raw_value: str | None) -> list[str]:
    if not raw_value:
        abort(400, description="Use ?stations=SOURCE1,SOURCE2")
    source_ids = []
    seen = set()
    for item in raw_value.split(","):
        source_id = item.strip()
        if not source_id or source_id in seen:
            continue
        source_ids.append(source_id)
        seen.add(source_id)
    if not source_ids:
        abort(400, description="Use ?stations=SOURCE1,SOURCE2")
    return source_ids


def _build_timeseries_payload_for_station(
    station: Station,
    parameter_ids: list[str],
    from_dt: datetime,
    to_dt: datetime,
    frost_client: FrostClient | None,
    nve_hydapi_client: NveHydApiClient | None,
    snower_client: SnowerClient | None,
) -> dict[str, Any]:
    normalized_rows = _fetch_timeseries_rows_for_station(
        station=station,
        parameter_ids=parameter_ids,
        from_dt=from_dt,
        to_dt=to_dt,
        frost_client=frost_client,
        nve_hydapi_client=nve_hydapi_client,
        snower_client=snower_client,
    )
    return {
        "station": _timeseries_station_properties(station),
        "series": {
            parameter_id: _build_direct_series_payload(
                parameter_id=parameter_id,
                rows=normalized_rows,
                from_dt=from_dt,
                to_dt=to_dt,
                provider=station.provider,
            )
            for parameter_id in parameter_ids
        },
    }


def _build_timeseries_payloads_for_stations(
    stations: list[Station],
    parameter_ids: list[str],
    from_dt: datetime,
    to_dt: datetime,
    frost_client: FrostClient | None,
    nve_hydapi_client: NveHydApiClient | None,
    snower_client: SnowerClient | None,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    rows_by_source: dict[str, list[dict[str, Any]]] = {station.source_id: [] for station in stations}
    errors: list[dict[str, str]] = []

    frost_stations = [station for station in stations if station.provider == "frost"]
    if frost_stations:
        if frost_client is None:
            errors.extend(
                {"source_id": station.source_id, "message": "Frost client is not configured"}
                for station in frost_stations
            )
        else:
            try:
                frost_rows = _fetch_frost_timeseries_rows_for_sources(
                    frost_client=frost_client,
                    source_ids=[station.source_id for station in frost_stations],
                    parameter_ids=parameter_ids,
                    from_dt=from_dt,
                    to_dt=to_dt,
                )
                _extend_rows_by_source(rows_by_source, frost_rows)
            except RuntimeError as exc:
                errors.extend({"source_id": station.source_id, "message": str(exc)} for station in frost_stations)

    nve_stations = [station for station in stations if station.provider == "nve_hydapi"]
    if nve_stations:
        if nve_hydapi_client is None:
            errors.extend(
                {"source_id": station.source_id, "message": "NVE HydAPI client is not configured"}
                for station in nve_stations
            )
        else:
            try:
                nve_rows = _fetch_nve_timeseries_rows_for_sources(
                    hydapi_client=nve_hydapi_client,
                    source_ids=[station.source_id for station in nve_stations],
                    parameter_ids=parameter_ids,
                    from_dt=from_dt,
                    to_dt=to_dt,
                )
                _extend_rows_by_source(rows_by_source, nve_rows)
            except RuntimeError as exc:
                errors.extend({"source_id": station.source_id, "message": str(exc)} for station in nve_stations)

    for station in stations:
        if station.provider == "snower":
            if snower_client is None:
                errors.append({"source_id": station.source_id, "message": "Snower client is not configured"})
                continue
            try:
                rows_by_source[station.source_id] = _fetch_snower_timeseries_rows(
                    snower_client=snower_client,
                    station=station,
                    parameter_ids=parameter_ids,
                    from_dt=from_dt,
                    to_dt=to_dt,
                )
            except RuntimeError as exc:
                errors.append({"source_id": station.source_id, "message": str(exc)})
        elif station.provider not in {"frost", "nve_hydapi"}:
            errors.append({"source_id": station.source_id, "message": f"Unsupported provider: {station.provider}"})

    payloads = [
        _build_timeseries_payload_from_rows(
            station=station,
            parameter_ids=parameter_ids,
            rows=rows_by_source.get(station.source_id, []),
            from_dt=from_dt,
            to_dt=to_dt,
        )
        for station in stations
        if not any(error["source_id"] == station.source_id for error in errors)
    ]
    return payloads, errors


def _build_timeseries_payload_from_rows(
    station: Station,
    parameter_ids: list[str],
    rows: list[dict[str, Any]],
    from_dt: datetime,
    to_dt: datetime,
) -> dict[str, Any]:
    return {
        "station": _timeseries_station_properties(station),
        "series": {
            parameter_id: _build_direct_series_payload(
                parameter_id=parameter_id,
                rows=rows,
                from_dt=from_dt,
                to_dt=to_dt,
                provider=station.provider,
            )
            for parameter_id in parameter_ids
        },
    }


def _extend_rows_by_source(
    rows_by_source: dict[str, list[dict[str, Any]]],
    rows: list[dict[str, Any]],
) -> None:
    for row in rows:
        source_id = _normalize_source_id(row.get("sourceId"))
        if source_id in rows_by_source:
            rows_by_source[source_id].append(row)


def _fetch_timeseries_rows_for_station(
    station: Station,
    parameter_ids: list[str],
    from_dt: datetime,
    to_dt: datetime,
    frost_client: FrostClient | None,
    nve_hydapi_client: NveHydApiClient | None,
    snower_client: SnowerClient | None,
) -> list[dict[str, Any]]:
    if station.provider == "frost":
        if frost_client is None:
            raise RuntimeError("Frost client is not configured")
        return _fetch_frost_timeseries_rows(
            frost_client=frost_client,
            source_id=station.source_id,
            parameter_ids=parameter_ids,
            from_dt=from_dt,
            to_dt=to_dt,
        )
    if station.provider == "nve_hydapi":
        if nve_hydapi_client is None:
            raise RuntimeError("NVE HydAPI client is not configured")
        return _fetch_nve_timeseries_rows(
            hydapi_client=nve_hydapi_client,
            source_id=station.source_id,
            parameter_ids=parameter_ids,
            from_dt=from_dt,
            to_dt=to_dt,
        )
    if station.provider == "snower":
        if snower_client is None:
            raise RuntimeError("Snower client is not configured")
        return _fetch_snower_timeseries_rows(
            snower_client=snower_client,
            station=station,
            parameter_ids=parameter_ids,
            from_dt=from_dt,
            to_dt=to_dt,
        )
    raise RuntimeError(f"Unsupported provider: {station.provider}")


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


def _fetch_frost_timeseries_rows_for_sources(
    frost_client: FrostClient,
    source_ids: list[str],
    parameter_ids: list[str],
    from_dt: datetime,
    to_dt: datetime,
) -> list[dict[str, Any]]:
    element_ids = _direct_observation_element_ids(parameter_ids)
    if not element_ids or not source_ids:
        return []
    return frost_client.fetch_observations_range(
        source_ids=source_ids,
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


def _fetch_nve_timeseries_rows_for_sources(
    hydapi_client: NveHydApiClient,
    source_ids: list[str],
    parameter_ids: list[str],
    from_dt: datetime,
    to_dt: datetime,
) -> list[dict[str, Any]]:
    series_specs = hydapi_client.fetch_series_specs_for_stations(source_ids)
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


def _fetch_snower_timeseries_rows(
    snower_client: SnowerClient,
    station: Station,
    parameter_ids: list[str],
    from_dt: datetime,
    to_dt: datetime,
) -> list[dict[str, Any]]:
    supported = [parameter_id for parameter_id in parameter_ids if parameter_id in {"air_temperature", "snow_depth"}]
    if not supported:
        return []
    return snower_client.fetch_observations_range(
        provider_context=station.provider_context,
        from_dt=from_dt,
        to_dt=to_dt,
        logical_parameter_ids=supported,
    )


def _normalize_source_id(value: str | None) -> str:
    if not value:
        return ""
    return value.split(":", 1)[0]


def _direct_observation_element_ids(parameter_ids: list[str]) -> set[str]:
    element_ids: set[str] = set()
    for parameter_id in parameter_ids:
        if parameter_id in {"precipitation_24h_rolling", "precipitation_accumulated"}:
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
    elif parameter_id == "precipitation_accumulated":
        points = _build_precipitation_accumulated_points(rows, from_dt, to_dt)
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
    if preferred_points or not definition.get("fallback_element_ids"):
        return preferred_points

    fallback_points = _build_direct_points(
        parameter_id,
        definition["fallback_element_ids"],
        rows,
        from_dt,
        to_dt,
    )
    if provider != "frost":
        return fallback_points
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


def _build_precipitation_accumulated_points(
    rows: list[dict[str, Any]],
    from_dt: datetime,
    to_dt: datetime,
) -> list[dict[str, Any]]:
    points = _build_direct_points(
        parameter_id="precipitation_1h",
        element_ids=PARAMETER_DEFINITIONS["precipitation_1h"]["element_ids"],
        rows=rows,
        from_dt=from_dt,
        to_dt=to_dt,
    )
    accumulated = 0.0
    accumulated_points: list[dict[str, Any]] = []
    for point in points:
        if point["value"] is None:
            continue
        accumulated += float(point["value"])
        accumulated_points.append(
            {
                "time": point["time"],
                "value": accumulated,
                "quality_code": point.get("quality_code"),
                "element_id": "precipitation_accumulated",
            }
        )
    return accumulated_points


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


def _is_suspect_nve_feature(station: Station, latest: StationLatest) -> bool:
    if station.provider != "nve_hydapi":
        return False

    if station.longitude is None or station.latitude is None:
        return True

    in_norway_bounds = 4.0 <= station.longitude <= 32.0 and 57.0 <= station.latitude <= 72.5
    if not in_norway_bounds:
        return True

    if latest.precipitation_1h is not None and (latest.precipitation_1h < 0 or latest.precipitation_1h > 300):
        return True
    if latest.snow_depth is not None and (latest.snow_depth < -5 or latest.snow_depth > 1000):
        return True
    if latest.air_temperature is not None and (latest.air_temperature < -60 or latest.air_temperature > 60):
        return True
    if latest.wind_speed is not None and (latest.wind_speed < 0 or latest.wind_speed > 100):
        return True
    if latest.discharge is not None and latest.discharge < 0:
        return True
    if latest.groundwater_level is not None and latest.groundwater_level < -100:
        return True

    return False


def _parameter_profile_properties(capability_flags: dict[str, bool]) -> dict[str, Any]:
    count = sum(1 for value in capability_flags.values() if value)
    return {
        "available_parameter_count": count,
        "parameter_profile": _parameter_profile_name(capability_flags),
    }


def _parameter_profile_name(capability_flags: dict[str, bool]) -> str:
    has_temperature = capability_flags.get("has_air_temperature", False)
    has_precipitation = capability_flags.get("has_precipitation_1h", False)
    has_wind = capability_flags.get("has_wind_speed", False) or capability_flags.get("has_wind_from_direction", False)
    has_snow = capability_flags.get("has_snow_depth", False)

    if has_temperature and has_precipitation and has_wind and has_snow:
        return "complete"
    if has_temperature and has_precipitation and has_wind:
        return "weather"
    if has_snow:
        return "snow"
    return "lesser"


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


def _compact_latest_base_properties(station: Station, latest: StationLatest) -> dict[str, Any]:
    recent_status = _recent_status(station.last_observation_time)
    return {
        "source_id": station.source_id,
        "provider": station.provider,
        "name": station.name,
        "stationholder": station.stationholder,
        "masl": station.masl,
        "observed_at": _isoformat(latest.observed_at),
        "has_recent_data": recent_status["has_recent_data"],
        "minutes_since_observation": recent_status["minutes_since_observation"],
    }


def _compact_latest_properties(
    station: Station,
    latest: StationLatest,
    discharge_percentile: NveDischargePercentile | None = None,
    flood_threshold: NveDischargeFloodThreshold | None = None,
) -> dict[str, Any]:
    properties = {
        **_compact_latest_base_properties(station, latest),
        "air_temperature": _air_temperature_value_for_station(station, latest.air_temperature),
        "air_temperature_observed_at": _isoformat(latest.air_temperature_observed_at),
        "precipitation_1h": None if _station_precipitation_is_excluded(station) else latest.precipitation_1h,
        "precipitation_observed_at": _isoformat(latest.precipitation_observed_at),
        "is_precipitation_suspect": (latest.is_precipitation_suspect or _station_precipitation_is_excluded(station)) or None,
        "precipitation_3h": None if _station_precipitation_is_excluded(station) else latest.precipitation_3h,
        "precipitation_24h": None if _station_precipitation_is_excluded(station) else latest.precipitation_24h,
        "snow_depth": latest.snow_depth,
        "snow_depth_observed_at": _isoformat(latest.snow_depth_observed_at),
        "snow_depth_change": latest.snow_depth_change,
        "wind_from_direction": latest.wind_from_direction,
        "wind_from_direction_observed_at": _isoformat(latest.wind_from_direction_observed_at),
        "wind_speed": latest.wind_speed,
        "wind_speed_observed_at": _isoformat(latest.wind_speed_observed_at),
        "discharge": latest.discharge,
        "discharge_observed_at": _isoformat(latest.discharge_observed_at),
        "groundwater_level": latest.groundwater_level,
        "groundwater_level_observed_at": _isoformat(latest.groundwater_level_observed_at),
        **_discharge_classification_properties(station, latest, discharge_percentile, flood_threshold),
    }
    return _without_null_values(properties)


def _load_discharge_percentiles(
    session,
    rows: list[tuple[Station, StationLatest]],
) -> dict[tuple[int, str], NveDischargePercentile]:
    keys = {
        key
        for station, latest in rows
        if (key := _discharge_percentile_key(station, latest)) is not None
    }
    if not keys:
        return {}

    return _load_discharge_percentiles_for_keys(session, keys)


def _load_discharge_percentiles_for_keys(
    session,
    keys: set[tuple[int, str]],
) -> dict[tuple[int, str], NveDischargePercentile]:
    if not keys:
        return {}

    station_ids = {station_id for station_id, _date_mmdd in keys}
    date_mmdds = {date_mmdd for _station_id, date_mmdd in keys}
    percentile_rows = (
        session.execute(
            select(NveDischargePercentile).where(
                NveDischargePercentile.station_id.in_(station_ids),
                NveDischargePercentile.date_mmdd.in_(date_mmdds),
            )
        )
        .scalars()
        .all()
    )
    return {(row.station_id, row.date_mmdd): row for row in percentile_rows}


def _load_discharge_flood_thresholds(
    session,
    rows: list[tuple[Station, StationLatest]],
) -> dict[int, NveDischargeFloodThreshold]:
    station_ids = {
        station.id
        for station, _latest in rows
        if station.provider == "nve_hydapi"
    }
    if not station_ids:
        return {}
    threshold_rows = session.execute(
        select(NveDischargeFloodThreshold).where(
            NveDischargeFloodThreshold.station_id.in_(station_ids)
        )
    ).scalars().all()
    return {row.station_id: row for row in threshold_rows}


def _discharge_percentile_key(station: Station, latest: StationLatest) -> tuple[int, str] | None:
    if station.provider != "nve_hydapi" or latest.discharge is None:
        return None
    observed_at = _ensure_utc(latest.discharge_observed_at)
    if observed_at is None:
        return None
    return station.id, observed_at.strftime("%m-%d")


def _discharge_7d_percentile_key(
    station: Station,
    aggregates: dict[str, Any],
) -> tuple[int, str] | None:
    if station.provider != "nve_hydapi" or aggregates.get("discharge_max_7d") is None:
        return None
    observed_at = aggregates.get("discharge_max_7d_time")
    if not observed_at:
        return None
    return station.id, _parse_timestamp(observed_at).strftime("%m-%d")


def _discharge_classification_properties(
    station: Station,
    latest: StationLatest,
    percentile: NveDischargePercentile | None,
    flood_threshold: NveDischargeFloodThreshold | None = None,
) -> dict[str, Any]:
    observed_at = _ensure_utc(latest.discharge_observed_at)
    return _discharge_classification_for_value(
        station=station,
        discharge=latest.discharge,
        observed_at=observed_at,
        percentile=percentile,
        flood_threshold=flood_threshold,
    )


def _discharge_classification_for_value(
    station: Station,
    discharge: float | None,
    observed_at: datetime | None,
    percentile: NveDischargePercentile | None,
    flood_threshold: NveDischargeFloodThreshold | None = None,
) -> dict[str, Any]:
    observed_at = _ensure_utc(observed_at)
    age_hours = None
    if observed_at is not None:
        age_hours = round((datetime.now(timezone.utc) - observed_at).total_seconds() / 3600, 2)

    if discharge is None and station.provider != "nve_hydapi":
        return {}

    base = {
        "discharge_observed_at": _isoformat(observed_at),
        "discharge_age_hours": age_hours,
        "discharge_age_class": _discharge_age_class(age_hours),
        "discharge_flood_qm": flood_threshold.discharge_qm if flood_threshold else None,
        "discharge_flood_q5": flood_threshold.discharge_q5 if flood_threshold else None,
        "discharge_flood_q50": flood_threshold.discharge_q50 if flood_threshold else None,
        "discharge_flood_unit": flood_threshold.unit if flood_threshold else None,
        "discharge_flood_series_version": flood_threshold.series_version if flood_threshold else None,
        "discharge_flood_updated_at": _isoformat(flood_threshold.updated_at) if flood_threshold else None,
    }
    if discharge is None:
        return {
            **base,
            "discharge_class": "missing_value",
            "discharge_class_rank": 0,
            "discharge_class_source": "none",
            "discharge_value_missing": True,
            "discharge_classification_missing": None,
        }

    if station.provider == "nve_hydapi" and flood_threshold is not None:
        flood_classification = _classify_discharge_by_flood_threshold(
            discharge,
            flood_threshold,
        )
        if flood_classification is not None:
            discharge_class, rank = flood_classification
            return {
                **base,
                "discharge_class": discharge_class,
                "discharge_class_rank": rank,
                "discharge_class_source": "flood_threshold",
                "discharge_value_missing": None,
                "discharge_classification_missing": None,
            }

    if station.provider != "nve_hydapi" or percentile is None:
        return {
            **base,
            "discharge_class": "missing_classification",
            "discharge_class_rank": 10,
            "discharge_class_source": "latest_value_only",
            "discharge_value_missing": None,
            "discharge_classification_missing": True,
        }

    discharge_class, rank = _classify_discharge_by_percentile(discharge, percentile)
    classification_missing = discharge_class == "missing_classification"
    return {
        **base,
        "discharge_class": discharge_class,
        "discharge_class_rank": rank,
        "discharge_class_source": "percentile" if not classification_missing else "latest_value_only",
        "discharge_value_missing": None,
        "discharge_classification_missing": classification_missing or None,
        "discharge_percentile_date": percentile.date_mmdd,
        "discharge_perc25": percentile.perc25,
        "discharge_perc60": percentile.perc60,
        "discharge_perc75": percentile.perc75,
        "discharge_perc90": percentile.perc90,
        "discharge_perc95": percentile.perc95,
    }


def _classify_discharge_by_percentile(
    value: float,
    percentile: NveDischargePercentile,
) -> tuple[str, int]:
    if (
        percentile.perc25 is None
        or percentile.perc60 is None
        or percentile.perc75 is None
        or percentile.perc90 is None
    ):
        return "missing_classification", 10
    if value >= percentile.perc90:
        return "high_plus", 60
    if value >= percentile.perc75:
        return "high", 50
    if value >= percentile.perc60:
        return "high_minus", 40
    if value >= percentile.perc25:
        return "normal", 30
    return "low", 20


def _classify_discharge_by_flood_threshold(
    value: float,
    threshold: NveDischargeFloodThreshold,
) -> tuple[str, int] | None:
    if threshold.discharge_q50 is not None and value >= threshold.discharge_q50:
        return "flood_over_50y", 90
    if threshold.discharge_q5 is not None and value >= threshold.discharge_q5:
        return "flood_5y_to_50y", 80
    if threshold.discharge_qm is not None and value >= threshold.discharge_qm:
        return "flood_mean_to_5y", 70
    return None


def _discharge_age_class(age_hours: float | None) -> str:
    if age_hours is None:
        return "missing"
    if age_hours <= 4:
        return "fresh"
    if age_hours <= 24:
        return "stale_4_24h"
    return "stale_over_24h"


def _seven_day_aggregates_by_station(
    observation_rows: list[Observation],
    stations_by_id: dict[int, Station],
) -> dict[int, dict[str, Any]]:
    element_to_parameter = {
        element_id: parameter_id
        for parameter_id, element_ids in SEVEN_DAY_AGGREGATE_ELEMENT_MAP.items()
        for element_id in element_ids
    }
    aggregates: dict[int, dict[str, Any]] = {}
    max_rows: dict[tuple[int, str], Observation] = {}
    precipitation_totals: dict[int, float] = {}

    for row in observation_rows:
        if row.value is None:
            continue

        parameter_id = element_to_parameter.get(row.element_id)
        if parameter_id is None:
            continue

        station = stations_by_id.get(row.station_id)
        if parameter_id == "air_temperature" and _is_suspect_road_station_temperature(station, row.value):
            continue

        if parameter_id == "precipitation_1h":
            if _is_precipitation_observation_suspect(station, row.value):
                continue
            precipitation_totals[row.station_id] = precipitation_totals.get(row.station_id, 0.0) + row.value

        key = (row.station_id, parameter_id)
        current = max_rows.get(key)
        if current is None or row.value > (current.value if current.value is not None else float("-inf")):
            max_rows[key] = row

    for (station_id, parameter_id), row in max_rows.items():
        station_aggregates = aggregates.setdefault(station_id, {})
        station_aggregates[f"{parameter_id}_max_7d"] = row.value
        station_aggregates[f"{parameter_id}_max_7d_time"] = _isoformat(row.reference_time)

    for station_id, value in precipitation_totals.items():
        aggregates.setdefault(station_id, {})["precipitation_7d_accumulated"] = round(value, 3)

    return aggregates


def _is_precipitation_observation_suspect(station: Station | None, value: float | None) -> bool:
    if value is None or value < 0:
        return True
    if _station_precipitation_is_excluded(station):
        return True

    stationholder = (station.stationholder or "").lower() if station else ""
    is_strict_provider = bool(
        station
        and (
            station.provider == "nve_hydapi"
            or "vegvesen" in stationholder
            or stationholder == "svv"
        )
    )
    if is_strict_provider:
        return value > 5

    return value > 300


def _station_precipitation_is_excluded(station: Station | None) -> bool:
    return bool(station and station.source_id in EXCLUDED_PRECIPITATION_SOURCE_IDS)


def _air_temperature_value_for_station(station: Station, value: float | None) -> float | None:
    if _is_suspect_road_station_temperature(station, value):
        return None
    return value


def _is_suspect_road_station_temperature(station: Station | None, value: float | None) -> bool:
    return bool(
        station
        and value is not None
        and _is_road_stationholder(station.stationholder)
        and -40.5 <= value <= -39.5
    )


def _is_road_stationholder(stationholder: str | None) -> bool:
    if not stationholder:
        return False
    normalized = stationholder.casefold()
    return "statens vegvesen" in normalized or "svv" in normalized


def _without_null_values(properties: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in properties.items() if value is not None}


def _latest_properties_for_station(station: Station, latest: StationLatest) -> dict[str, Any]:
    properties = _latest_properties(latest)
    if _is_suspect_road_station_temperature(station, latest.air_temperature):
        properties["air_temperature"] = None
        properties["air_temperature_unit"] = None
    if _is_suspect_road_station_temperature(station, latest.air_temperature_min):
        properties["air_temperature_min"] = None
        properties["air_temperature_min_unit"] = None
        properties["air_temperature_min_time"] = None
    if _is_suspect_road_station_temperature(station, latest.air_temperature_max):
        properties["air_temperature_max"] = None
        properties["air_temperature_max_unit"] = None
        properties["air_temperature_max_time"] = None
    if not _station_precipitation_is_excluded(station):
        return properties

    for key in [
        "precipitation_1h",
        "precipitation_1h_unit",
        "precipitation_1h_max",
        "precipitation_1h_max_unit",
        "precipitation_1h_max_period",
        "precipitation_3h",
        "precipitation_3h_unit",
        "precipitation_3h_max",
        "precipitation_3h_max_unit",
        "precipitation_3h_max_period",
        "precipitation_24h",
        "precipitation_24h_unit",
    ]:
        properties[key] = None
    properties["is_precipitation_suspect"] = True
    return properties


def _latest_properties(latest: StationLatest) -> dict[str, Any]:
    return {
        "observed_at": _isoformat(latest.observed_at),
        "air_temperature": latest.air_temperature,
        "air_temperature_unit": latest.air_temperature_unit,
        "air_temperature_observed_at": _isoformat(latest.air_temperature_observed_at),
        "air_temperature_min": latest.air_temperature_min,
        "air_temperature_min_unit": latest.air_temperature_min_unit,
        "air_temperature_min_time": latest.air_temperature_min_time,
        "air_temperature_max": latest.air_temperature_max,
        "air_temperature_max_unit": latest.air_temperature_max_unit,
        "air_temperature_max_time": latest.air_temperature_max_time,
        "precipitation_1h": latest.precipitation_1h,
        "precipitation_1h_unit": latest.precipitation_1h_unit,
        "precipitation_observed_at": _isoformat(latest.precipitation_observed_at),
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
        "snow_depth_observed_at": _isoformat(latest.snow_depth_observed_at),
        "snow_depth_change": latest.snow_depth_change,
        "snow_depth_change_unit": latest.snow_depth_change_unit,
        "wind_from_direction": latest.wind_from_direction,
        "wind_from_direction_unit": latest.wind_from_direction_unit,
        "wind_from_direction_observed_at": _isoformat(latest.wind_from_direction_observed_at),
        "wind_from_direction_max": latest.wind_from_direction_max,
        "wind_from_direction_max_unit": latest.wind_from_direction_max_unit,
        "wind_speed": latest.wind_speed,
        "wind_speed_unit": latest.wind_speed_unit,
        "wind_speed_observed_at": _isoformat(latest.wind_speed_observed_at),
        "wind_speed_max": latest.wind_speed_max,
        "wind_speed_max_unit": latest.wind_speed_max_unit,
        "wind_speed_max_time": latest.wind_speed_max_time,
        "discharge": latest.discharge,
        "discharge_unit": latest.discharge_unit,
        "discharge_observed_at": _isoformat(latest.discharge_observed_at),
        "groundwater_level": latest.groundwater_level,
        "groundwater_level_unit": latest.groundwater_level_unit,
        "groundwater_level_observed_at": _isoformat(latest.groundwater_level_observed_at),
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
