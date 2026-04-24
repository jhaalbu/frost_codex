from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Any

import requests
from flask import Blueprint, Flask, abort, jsonify, request
from sqlalchemy import and_, select

from frost_sync.avalanche import (
    AvalancheWeatherSnapshot,
    NveGtsClient,
    NvdbClient,
    build_avalanche_risk_payload,
    summarize_backtest,
)
from frost_sync.config import load_settings
from frost_sync.db import create_session_factory
from frost_sync.models import Observation, Station, StationCapability, StationLatest


CAPABILITY_FLAG_MAP = {
    "air_temperature": "has_air_temperature",
    "sum(precipitation_amount PT1H)": "has_precipitation_1h",
    "snow_depth": "has_snow_depth",
    "surface_snow_thickness": "has_snow_depth",
    "wind_from_direction": "has_wind_from_direction",
    "wind_speed": "has_wind_speed",
}

PRECIPITATION_ELEMENT = "sum(precipitation_amount PT1H)"
AIR_TEMPERATURE_ELEMENT = "air_temperature"
SNOW_DEPTH_ELEMENT_IDS = {"snow_depth", "surface_snow_thickness"}
WIND_DIRECTION_ELEMENT = "wind_from_direction"
WIND_SPEED_ELEMENT = "wind_speed"


def create_app() -> Flask:
    app = Flask(__name__)
    app.register_blueprint(create_blueprint())
    return app


def create_blueprint(name: str = "frost_sync") -> Blueprint:
    settings = load_settings()
    session_factory = create_session_factory(settings.database_url)
    nvdb_client = NvdbClient(timeout_seconds=settings.request_timeout_seconds)
    gts_client = NveGtsClient(timeout_seconds=settings.request_timeout_seconds)
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

    @blueprint.get("/api/avalanche-risk")
    def avalanche_risk() -> Any:
        road = (request.args.get("road") or "").strip()
        segment = (request.args.get("segment") or "").strip()
        station_source_id = (request.args.get("station") or "").strip()
        if not road or not segment or not station_source_id:
            abort(400, description="Use ?road=RV5&segment=S8D1&station=SN55740")

        gts_x = request.args.get("x", type=int)
        gts_y = request.args.get("y", type=int)
        if (gts_x is None) != (gts_y is None):
            abort(400, description="Use both ?x=... and ?y=... for optional NVE GridTimeSeries data")
        as_of = _parse_optional_timestamp(request.args.get("as_of"))
        start_date = _parse_optional_date(request.args.get("start_date"), default=date.today() - timedelta(days=7))
        end_date = _parse_optional_date(request.args.get("end_date"), default=date.today())
        if as_of is not None:
            default_gts_end = as_of.date()
            default_gts_start = default_gts_end - timedelta(days=7)
            if "start_date" not in request.args:
                start_date = default_gts_start
            if "end_date" not in request.args:
                end_date = default_gts_end

        with session_factory() as session:
            row = (
                session.execute(
                    select(Station, StationLatest)
                    .outerjoin(StationLatest, StationLatest.station_id == Station.id)
                    .where(Station.source_id == station_source_id)
                )
                .one_or_none()
            )
            if row is None:
                abort(404, description=f"Unknown station: {station_source_id}")
            station, latest = row
            if as_of is not None:
                latest = _load_station_snapshot_as_of(session, station, as_of)

        try:
            events = nvdb_client.fetch_snow_avalanche_events(road=road, segment=segment)
            gts_data = None
            gts_skipped_themes = []
            if gts_x is not None and gts_y is not None:
                gts_data, gts_skipped_themes = gts_client.fetch_latest_values(
                    x=gts_x,
                    y=gts_y,
                    start_date=start_date,
                    end_date=end_date,
                )
        except requests.RequestException as exc:
            abort(502, description=f"Failed to fetch external avalanche data: {exc}")
        except ValueError as exc:
            abort(400, description=str(exc))

        return jsonify(
            build_avalanche_risk_payload(
                station=station,
                latest=latest,
                events=events,
                road=road,
                segment=segment,
                gts_data=gts_data,
                gts_skipped_themes=gts_skipped_themes,
                assessment_time=as_of,
            )
        )

    @blueprint.get("/api/avalanche-risk/debug/nvdb")
    def avalanche_risk_debug_nvdb() -> Any:
        road = (request.args.get("road") or "").strip()
        segment = (request.args.get("segment") or "").strip()
        if not road or not segment:
            abort(400, description="Use ?road=RV5&segment=S8D1")

        max_pages = request.args.get("max_pages", default=1, type=int)
        max_pages = max(1, min(max_pages, 5))

        try:
            payload = nvdb_client.fetch_raw_avalanche_objects(
                road=road,
                segment=segment,
                max_pages=max_pages,
            )
        except requests.RequestException as exc:
            abort(502, description=f"Failed to fetch NVDB avalanche data: {exc}")

        return jsonify(payload)

    @blueprint.get("/api/avalanche-risk/backtest")
    def avalanche_risk_backtest() -> Any:
        road = (request.args.get("road") or "").strip()
        segment = (request.args.get("segment") or "").strip()
        station_source_id = (request.args.get("station") or "").strip()
        if not road or not segment or not station_source_id:
            abort(400, description="Use ?road=RV5&segment=S8D1&station=SN55740")

        from_dt = _parse_timestamp(request.args.get("from")) if request.args.get("from") else None
        to_dt = _parse_timestamp(request.args.get("to")) if request.args.get("to") else None
        if from_dt is None or to_dt is None:
            abort(400, description="Use both ?from=... and ?to=... for backtesting")
        if from_dt > to_dt:
            abort(400, description="?from must be earlier than or equal to ?to")

        step_hours = request.args.get("step_hours", default=24, type=int)
        event_window_hours = request.args.get("event_window_hours", default=24, type=int)
        gts_x = request.args.get("x", type=int)
        gts_y = request.args.get("y", type=int)
        if (gts_x is None) != (gts_y is None):
            abort(400, description="Use both ?x=... and ?y=... for optional NVE GridTimeSeries data")
        if step_hours < 1 or step_hours > 168:
            abort(400, description="step_hours must be between 1 and 168")
        if event_window_hours < 1 or event_window_hours > 168:
            abort(400, description="event_window_hours must be between 1 and 168")

        with session_factory() as session:
            row = (
                session.execute(
                    select(Station).where(Station.source_id == station_source_id)
                )
                .scalar_one_or_none()
            )
            if row is None:
                abort(404, description=f"Unknown station: {station_source_id}")
            station = row

        try:
            events = nvdb_client.fetch_snow_avalanche_events(road=road, segment=segment)
        except requests.RequestException as exc:
            abort(502, description=f"Failed to fetch NVDB avalanche data: {exc}")
        except RuntimeError as exc:
            abort(502, description=str(exc))

        timestamps = list(_time_range(from_dt, to_dt, step_hours))
        series: list[dict[str, Any]] = []
        with session_factory() as session:
            for as_of in timestamps:
                snapshot = _load_station_snapshot_as_of(session, station, as_of)
                gts_data = None
                gts_skipped_themes: list[dict[str, str]] = []
                if gts_x is not None and gts_y is not None:
                    try:
                        gts_data, gts_skipped_themes = gts_client.fetch_latest_values(
                            x=gts_x,
                            y=gts_y,
                            start_date=as_of.date() - timedelta(days=7),
                            end_date=as_of.date(),
                        )
                    except requests.RequestException as exc:
                        gts_skipped_themes = [{"theme": "all", "status": "request_error", "details": str(exc)}]

                payload = build_avalanche_risk_payload(
                    station=station,
                    latest=snapshot,
                    events=events,
                    road=road,
                    segment=segment,
                    gts_data=gts_data,
                    gts_skipped_themes=gts_skipped_themes,
                    assessment_time=as_of,
                )
                payload["event_match"] = _has_event_near_timestamp(events, as_of, event_window_hours)
                series.append(payload)

        return jsonify(
            {
                "road": road,
                "segment": segment,
                "station": station_source_id,
                "from": _isoformat(from_dt),
                "to": _isoformat(to_dt),
                "step_hours": step_hours,
                "event_window_hours": event_window_hours,
                "summary": summarize_backtest(series, events, event_window_hours),
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


def _parse_optional_date(value: str | None, default: date) -> date:
    if not value:
        return default
    try:
        return date.fromisoformat(value)
    except ValueError:
        abort(400, description=f"Invalid date: {value}")


def _parse_optional_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    return _parse_timestamp(value)


def _load_station_snapshot_as_of(
    session,
    station: Station,
    as_of: datetime,
) -> AvalancheWeatherSnapshot | None:
    lookback_start = as_of - timedelta(days=14)
    rows = (
        session.execute(
            select(Observation)
            .where(
                and_(
                    Observation.station_id == station.id,
                    Observation.reference_time >= lookback_start,
                    Observation.reference_time <= as_of,
                    Observation.element_id.in_(
                        [
                            AIR_TEMPERATURE_ELEMENT,
                            PRECIPITATION_ELEMENT,
                            *sorted(SNOW_DEPTH_ELEMENT_IDS),
                            WIND_DIRECTION_ELEMENT,
                            WIND_SPEED_ELEMENT,
                        ]
                    ),
                )
            )
            .order_by(Observation.reference_time)
        )
        .scalars()
        .all()
    )
    if not rows:
        return None

    latest_by_element = _latest_observation_by_element(rows)
    rows_24h = [
        row
        for row in rows
        if _ensure_utc(row.reference_time) is not None
        and _ensure_utc(row.reference_time) > as_of - timedelta(hours=24)
    ]
    precipitation_rows = [
        row for row in rows_24h if row.element_id == PRECIPITATION_ELEMENT and row.value is not None
    ]
    temperature_rows = [
        row for row in rows_24h if row.element_id == AIR_TEMPERATURE_ELEMENT and row.value is not None
    ]
    snow_depth_rows = [
        row for row in rows_24h if row.element_id in SNOW_DEPTH_ELEMENT_IDS and row.value is not None
    ]
    wind_speed_rows = [
        row for row in rows_24h if row.element_id == WIND_SPEED_ELEMENT and row.value is not None
    ]
    wind_direction_by_time = {
        _ensure_utc(row.reference_time): row
        for row in rows_24h
        if row.element_id == WIND_DIRECTION_ELEMENT and row.value is not None
    }

    latest_air = latest_by_element.get(AIR_TEMPERATURE_ELEMENT)
    latest_precip = latest_by_element.get(PRECIPITATION_ELEMENT)
    latest_wind_speed = latest_by_element.get(WIND_SPEED_ELEMENT)
    latest_wind_direction = latest_by_element.get(WIND_DIRECTION_ELEMENT)
    latest_snow = _latest_snow_row(rows)
    wind_speed_max_row = _max_observation_row(wind_speed_rows)
    wind_direction_max = None
    if wind_speed_max_row is not None:
        direction_row = wind_direction_by_time.get(_ensure_utc(wind_speed_max_row.reference_time))
        wind_direction_max = direction_row.value if direction_row else None

    return AvalancheWeatherSnapshot(
        observed_at=_snapshot_observed_at(
            latest_air,
            latest_precip,
            latest_wind_speed,
            latest_wind_direction,
            latest_snow,
        ),
        air_temperature=latest_air.value if latest_air else None,
        precipitation_3h=_rolling_sum_for_window(precipitation_rows, as_of, hours=3),
        precipitation_24h=_sum_observations(precipitation_rows),
        snow_depth=latest_snow.value if latest_snow else None,
        snow_depth_change=_snow_depth_change(snow_depth_rows),
        wind_speed=latest_wind_speed.value if latest_wind_speed else None,
        wind_speed_max=wind_speed_max_row.value if wind_speed_max_row else None,
        wind_from_direction=latest_wind_direction.value if latest_wind_direction else None,
        wind_from_direction_max=wind_direction_max,
    )


def _load_capabilities(session) -> dict[int, dict[str, bool]]:
    rows = session.execute(select(StationCapability)).scalars().all()
    capabilities: dict[int, dict[str, bool]] = {}
    for row in rows:
        flags = capabilities.setdefault(row.station_id, _empty_capability_flags())
        flag_name = CAPABILITY_FLAG_MAP.get(row.element_id)
        if flag_name:
            flags[flag_name] = row.available
    return capabilities


def _latest_observation_by_element(rows: list[Observation]) -> dict[str, Observation]:
    latest: dict[str, Observation] = {}
    for row in rows:
        current = latest.get(row.element_id)
        row_time = _ensure_utc(row.reference_time)
        current_time = _ensure_utc(current.reference_time) if current else None
        if current is None:
            latest[row.element_id] = row
            continue
        if row_time is not None and (current_time is None or row_time >= current_time):
            latest[row.element_id] = row
    return latest


def _latest_snow_row(rows: list[Observation]) -> Observation | None:
    snow_rows = [row for row in rows if row.element_id in SNOW_DEPTH_ELEMENT_IDS and row.value is not None]
    if not snow_rows:
        return None
    snow_rows.sort(key=lambda row: _ensure_utc(row.reference_time) or datetime.min.replace(tzinfo=timezone.utc))
    return snow_rows[-1]


def _max_observation_row(rows: list[Observation]) -> Observation | None:
    candidates = [row for row in rows if row.value is not None]
    if not candidates:
        return None
    return max(
        candidates,
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


def _sum_observations(rows: list[Observation]) -> float | None:
    values = [row.value for row in rows if row.value is not None]
    return float(sum(values)) if values else None


def _snow_depth_change(rows: list[Observation]) -> float | None:
    if len(rows) < 2:
        return None
    ordered = sorted(
        rows,
        key=lambda row: _ensure_utc(row.reference_time) or datetime.min.replace(tzinfo=timezone.utc),
    )
    first_row = ordered[0]
    last_row = ordered[-1]
    if first_row.value is None or last_row.value is None:
        return None
    return last_row.value - first_row.value


def _snapshot_observed_at(*rows: Observation | None) -> datetime | None:
    timestamps = [_ensure_utc(row.reference_time) for row in rows if row is not None]
    if not timestamps:
        return None
    return max(timestamps)


def _time_range(start: datetime, end: datetime, step_hours: int) -> list[datetime]:
    values: list[datetime] = []
    current = start
    while current <= end:
        values.append(current)
        current = current + timedelta(hours=step_hours)
    return values


def _has_event_near_timestamp(events, as_of: datetime, event_window_hours: int) -> bool:
    event_window = timedelta(hours=event_window_hours)
    as_of_date = as_of.date()
    for event in events:
        event_date = _parse_event_date(event.event_date)
        if event_date is None:
            continue
        event_dt = datetime.combine(event_date, time.min, tzinfo=timezone.utc)
        if abs(event_dt - as_of) <= event_window:
            return True
        if event_date == as_of_date:
            return True
    return False


def _parse_event_date(value: str | None) -> date | None:
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
    }
    return alias_map.get(element_id, "")


def _matches_has_filter(has_filter: str, capability_flags: dict[str, bool], latest: StationLatest) -> bool:
    flag_name = _capability_flag_name(has_filter)
    if not flag_name:
        return False

    if flag_name == "has_snow_depth":
        return capability_flags.get(flag_name, False) and latest.snow_depth is not None

    return capability_flags.get(flag_name, False)


def _station_properties(station: Station) -> dict[str, Any]:
    return {
        "source_id": station.source_id,
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
