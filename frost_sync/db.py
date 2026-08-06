from __future__ import annotations

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker


class Base(DeclarativeBase):
    pass


def create_session_factory(database_url: str) -> sessionmaker[Session]:
    engine = create_engine(database_url, future=True, **_engine_kwargs(database_url))
    return sessionmaker(bind=engine, future=True, expire_on_commit=False)


def create_schema(database_url: str) -> None:
    import frost_sync.models  # noqa: F401

    engine = create_engine(database_url, future=True, **_engine_kwargs(database_url))
    Base.metadata.create_all(engine)
    upgrade_schema(database_url)


def upgrade_schema(database_url: str) -> None:
    engine = create_engine(database_url, future=True, **_engine_kwargs(database_url))
    inspector = inspect(engine)
    tables = set(inspector.get_table_names())
    ddl_statements: list[str] = []

    if "stations" in tables:
        station_columns = {column["name"] for column in inspector.get_columns("stations")}
        if "provider" not in station_columns:
            ddl_statements.append("ALTER TABLE stations ADD COLUMN provider VARCHAR(32) NOT NULL DEFAULT 'frost'")
        if "provider_context" not in station_columns:
            ddl_statements.append("ALTER TABLE stations ADD COLUMN provider_context VARCHAR(2048)")
        if "stationholder" not in station_columns:
            ddl_statements.append("ALTER TABLE stations ADD COLUMN stationholder VARCHAR(512)")

    if "nve_discharge_percentiles" in tables:
        percentile_columns = {
            column["name"] for column in inspector.get_columns("nve_discharge_percentiles")
        }
        if "perc60" not in percentile_columns:
            ddl_statements.append("ALTER TABLE nve_discharge_percentiles ADD COLUMN perc60 FLOAT")

    if "station_latest" not in tables:
        with engine.begin() as connection:
            for ddl in ddl_statements:
                connection.execute(text(ddl))
        return

    columns = {column["name"] for column in inspector.get_columns("station_latest")}

    expected_columns = {
        "air_temperature_observed_at": "DATETIME",
        "air_temperature_min": "FLOAT",
        "air_temperature_min_unit": "VARCHAR(64)",
        "air_temperature_min_time": "VARCHAR(64)",
        "air_temperature_max": "FLOAT",
        "air_temperature_max_unit": "VARCHAR(64)",
        "air_temperature_max_time": "VARCHAR(64)",
        "precipitation_1h_max": "FLOAT",
        "precipitation_observed_at": "DATETIME",
        "precipitation_1h_max_unit": "VARCHAR(64)",
        "precipitation_1h_max_period": "VARCHAR(128)",
        "is_precipitation_suspect": "BOOLEAN NOT NULL DEFAULT 0",
        "precipitation_3h": "FLOAT",
        "precipitation_3h_unit": "VARCHAR(64)",
        "precipitation_3h_max": "FLOAT",
        "precipitation_3h_max_unit": "VARCHAR(64)",
        "precipitation_3h_max_period": "VARCHAR(128)",
        "precipitation_24h": "FLOAT",
        "precipitation_24h_unit": "VARCHAR(64)",
        "snow_depth_change": "FLOAT",
        "snow_depth_observed_at": "DATETIME",
        "snow_depth_change_unit": "VARCHAR(64)",
        "wind_speed_max": "FLOAT",
        "wind_speed_observed_at": "DATETIME",
        "wind_speed_max_unit": "VARCHAR(64)",
        "wind_speed_max_time": "VARCHAR(64)",
        "wind_from_direction_max": "FLOAT",
        "wind_from_direction_observed_at": "DATETIME",
        "wind_from_direction_max_unit": "VARCHAR(64)",
        "discharge": "FLOAT",
        "discharge_unit": "VARCHAR(64)",
        "discharge_observed_at": "DATETIME",
        "groundwater_level": "FLOAT",
        "groundwater_level_unit": "VARCHAR(64)",
        "groundwater_level_observed_at": "DATETIME",
    }

    for column_name, column_type in expected_columns.items():
        if column_name not in columns:
            ddl_statements.append(f"ALTER TABLE station_latest ADD COLUMN {column_name} {column_type}")

    with engine.begin() as connection:
        for ddl in ddl_statements:
            connection.execute(text(ddl))
        _backfill_latest_observation_times(connection)


def _backfill_latest_observation_times(connection) -> None:
    element_groups = {
        "air_temperature_observed_at": ("air_temperature", "air_temperature_unit", ("air_temperature",)),
        "precipitation_observed_at": (
            "precipitation_1h",
            "precipitation_1h_unit",
            ("sum(precipitation_amount PT1H)", "precipitation_1h"),
        ),
        "snow_depth_observed_at": ("snow_depth", "snow_depth_unit", ("snow_depth", "surface_snow_thickness")),
        "wind_from_direction_observed_at": (
            "wind_from_direction",
            "wind_from_direction_unit",
            ("wind_from_direction",),
        ),
        "wind_speed_observed_at": ("wind_speed", "wind_speed_unit", ("wind_speed",)),
        "discharge_observed_at": ("discharge", "discharge_unit", ("discharge",)),
        "groundwater_level_observed_at": (
            "groundwater_level",
            "groundwater_level_unit",
            ("groundwater_level",),
        ),
    }
    for column_name, (value_field, unit_field, element_ids) in element_groups.items():
        placeholders = ", ".join(f":element_{index}" for index in range(len(element_ids)))
        parameters = {f"element_{index}": element_id for index, element_id in enumerate(element_ids)}
        backfill_condition = f"{column_name} IS NULL"
        if column_name == "precipitation_observed_at":
            backfill_condition += " AND is_precipitation_suspect = 0"
        latest_row_query = (
            "SELECT observations.{selected_field} FROM observations "
            "WHERE observations.station_id = station_latest.station_id "
            f"AND observations.element_id IN ({placeholders}) "
            "ORDER BY observations.reference_time DESC, observations.id DESC LIMIT 1"
        )
        for target_field, selected_field in (
            (value_field, "value"),
            (unit_field, "unit"),
            (column_name, "reference_time"),
        ):
            connection.execute(
                text(
                    f"UPDATE station_latest SET {target_field} = ("
                    f"{latest_row_query.format(selected_field=selected_field)}"
                    f") WHERE {backfill_condition}"
                ),
                parameters,
            )


def _engine_kwargs(database_url: str) -> dict:
    if database_url.startswith("sqlite"):
        return {"connect_args": {"check_same_thread": False}}
    if database_url.startswith("mysql"):
        return {"pool_recycle": 280, "pool_pre_ping": True}
    return {}
