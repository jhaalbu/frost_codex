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
        if "stationholder" not in station_columns:
            ddl_statements.append("ALTER TABLE stations ADD COLUMN stationholder VARCHAR(512)")

    if "station_latest" not in tables:
        with engine.begin() as connection:
            for ddl in ddl_statements:
                connection.execute(text(ddl))
        return

    columns = {column["name"] for column in inspector.get_columns("station_latest")}

    expected_columns = {
        "air_temperature_min": "FLOAT",
        "air_temperature_min_unit": "VARCHAR(64)",
        "air_temperature_max": "FLOAT",
        "air_temperature_max_unit": "VARCHAR(64)",
        "air_temperature_max_time": "VARCHAR(64)",
        "precipitation_1h_max": "FLOAT",
        "precipitation_1h_max_unit": "VARCHAR(64)",
        "precipitation_1h_max_period": "VARCHAR(128)",
        "precipitation_3h": "FLOAT",
        "precipitation_3h_unit": "VARCHAR(64)",
        "precipitation_3h_max": "FLOAT",
        "precipitation_3h_max_unit": "VARCHAR(64)",
        "precipitation_3h_max_period": "VARCHAR(128)",
        "precipitation_24h": "FLOAT",
        "precipitation_24h_unit": "VARCHAR(64)",
        "snow_depth_change": "FLOAT",
        "snow_depth_change_unit": "VARCHAR(64)",
        "wind_speed_max": "FLOAT",
        "wind_speed_max_unit": "VARCHAR(64)",
        "wind_speed_max_time": "VARCHAR(64)",
        "wind_from_direction_max": "FLOAT",
        "wind_from_direction_max_unit": "VARCHAR(64)",
    }

    for column_name, column_type in expected_columns.items():
        if column_name not in columns:
            ddl_statements.append(f"ALTER TABLE station_latest ADD COLUMN {column_name} {column_type}")

    with engine.begin() as connection:
        for ddl in ddl_statements:
            connection.execute(text(ddl))


def _engine_kwargs(database_url: str) -> dict:
    if database_url.startswith("sqlite"):
        return {"connect_args": {"check_same_thread": False}}
    if database_url.startswith("mysql"):
        return {"pool_recycle": 280, "pool_pre_ping": True}
    return {}
