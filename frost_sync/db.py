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
    if "station_latest" not in tables:
        return

    columns = {column["name"] for column in inspector.get_columns("station_latest")}
    ddl_statements: list[str] = []

    if "precipitation_24h" not in columns:
        ddl_statements.append("ALTER TABLE station_latest ADD COLUMN precipitation_24h FLOAT")
    if "precipitation_24h_unit" not in columns:
        ddl_statements.append("ALTER TABLE station_latest ADD COLUMN precipitation_24h_unit VARCHAR(64)")

    if not ddl_statements:
        return

    with engine.begin() as connection:
        for ddl in ddl_statements:
            connection.execute(text(ddl))


def _engine_kwargs(database_url: str) -> dict:
    if database_url.startswith("sqlite"):
        return {"connect_args": {"check_same_thread": False}}
    if database_url.startswith("mysql"):
        return {"pool_recycle": 280, "pool_pre_ping": True}
    return {}
