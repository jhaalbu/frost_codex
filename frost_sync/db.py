from __future__ import annotations

from sqlalchemy import create_engine
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


def _engine_kwargs(database_url: str) -> dict:
    if database_url.startswith("sqlite"):
        return {"connect_args": {"check_same_thread": False}}
    if database_url.startswith("mysql"):
        return {"pool_recycle": 280, "pool_pre_ping": True}
    return {}
