from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from frost_sync.db import Base


class Station(Base):
    __tablename__ = "stations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    name: Mapped[Optional[str]] = mapped_column(String(255))
    country: Mapped[Optional[str]] = mapped_column(String(8), index=True)
    county: Mapped[Optional[str]] = mapped_column(String(255))
    municipality: Mapped[Optional[str]] = mapped_column(String(255))
    masl: Mapped[Optional[float]] = mapped_column(Float)
    longitude: Mapped[Optional[float]] = mapped_column(Float, index=True)
    latitude: Mapped[Optional[float]] = mapped_column(Float, index=True)
    valid_from: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    valid_to: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_observation_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), index=True)

    capabilities: Mapped[list["StationCapability"]] = relationship(back_populates="station", cascade="all, delete-orphan")
    observations: Mapped[list["Observation"]] = relationship(back_populates="station", cascade="all, delete-orphan")
    latest: Mapped[Optional["StationLatest"]] = relationship(
        back_populates="station",
        cascade="all, delete-orphan",
        uselist=False,
    )


class StationCapability(Base):
    __tablename__ = "station_capabilities"
    __table_args__ = (UniqueConstraint("station_id", "element_id", name="uq_station_capability"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    station_id: Mapped[int] = mapped_column(ForeignKey("stations.id"), index=True)
    element_id: Mapped[str] = mapped_column(String(128), index=True)
    available: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    station: Mapped[Station] = relationship(back_populates="capabilities")


class Observation(Base):
    __tablename__ = "observations"
    __table_args__ = (UniqueConstraint("station_id", "reference_time", "element_id", name="uq_observation"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    station_id: Mapped[int] = mapped_column(ForeignKey("stations.id"), index=True)
    reference_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    element_id: Mapped[str] = mapped_column(String(128), index=True)
    value: Mapped[Optional[float]] = mapped_column(Float)
    unit: Mapped[Optional[str]] = mapped_column(String(64))
    time_offset: Mapped[Optional[str]] = mapped_column(String(64))
    level: Mapped[Optional[str]] = mapped_column(String(255))
    quality_code: Mapped[Optional[int]] = mapped_column(Integer)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    station: Mapped[Station] = relationship(back_populates="observations")


class StationLatest(Base):
    __tablename__ = "station_latest"

    station_id: Mapped[int] = mapped_column(ForeignKey("stations.id"), primary_key=True)
    source_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    observed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), index=True)
    air_temperature: Mapped[Optional[float]] = mapped_column(Float)
    air_temperature_unit: Mapped[Optional[str]] = mapped_column(String(64))
    precipitation_1h: Mapped[Optional[float]] = mapped_column(Float)
    precipitation_1h_unit: Mapped[Optional[str]] = mapped_column(String(64))
    snow_depth: Mapped[Optional[float]] = mapped_column(Float)
    snow_depth_unit: Mapped[Optional[str]] = mapped_column(String(64))
    wind_from_direction: Mapped[Optional[float]] = mapped_column(Float)
    wind_from_direction_unit: Mapped[Optional[str]] = mapped_column(String(64))
    wind_speed: Mapped[Optional[float]] = mapped_column(Float)
    wind_speed_unit: Mapped[Optional[str]] = mapped_column(String(64))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    station: Mapped[Station] = relationship(back_populates="latest")
