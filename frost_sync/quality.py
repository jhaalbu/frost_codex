"""Shared checks for incoming measurements and previously stored data."""
from datetime import datetime, timezone
import math


TEMPERATURE_ELEMENTS = {"air_temperature", "mean(air_temperature PT1H)"}


def utc(value):
    if value is None:
        return None
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def valid_temperature(value) -> bool:
    return value is not None and math.isfinite(value) and -60 <= value <= 60


def valid_observation(row, now: datetime) -> bool:
    observed_at = utc(row.reference_time)
    return (
        observed_at is not None
        and observed_at <= now
        and (row.element_id not in TEMPERATURE_ELEMENTS or valid_temperature(row.value))
    )


def sanitize_latest(latest, now: datetime) -> None:
    """Clear invalid cached values without deleting observation history.

    A bad station-wide timestamp must not prevent valid incoming values from
    becoming latest. Recover it from valid per-parameter timestamps instead.
    """
    fields = {
        "air_temperature": "air_temperature_observed_at",
        "precipitation_1h": "precipitation_observed_at",
        "snow_depth": "snow_depth_observed_at",
        "wind_speed": "wind_speed_observed_at",
        "wind_from_direction": "wind_from_direction_observed_at",
        "discharge": "discharge_observed_at",
        "groundwater_level": "groundwater_level_observed_at",
    }
    bad_overall = utc(latest.observed_at) is not None and utc(latest.observed_at) > now
    invalidated = False
    times = []
    for field, time_field in fields.items():
        stamp = utc(getattr(latest, time_field))
        value = getattr(latest, field)
        bad = (stamp is not None and stamp > now) or (
            field == "air_temperature" and value is not None and not valid_temperature(value)
        )
        if bad:
            setattr(latest, field, None)
            setattr(latest, field + "_unit", None)
            setattr(latest, time_field, None)
            invalidated = True
        elif stamp is not None:
            times.append(stamp)
    if bad_overall or invalidated:
        latest.observed_at = max(times, default=None)
    # Window metrics based on a future observation are not trustworthy either.
    for field in ("air_temperature_min", "air_temperature_max", "precipitation_1h_max",
                  "precipitation_3h", "precipitation_3h_max", "precipitation_24h",
                  "snow_depth_change", "wind_speed_max", "wind_from_direction_max"):
        value = getattr(latest, field)
        bad = bad_overall or (field.startswith("air_temperature") and value is not None
                              and not valid_temperature(value))
        for suffix in ("_time", "_period"):
            stamp = getattr(latest, field + suffix, None)
            if stamp:
                parsed = datetime.fromisoformat(stamp.rsplit("/", 1)[-1].replace("Z", "+00:00"))
                bad = bad or utc(parsed) > now
        if bad:
            setattr(latest, field, None)
            for suffix in ("_unit", "_time", "_period"):
                if hasattr(latest, field + suffix):
                    setattr(latest, field + suffix, None)
