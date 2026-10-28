"""Daily HF-HBV model discharge, kept separate from observed discharge."""
from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
import math
from zoneinfo import ZoneInfo

import requests


def next_hbv_reference_time(now: datetime) -> datetime:
    """Tomorrow's VEPS daily reference (06 UTC), using the Norwegian date."""
    tomorrow = now.astimezone(ZoneInfo("Europe/Oslo")).date() + timedelta(days=1)
    return datetime.combine(tomorrow, time(6), tzinfo=timezone.utc)


class NveVepsClient:
    def __init__(self, base_url: str = "https://vepsapi.nve.no", timeout_seconds: int = 60):
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        # Never reuse the authenticated HydAPI session for VEPS.
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.headers.update({"Accept": "application/json", "User-Agent": "frost-sync/1.0"})

    def fetch_observations_range(self, source_ids: list[str], from_dt: datetime,
                                 to_dt: datetime) -> list[dict]:
        """Return model rows in the common series format, including future points.

        Missing locations/elements mean no model coverage. Daily timestamps retain
        VEPS's reference time; no observation freshness or quality is inferred.
        """
        if from_dt.tzinfo is None or to_dt.tzinfo is None or from_dt > to_dt:
            raise ValueError("VEPS requires an ordered, timezone-aware time range")
        source_ids = list(dict.fromkeys(source_ids))
        points = {}
        for offset in range(0, len(source_ids), 25):
            batch = source_ids[offset:offset + 25]
            try:
                response = self.session.get(
                    f"{self.base_url}/timeseriesdata/read",
                    params={
                        "locations": ",".join(batch),
                        "referenceTime": f"{from_dt.isoformat()}/{to_dt.isoformat()}",
                        "dataSource": "hf_hbv", "elements": "6004",
                        "zone": "false", "ensemble": "false",
                        "timeOffsets": "PT6H", "timeResolutions": "P1D",
                        "generatedTime": "1900-01-01/2100-01-01",
                    }, timeout=self.timeout_seconds,
                )
                response.raise_for_status()
                payload = response.json()
            except (requests.RequestException, ValueError) as exc:
                raise RuntimeError(f"NVE VEPS request failed: {type(exc).__name__}") from None
            if not isinstance(payload, dict) or not isinstance(payload.get("locations"), list):
                raise RuntimeError("NVE VEPS returned an invalid locations payload")
            for location in payload["locations"]:
                if not isinstance(location, dict) or not isinstance(location.get("elements", []), list):
                    raise RuntimeError("NVE VEPS returned an invalid location")
                source_id = location.get("station_id")
                if source_id not in batch:
                    continue
                for element in location.get("elements", []):
                    if not isinstance(element, dict) or not isinstance(element.get("data", []), list):
                        raise RuntimeError("NVE VEPS returned an invalid element")
                    if (str(element.get("parameter_key")) != "6004"
                            or element.get("method_name") != "mean"
                            or element.get("unit") not in {"m^3/s", "m3/s", "m³/s"}):
                        continue
                    for point in element.get("data", []):
                        if not isinstance(point, dict):
                            continue
                        if point.get("source") != "hf_hbv":
                            continue
                        raw_time = point.get("reference_time")
                        if not isinstance(raw_time, str):
                            continue
                        try:
                            stamp = datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
                            values = point["values"]
                            if not isinstance(values, list) or len(values) != 1 or isinstance(values[0], bool):
                                continue
                            value = float(values[0])
                        except (KeyError, ValueError, TypeError, IndexError):
                            continue
                        if (stamp.tzinfo is None or not from_dt <= stamp <= to_dt
                                or not math.isfinite(value) or value < 0):
                            continue
                        stamp = stamp.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
                        points[source_id, stamp] = {
                            "sourceId": source_id, "referenceTime": stamp,
                            "observations": [{"elementId": "discharge_hbv", "value": value,
                                              "unit": "m3/s", "qualityCode": None}],
                        }
        return [points[key] for key in sorted(points)]
