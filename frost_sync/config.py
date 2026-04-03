from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


DEFAULT_DATABASE_URL = "sqlite:///frost_local.db"
DEFAULT_ENV_FILE = ".env"


@dataclass(frozen=True)
class Settings:
    frost_client_id: str | None
    database_url: str = DEFAULT_DATABASE_URL
    frost_base_url: str = "https://frost.met.no"
    request_timeout_seconds: int = 60
    page_limit: int = 1000
    source_batch_size: int = 100


def load_settings() -> Settings:
    _load_dotenv()

    client_id = os.getenv("FROST_CLIENT_ID", "").strip() or None
    database_url = os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL).strip() or DEFAULT_DATABASE_URL
    timeout = int(os.getenv("FROST_TIMEOUT_SECONDS", "60"))
    page_limit = int(os.getenv("FROST_PAGE_LIMIT", "1000"))
    source_batch_size = int(os.getenv("FROST_SOURCE_BATCH_SIZE", "100"))

    return Settings(
        frost_client_id=client_id,
        database_url=database_url,
        request_timeout_seconds=timeout,
        page_limit=page_limit,
        source_batch_size=source_batch_size,
    )


def require_frost_client_id(settings: Settings) -> str:
    if not settings.frost_client_id:
        raise RuntimeError("Missing required environment variable FROST_CLIENT_ID")
    return settings.frost_client_id


def _load_dotenv(env_file: str = DEFAULT_ENV_FILE) -> None:
    path = Path(os.getenv("FROST_ENV_FILE", env_file))
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value
