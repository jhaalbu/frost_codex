from __future__ import annotations

import argparse
import logging

from frost_sync.config import load_settings, require_frost_client_id
from frost_sync.db import create_schema, create_session_factory
from frost_sync.frost_api import FrostClient
from frost_sync.service import SyncService


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="Sync Frost observations into a local database")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init-db", help="Create database schema")
    subparsers.add_parser("run-hourly", help="Run one hourly sync")
    serve_parser = subparsers.add_parser("serve", help="Run Flask API for ArcGIS/web clients")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=5000)

    args = parser.parse_args()

    if args.command == "init-db":
        settings = load_settings()
        create_schema(settings.database_url)
        print("Database schema created.")
        return

    if args.command == "run-hourly":
        settings = load_settings()
        session_factory = create_session_factory(settings.database_url)
        frost_client = FrostClient(
            base_url=settings.frost_base_url,
            client_id=require_frost_client_id(settings),
            timeout_seconds=settings.request_timeout_seconds,
            acceptable_quality_codes=settings.acceptable_quality_codes,
        )

        with session_factory() as session:
            service = SyncService(session=session, frost_client=frost_client)
            summary = service.run_hourly_sync(
                page_limit=settings.page_limit,
                source_batch_size=settings.source_batch_size,
                retention_days=settings.retention_days,
            )

        print(
            "Sync finished. "
            f"stations_seen={summary.stations_seen} "
            f"capabilities_updated={summary.capabilities_updated} "
            f"observations_written={summary.observations_written} "
            f"latest_updated={summary.latest_updated} "
            f"observations_deleted={summary.observations_deleted} "
            f"station_errors={summary.station_errors}"
        )
        return

    if args.command == "serve":
        from frost_sync.web import create_app

        app = create_app()
        app.run(host=args.host, port=args.port, debug=False)
        return

    raise RuntimeError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()
