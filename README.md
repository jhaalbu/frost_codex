# Frost station sync prototype

This project syncs weather observations from MET Norway's Frost API for all Norwegian stations and exposes the data through a small Flask API that fits ArcGIS/web map use cases.
It can also import station metadata and selected latest observations from NVE HydAPI and Snower when those integrations are configured.

The local development setup uses SQLite by default. The code is structured so the same service can later run on PythonAnywhere with MySQL by changing the database URL.

## What it stores

- `stations`: metadata and coordinates for stations
  Includes `provider`, so stations can come from `frost`, `nve_hydapi` or `snower`
- `station_capabilities`: which supported elements each station exposes
- `observations`: history table with one row per element and timestamp
- `station_latest`: one row per station with latest values for map display, including rolling precipitation for the last 24 hours, plus hydrology fields like `discharge` and `groundwater_level`
- `stations.stationholder`: the Frost station holder, exposed as `stationholder` in GeoJSON

## Quick start

1. Install Python 3.11+.
2. Create and activate a virtual environment.
3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. Set environment variables:

```bash
set FROST_CLIENT_ID=your-client-id
set DATABASE_URL=sqlite:///frost_arcgis.db
```

Or create a `.env` file in the project root:

```text
FROST_CLIENT_ID=your-client-id
DATABASE_URL=sqlite:///frost_arcgis.db
NVE_HYDAPI_KEY=your-nve-api-key
SNOWER_USERNAME=your-snower-username
SNOWER_PASSWORD=your-snower-password
SNOWER_DOMAIN_ID=your-snower-domain-id
FROST_SOURCE_BATCH_SIZE=25
FROST_RETENTION_DAYS=14
```

5. Create the database schema:

```bash
python -m frost_sync init-db
```

6. Run one sync:

```bash
python -m frost_sync run-hourly
```

7. Run the local API:

```bash
flask --app app run --host 127.0.0.1 --port 5000
```

## API endpoints

- `GET /health`
- `GET /api/stations/latest.geojson`
- `GET /api/stations/latest.geojson?has=air_temperature`
- `GET /api/stations/history.geojson?date=2026-04-03`
- `GET /api/stations/history.geojson?from=2026-04-03T00:00:00Z&to=2026-04-03T23:59:59Z`
- `GET /api/stations/SN18700`
- `GET /api/stations/SN18700/observations?date=2026-04-03`
- `GET /api/parameters`
- `GET /api/stations/SN18700/timeseries?parameters=air_temperature,precipitation_1h,precipitation_24h_rolling&from=2026-05-04T00:00:00Z&to=2026-05-05T23:59:59Z`

The `latest.geojson` endpoint is the best starting point for ArcGIS map display because it returns one feature per station with the latest values already flattened into fields.
It includes both `precipitation_1h` and rolling `precipitation_24h`, and can also include `discharge` and `groundwater_level` for NVE HydAPI stations, plus `snow_depth` and mapped temperature values from Snower monitors.
To make ArcGIS symbolization easier, the endpoint also includes `available_parameter_count` and `parameter_profile`.

The `timeseries` endpoint is meant for plotting in applications like VertiGIS/Highcharts.
It fetches data directly from Frost, NVE HydAPI or Snower instead of reading the local history table, so you can request longer periods without having to keep all plotting data in the local database.
The only derived plotting series currently exposed is `precipitation_24h_rolling`, which is calculated from hourly precipitation values returned by the provider.
For Frost plotting, the endpoint now prefers hourly series such as `mean(air_temperature PT1H)`, `mean(wind_speed PT1H)`, `mean(wind_from_direction PT1H)` and `max(wind_speed_of_gust PT1H)`.
If a station does not expose those hourly Frost elements, the API falls back to raw observations and aggregates them to hourly values before returning the series.
The response is intentionally simple:
- `station` contains `provider`, `source_id`, `stationholder`, `name` and `masl`
- `series` is an object keyed by parameter name
- each parameter contains `parameter`, `unit` and `data`
- each `data` point contains `time`, `timestamp`, `value` and `quality_code`

## Reuse inside an existing Flask app

If you already have a Flask app on PythonAnywhere, this project can live inside that same app. The repo now includes [app.py](C:\Users\Aalbu\OneDrive\Dokumenter\Koding\frost_codex\app.py), which combines:

- the existing Frost proxy routes: `/frost`, `/frost_available`, `/sources`
- the new database-backed ArcGIS routes under `/weather/...`

If you want to register only the database-backed routes inside another Flask app, use the blueprint directly:

```python
from frost_sync.web import create_blueprint

app.register_blueprint(create_blueprint(), url_prefix="/weather")
```

Then the endpoints will be available as:

- `/weather/health`
- `/weather/api/stations/latest.geojson`
- `/weather/api/stations/history.geojson?date=2026-04-03`

## PythonAnywhere

Recommended production setup on PythonAnywhere:

- Use `MySQL`, not SQLite
- Keep the web app as Flask/WSGI
- Run the sync as an hourly scheduled task

1. Upload the project to `/home/yourusername/frost_codex`
2. Create a virtualenv and install dependencies:

```bash
mkvirtualenv --python=/usr/bin/python3.13 frostenv
pip install -r /home/yourusername/frost_codex/requirements.txt
```

3. Create a `.env` file in the project root with production values:

```text
FROST_CLIENT_ID=your-frost-client-id
DATABASE_URL=mysql+pymysql://yourusername:your-mysql-password@your-mysql-host/yourusername$weather?charset=utf8mb4
NVE_HYDAPI_KEY=your-nve-hydapi-key
SNOWER_USERNAME=your-snower-username
SNOWER_PASSWORD=your-snower-password
SNOWER_DOMAIN_ID=your-snower-domain-id
FROST_TIMEOUT_SECONDS=60
FROST_PAGE_LIMIT=1000
FROST_SOURCE_BATCH_SIZE=25
FROST_RETENTION_DAYS=14
```

4. Initialize the database:

```bash
cd /home/yourusername/frost_codex
workon frostenv
python -m frost_sync init-db
```

5. Create an hourly scheduled task:

```bash
cd /home/yourusername/frost_codex && /home/yourusername/.virtualenvs/frostenv/bin/python -m frost_sync run-hourly
```

6. Create a Flask web app in the PythonAnywhere dashboard and point its WSGI file at [pythonanywhere_wsgi.py](C:\Users\Aalbu\OneDrive\Dokumenter\Koding\frost_codex\pythonanywhere_wsgi.py). Replace `yourusername` in that file with your actual PythonAnywhere username.

Use a MySQL connection string such as:

```text
mysql+pymysql://yourusername:your-mysql-password@your-mysql-host/yourusername$weather?charset=utf8mb4
```

## Notes

- Frost API authentication uses the client ID as the username and an empty password.
- NVE HydAPI requires an API key in the `X-API-Key` header; station and latest observation sync is enabled only when `NVE_HYDAPI_KEY` is set.
- Snower requires `SNOWER_USERNAME`, `SNOWER_PASSWORD` and `SNOWER_DOMAIN_ID`; the integration authenticates through `POST /login` and then uses `authentication-key` and `domain-id` headers for the remaining calls.
- The sync uses `sources` and `observations` endpoints.
- Some stations do not have all requested elements, so capability tracking is stored separately from observation values.
- If a `.env` file exists in the project root, it is loaded automatically.
- You can override the env file location with `FROST_ENV_FILE`, which is useful in PythonAnywhere WSGI setups.
- For SQLite testing, use a fresh database file when the schema changes significantly.
- MySQL connections use `pool_recycle=280` and `pool_pre_ping=True`, matching PythonAnywhere's SQLAlchemy guidance.
- `app.py` expects `FROST_CLIENT_ID` in environment variables or `.env`; the key is no longer hardcoded in source.
- `FROST_RETENTION_DAYS=14` prunes old rows from `observations` while keeping `station_latest` available for map display.
- Re-running `python -m frost_sync init-db` is safe and will add newer `station_latest` columns like `precipitation_24h` when needed.
- For stations held by SVV/Statens vegvesen, hourly precipitation above 5 mm is marked with `is_precipitation_suspect` and excluded from the latest precipitation value used in map display.
