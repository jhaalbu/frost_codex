# Data methods and filters

Implementation notes for the stored observations and GeoJSON exports. For the API contract, see [API reference](api.md). Live timeseries do not apply every station-specific filter used by the stored map data; see the API reference before comparing the two.


See [nve_discharge_classification.md](nve_discharge_classification.md) for notes on HydAPI rating curves, percentiles and discharge classification fields for ArcGIS/VertiGIS symbolization.

### Providers

See [Datakjelder](providers.md) for all six providers and their current mappings.

### Quality filtering

- Frost requests use `FROST_QUALITY_CODES`, default `0,1,2,3,4`.
- Observations with `quality_code >= 5` are skipped during storage when quality is present.
- `prune-db` also deletes any stored observations with `quality_code >= 5`.
- NVE and Snower quality values are stored when available, but missing quality is allowed because not all provider payloads use the same quality-code model.

### Latest values

- `station_latest` stores one flattened row per station for map display.
- `observed_at` is the provider's observation/reference time for the newest mapped element in the flattened row. It is kept as the station-wide latest time for compatibility and general station freshness.
- Each primary latest value also has its own timestamp: `air_temperature_observed_at`, `precipitation_observed_at`, `snow_depth_observed_at`, `wind_speed_observed_at`, `wind_from_direction_observed_at`, `discharge_observed_at` and `groundwater_level_observed_at`. Use these fields when the age of an individual value matters; parameters on the same station can update at different times.
- `updated_at` is the UTC time when the local `station_latest` row was last changed or its derived values were recalculated. It describes local processing, not when the station made an observation, and can therefore be later than `observed_at`.
- `minutes_since_observation` is not stored in the database. It is calculated when the GeoJSON response or cache is built as the number of whole minutes between the current UTC time and `stations.last_observation_time`, which normally matches `observed_at`. It is `null` when no observation time is known.
- `has_recent_data` is calculated from the same observation time and is `true` when `minutes_since_observation` is at most 120 minutes.
- API timestamps are returned as ISO 8601 UTC values, for example `2026-08-05T12:00:00Z`.
- Re-running `init-db` adds missing per-parameter timestamp columns and backfills them from the newest matching rows retained in `observations`. A timestamp remains `null` when no matching history row is available and is populated by a later sync.
- Capability fields such as `has_air_temperature` and `has_snow_depth` come from `station_capabilities`, not from whether the latest value is non-null.
- `parameter_profile` is derived from capabilities: `complete` means temperature, precipitation, wind and snow; `weather` means temperature, precipitation and wind; `snow` means snow is available; all other combinations are `lesser`.

### Precipitation filters

- Frost and NVE hourly precipitation is normalized to the logical field `precipitation_1h`.
- For NVE HydAPI stations, `precipitation_1h < 0` and `precipitation_1h > 5 mm` are treated as suspect.
- For SVV/Statens vegvesen road stations, `precipitation_1h > 5 mm` is treated as suspect because optical sensors can over-report during blowing snow.
- Suspect precipitation is excluded from the latest precipitation value and from rolling/max precipitation calculations.
- `is_precipitation_suspect` is set when the latest precipitation observation for a station was removed by this filter.
- For `latest.7d.geojson`, the same strict 5 mm hourly threshold is used for NVE and Vegvesen/SVV precipitation before calculating max and accumulated precipitation.
- Known stations `2.36.0` and `SN11000` are excluded from hourly precipitation because they have consistently reported values that do not behave like real one-hour precipitation.

### Snow filters

- Frost `surface_snow_thickness`, NVE `snow_depth` and Snower snow depth values are normalized to the logical field `snow_depth`.
- Snow values of `-1` are normalized to `0`.
- Snow values `<= -3` are discarded.
- For NVE snow depth, negative values are normalized to `0`, while values above `1000` are discarded as unrealistic.
- `snow_depth_change` compares the latest snow depth against the closest observation around 24 hours earlier, using a 7-day lookup window.
- If latest snow depth is `0` and no recent snow depth in the last 24 hours is above `0`, `snow_depth_change` is omitted.

### Temperature filters

- For SVV/Statens vegvesen stations, air temperature values from `-40.5` to `-39.5` are treated as sensor/error codes and omitted from latest values and derived temperature aggregates.
- Air temperature values close to `0` are omitted from `station_latest` when neighbouring values in the surrounding 6 hours are consistently at least `5` degrees away from zero on the same side, when the surrounding 24-hour pattern is dominated by exact zero values while several same-side non-zero values are far from zero, or when the last 6 hours are a flat run of zero values. This removes isolated and repeated sensor dropouts without filtering normal freezing-point weather.
- If the newest raw air temperature value is filtered as suspect, `latest.air_temperature` is set to `null` instead of falling back to an older valid temperature.

### Discharge classification

- NVE HydAPI discharge is normalized to the logical field `discharge`.
- `sync-metadata` fetches HydAPI `Percentiles` for NVE stations with discharge series and stores them in `nve_discharge_percentiles`.
- `sync-metadata` also uses the HydAPI discharge series version to fetch Chartserver statistics `QCm`, `QC5` and `QC50`, stored as `discharge_flood_qm`, `discharge_flood_q5` and `discharge_flood_q50` thresholds.
- Latest GeoJSON endpoints classify latest discharge against the percentile row for the observation date.
- Flood thresholds take priority over percentiles: at or above `Q50` is `flood_over_50y`, at or above `Q5` is `flood_5y_to_50y`, and at or above `QM` is `flood_mean_to_5y`. Values below `QM`, or stations without flood thresholds, use the existing percentile classification.
- Chartserver errors and missing series versions are handled per station; existing stored thresholds and percentile classification remain available.
- The current percentile classes are `low`, `normal`, `high_minus`, `high` and `high_plus`.
- Percentile bands are `< P25 = low`, `P25-P60 = normal`, `P60-P75 = high_minus`, `P75-P90 = high`, and `>= P90 = high_plus`. Flood classes override these bands when `QM`, `Q5` or `Q50` is reached.
- This percentile classification is not the same as official NVE flood warning thresholds.
- Run `init-db` once after deploying this change so the NVE percentile and flood-threshold tables exist, then run `sync-metadata` to populate them.

### 24-hour derived fields

- `precipitation_24h` is the sum of hourly precipitation rows in the last 24 hours before the station's latest observation time.
- `precipitation_1h_max` is the largest accepted hourly precipitation value in that same 24-hour window.
- `precipitation_1h_max_period` stores the one-hour period where that max occurred.
- `precipitation_3h` is a rolling 3-hour sum ending at the station's latest observation time.
- `precipitation_3h_max` is the largest rolling 3-hour sum in the last 24 hours.
- `precipitation_3h_max_period` stores the 3-hour period where that max occurred.
- `air_temperature_min` and `air_temperature_max` are min/max temperature over the latest 24-hour window. `air_temperature_min_time` and `air_temperature_max_time` store when those extremes occurred.
- `wind_speed_max` is the largest wind speed over the latest 24-hour window, and `wind_from_direction_max` is the wind direction at that same timestamp when available.

### 7-day GeoJSON method

- `latest.7d.geojson` is calculated from stored `observations`, not from `station_latest` columns.
- Incoming observations with a timestamp later than the current UTC time are rejected. Air temperatures outside the inclusive range -60 to +60 degrees Celsius are rejected as well.
- Exports also filter these invalid historical observations. Invalid cached latest values and future observation timestamps are suppressed; the observation history is not deleted. After deploying this change, run `python -m frost_sync refresh-geojson-cache` to replace existing cached exports, and reload the web application. Normal hourly sync also rebuilds the cache.
- The time window is the last 7 days from payload build time: cache-generation time for file responses, or request time for database fallback.
- It returns one feature per station only when at least one 7-day aggregate exists.
- In normal operation, `run-hourly` pre-generates this endpoint as a cache file; the web route only rebuilds it from the database if the cache file is missing.
- Maximum fields are named like `air_temperature_max_7d`, `snow_depth_max_7d`, `wind_speed_max_7d`, `wind_gust_max_7d`, `discharge_max_7d` and `groundwater_level_max_7d`.
- Each max field also gets a timestamp field named `*_max_7d_time`.
- Air temperature also exposes `air_temperature_min_7d` and `air_temperature_min_7d_time`.
- `wind_direction_at_7dmax` is the wind direction observed at the exact timestamp of `wind_speed_max_7d`; it is omitted when no matching direction observation exists.
- For discharge, the 7-day endpoint keeps only `discharge_max_7d`, `discharge_max_7d_time` and `discharge_class`. The class is calculated from `discharge_max_7d`, using the daily percentiles for `discharge_max_7d_time` and the stored flood thresholds.
- `latest.7d.geojson` omits `has_recent_data`; `minutes_since_observation` remains available.
- `precipitation_7d_accumulated` is the sum of accepted hourly precipitation values in the 7-day window.
- `precipitation_24h_max` is the largest rolling 24-hour precipitation sum whose end time falls within the 7-day window, and `precipitation_24h_max_time` is that end time. The calculation reads an extra 24 hours before the 7-day window so the first rolling windows are complete; that lookback is not included in `precipitation_7d_accumulated` or other 7-day maxima.
- Because this endpoint reads from `observations`, at least 7 days of retained observations are needed for the ordinary 7-day aggregates. Use at least 8 days for `precipitation_24h_max`, which also needs the preceding 24-hour lookback. Retention alone does not guarantee complete sampling or backfill missing history.

### Daily GeoJSON method

- `daily.geojson` is intended for a daily FME append/update into an ArcGIS history layer. It returns one feature per station with a stable `daily_id` in the form `source_id_YYYYMMDD`.
- A reporting day runs from 06:00 on `period_date` up to, but not including, 06:00 the next day in the `Europe/Oslo` time zone. This remains correct across daylight-saving transitions. `period_start` and `period_end` are returned as UTC timestamps.
- Without a date parameter, the endpoint returns the latest completed 06:00–06:00 period and normally uses the pre-generated cache. Use `?date=YYYY-MM-DD` to rebuild a specific retained period directly from the database.
- The schema is stable: fields without observations are retained as `null`, which makes it suitable for FME and a fixed ArcGIS feature-service schema.
- Temperature includes daily minimum and maximum with timestamps. Other measurement types expose only maximum values with timestamps. Wind direction is included at the exact time of maximum wind speed when available.
- `precipitation_24h` is the accepted hourly precipitation sum for the reporting period. The endpoint also includes the largest 1-hour and rolling 3-hour precipitation values and their accumulation end times.
- `discharge_class` is calculated from `discharge_max`, using percentiles for the date of that maximum and stored `QM`, `Q5` and `Q50` thresholds.
- The endpoint reads from retained `observations`; a historical date older than `FROST_RETENTION_DAYS` can therefore contain null measurement fields.

### Compact GeoJSON method

- `latest.compact.geojson` keeps the same station filtering as `latest.geojson`, but removes capability flags, coordinate properties, unit fields and all null values.
- `latest.7d.geojson` uses the same compact style so it can be added directly to an ArcGIS web map with less payload overhead.
- Both compact endpoints keep coordinates only in GeoJSON geometry.
- Both compact endpoints are served from pre-generated files when those files exist in `FROST_GEOJSON_CACHE_DIR`.
- The web response includes `X-GeoJSON-Cache`; expected values are `file-gzip`, `file` or `database-fallback`.

### Timeseries method

- `/api/stations/<source_id>/timeseries` and `/api/timeseries` fetch plotting data directly from the provider instead of reading historical observations from the local database.
- The API resolves provider from `source_id`, so the client does not need to send provider in the request.
- Frost and NVE requests are batched by provider for `/api/timeseries`.
- Frost timeseries prefers hourly elements such as `mean(air_temperature PT1H)`, `mean(wind_speed PT1H)`, `mean(wind_from_direction PT1H)` and `max(wind_speed_of_gust PT1H)`.
- If preferred Frost hourly elements are not available, raw values are fetched and aggregated to hourly points in the API response.
- `precipitation_24h_rolling` is derived from provider hourly precipitation values.
- `precipitation_accumulated` accumulates precipitation through the requested time range.

### Retention and scheduled jobs

- `run-hourly` updates observations and `station_latest`, but does not prune old observations.
- `run-hourly` also refreshes cached `latest.geojson`, `latest.fme.geojson`, `latest.compact.geojson`, `latest.7d.geojson` and `daily.geojson` files after the database commit.
- `sync-metadata` refreshes station metadata and capabilities and should run daily or when provider configuration changes.
- `prune-db` deletes observations older than `FROST_RETENTION_DAYS` and bad-quality rows.
- The default retention is 14 days, which is enough for 24-hour latest fields and the 7-day GeoJSON endpoint.
- `refresh-geojson-cache` rebuilds the cached latest GeoJSON files, including gzip-compressed `.gz` variants.
- `FROST_GEOJSON_CACHE_DIR` controls where pre-generated GeoJSON files are stored. Relative paths are resolved from the project root.

