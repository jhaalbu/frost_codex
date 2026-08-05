# NVE discharge classification and rating curves

This note describes how HydAPI rating curves can be used in this app, and which data fields we should expose for ArcGIS/VertiGIS symbolization.

## What HydAPI rating curves provide

HydAPI `Ratingcurves` lists discharge series that have available rating curves.

For a specific station, `GET /api/v1/Ratingcurves/{stationId}` returns one or more curve periods. Each period is limited by `dtStartDate` and `dtEndDate`. If `dtEndDate` is null, the period is still active.

Each period can contain one or more segments. A segment is valid from `minimum` to `maximum`, and discharge is calculated as:

```text
Q = constant * (input - zero) ^ exponent
```

In this formula:

- `input` is water stage.
- `Q` is discharge.
- `constant`, `zero` and `exponent` come from the selected segment.
- the selected segment is the segment where water stage is within `minimum` and `maximum`.

## What rating curves do not provide

Rating curves do not tell us whether the current discharge is:

- over 50-year flood
- between 5-year and 50-year flood
- between mean annual flood and 5-year flood
- large, normal or low discharge

Those classes require classification thresholds. HydAPI rating curves only convert water stage to discharge. This app now obtains the thresholds from NVE Chartserver statistics for the versioned discharge series and stores them locally.

HydAPI `Percentiles` can help classify normal, high and low flow if percentile data exists for the station/parameter, but percentiles are separate from rating curves.

## Suggested map fields

To make symbolization simple, the GeoJSON should expose one flat field for the class and a few supporting fields.

Recommended fields:

- `discharge`: latest discharge value in m3/s.
- `discharge_observed_at`: timestamp for latest discharge observation.
- `discharge_age_hours`: age of latest discharge observation.
- `discharge_age_class`: `fresh`, `stale_4_24h`, `stale_over_24h` or `missing`.
- `discharge_class`: symbol class name.
- `discharge_class_rank`: numeric rank for sorting and renderer rules.
- `discharge_class_source`: `flood_threshold`, `percentile`, `rating_curve`, `latest_value_only` or `none`.
- `discharge_classification_missing`: true when discharge exists but no classification threshold exists.
- `discharge_value_missing`: true when no discharge value exists.
- `discharge_error`: optional short error/status text.

Suggested `discharge_class` values:

- `flood_over_50y`
- `flood_5y_to_50y`
- `flood_mean_to_5y`
- `high_plus`
- `high`
- `high_minus`
- `normal`
- `low`
- `missing_value`
- `missing_classification`
- `other_error`

Suggested `discharge_class_rank` values:

- `90`: over 50-year flood
- `80`: between 5-year and 50-year flood
- `70`: between mean annual flood and 5-year flood
- `60`: high plus
- `50`: high
- `40`: high minus
- `30`: normal
- `20`: low
- `10`: missing classification
- `0`: missing value
- `-1`: other error

## Classification method proposal

Use this priority order:

1. If latest discharge is missing, set `discharge_class = missing_value`.
2. If flood thresholds exist for the station, classify against those first.
3. If flood thresholds do not exist but percentile thresholds exist, classify with percentiles.
4. If only discharge exists, set `discharge_class = missing_classification`.
5. If there is an API or calculation error, set `discharge_class = other_error`.

Flood-threshold classification:

- `discharge >= q50`: `flood_over_50y`
- `q5 <= discharge < q50`: `flood_5y_to_50y`
- `qm <= discharge < q5`: `flood_mean_to_5y`

Percentile-based fallback:

- `discharge >= perc90`: `high_plus`
- `perc75 <= discharge < perc90`: `high`
- `perc60 <= discharge < perc75`: `high_minus`
- `perc25 <= discharge < perc60`: `normal`
- `discharge < perc25`: `low`

This is a practical percentile-based classification, not the same as NVE flood warning thresholds.

## Rating curve use in this app

Rating curves are most useful when:

- a station has water stage but not direct discharge
- we want to validate discharge against stage
- we want to show whether a discharge value was measured directly or calculated from stage

Suggested future fields if we calculate discharge from stage:

- `water_stage`
- `water_stage_observed_at`
- `rating_curve_available`
- `rating_curve_period_no`
- `rating_curve_segment_no`
- `rating_curve_valid_from`
- `rating_curve_valid_to`
- `discharge_calculated_from_stage`
- `discharge_calculation_error`

For the current map symbolization, direct `discharge` from HydAPI is the simplest and safest starting point. Rating curves can be added later to fill gaps for stations that only expose water stage.

## First implementation step

The app stores HydAPI discharge percentiles in `nve_discharge_percentiles` when `sync-metadata` is run. The compact/latest GeoJSON then joins today's percentile row to the latest discharge value and emits:

- `discharge_observed_at`
- `discharge_age_hours`
- `discharge_age_class`
- `discharge_class`
- `discharge_class_rank`
- `discharge_class_source`
- `discharge_classification_missing`
- `discharge_value_missing`
- `discharge_percentile_date`
- `discharge_perc25`
- `discharge_perc60`
- `discharge_perc75`
- `discharge_perc90`
- `discharge_perc95`

If a station has discharge but no percentile row for the current date, it gets:

```text
discharge_class = missing_classification
discharge_class_source = latest_value_only
discharge_classification_missing = true
```

If a station has percentile data, `discharge_class_source = percentile`.

## Flood-threshold implementation

For an NVE discharge series such as `84.21.0.1001.1`, `84.21.0` is the station ID, `1001` is the discharge parameter and the final `1` is the series version. The version is discovered from HydAPI series metadata rather than assumed.

During `sync-metadata`, the app requests Chartserver statistics `qcm`, `qc5` and `qc50` and stores one `nve_discharge_flood_thresholds` row per station. GeoJSON exposes:

- `discharge_flood_qm`
- `discharge_flood_q5`
- `discharge_flood_q50`
- `discharge_flood_unit`
- `discharge_flood_series_version`
- `discharge_flood_updated_at`

Flood classification takes priority when the latest discharge reaches a flood threshold. Below `QM`, the daily HydAPI percentile classification remains the fallback. A Chartserver error affects only that station and does not delete a previously stored threshold row.

Deployment notes:

- Run `python -m frost_sync init-db` once after deploying this change so the new `nve_discharge_percentiles` table exists.
- Run `python -m frost_sync sync-metadata` to populate or refresh NVE percentile rows.
- Run `python -m frost_sync run-hourly` or `python -m frost_sync refresh-geojson-cache` after that so the cached GeoJSON includes the new fields.
