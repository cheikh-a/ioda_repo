# Data Dictionary

## `data/processed/entity_catalog.parquet`

Machine-readable discovery catalog (one row per entity x datasource).

Key columns:

- `level`: `country` or `region`
- `entity_type`: API entity type (`country` / `region`)
- `entity_id`: API entity code (ISO2 for countries, numeric-like code for regions)
- `entity_name`
- `iso2`: country ISO-3166-1 alpha-2 when applicable
- `parent_country_id`, `parent_country_name`: populated for regions
- `attrs_json`: raw entity metadata attributes from `/entities/query`
- `metric`: datasource short name from `/datasources/`
- `metric_name`: datasource display name
- `unit`: datasource unit text
- `coverage_min_ts`, `coverage_max_ts`: inferred earliest/latest Unix timestamps (nullable)
- `coverage_min_utc`, `coverage_max_utc`: UTC timestamps derived from coverage fields (nullable)
- `coverage_status`: coverage probe status (e.g. `ok`, `no_recent_data`, `transient_error`)
- `coverage_method`: coverage inference method label
- `coverage_checked_at_utc`: timestamp when coverage was checked
- `coverage_source`: `probe` or `cache`

## `data/processed/ioda_long.parquet`

Normalized tidy table built from raw chunk JSON.

Core columns:

- `timestamp_utc`: observation timestamp in UTC
- `level`: `country` / `region`
- `entity_type`, `entity_id`, `entity_name`
- `parent_country_id`, `parent_country_name` (regions)
- `datasource`: base datasource short name
- `subtype`: API subtype when present (e.g., some GTR products)
- `metric`: normalized metric key
  - scalar datasources often remain as the datasource name (e.g. `bgp`)
  - nested payloads expand into derived metrics (e.g. `ping-slash24-latency__mean_latency`)
- `series_variant`: deterministic qualifier from nested payload dimensions (e.g., `team=team-2`)
- `value`: numeric value (nullable)
- `unit`: datasource unit (best-effort for derived metrics via base datasource)
- `source_fields_json`: JSON string with preserved non-numeric nested fields used to derive `series_variant`
- `step_seconds`, `native_step_seconds`: API-reported aggregation steps
- `raw_file`: source raw JSON path (relative to repo root)
- `raw_window_start_ts`, `raw_window_end_ts`: requested chunk window (from filename)
- `duplicate_key_count`: number of raw rows sharing the same normalized key before dedupe

Dedupe rule:

- Exact duplicates are dropped.
- For duplicate normalized keys (`timestamp_utc`, `level`, `entity_id`, `metric`, `series_variant`), the first row in deterministic sort order is kept.

## `data/processed/ioda_country_panel.parquet`

Wide-format panel for country-level analysis.

Index-like columns:

- `timestamp_utc`
- `entity_id`
- `entity_name`
- `parent_country_id` (empty for countries)
- `parent_country_name` (empty for countries)

Metric columns:

- One column per `metric` or `metric + series_variant` combination from `ioda_long.parquet`

## `data/processed/ioda_region_panel.parquet`

Wide-format panel for region-level analysis (written when region data exists).

Same structure as country panel, with region metadata and populated `parent_country_*` fields.

## `data/processed/qa_summary.parquet`

Group-level QA summary (grouped by entity + metric + series variant).

Key columns:

- `level`, `entity_id`, `entity_name`
- `metric`, `series_variant`, `metric_key`
- `unit`
- `min_timestamp_utc`, `max_timestamp_utc`
- `n_rows`, `n_non_null`, `n_null`, `null_fraction`
- `median_step_seconds`, `max_gap_seconds`, `gap_count`
- `negative_count`
- `bounded_range_violations`
- `spike_count`
- `duplicate_rows`

Note:

- `max_timestamp_utc` is also used by `--since-last-run` mode as a processed coverage checkpoint.
