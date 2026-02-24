# IODA West Africa Outage Data Pipeline

Reproducible Python pipeline to retrieve IODA API v2 outage-related time-series signals for West Africa and build analysis-ready parquet panels.

Supports:

- Country aggregates (national level)
- Region entities (subnational) when available via IODA `entities/query`

The pipeline is API-first (official IODA v2 endpoints), stores raw JSON responses, and produces normalized + QA outputs.

## What This Repo Does

1. Discovers IODA datasources (signals/metrics) and entities (countries, regions)
2. Resolves a configurable West Africa entity set (ECOWAS default + optional Mauritania)
3. Probes earliest/latest availability (with caching) when coverage metadata is not exposed directly
4. Fetches chunked raw JSON from `/signals/raw/...` with retries/backoff/rate limiting
5. Normalizes nested payloads into a tidy long table and wide panels
6. Generates QA coverage/missingness/anomaly summaries

## Project Layout

```text
ioda_repo/
  README.md
  pyproject.toml
  config/
    west_africa.yaml
  src/ioda/
    __init__.py
    api.py
    discover.py
    fetch.py
    transform.py
    qa.py
    utils.py
  scripts/
    ioda_discover.py
    ioda_fetch.py
    ioda_monthly_closeout.py
    ioda_build_panel.py
    ioda_qa.py
  data/
    raw/
    intermediate/
    processed/
    logs/
  docs/
    api_notes.md
    entity_catalog.md
    data_dictionary.md
    qa_report.md
  tests/
    test_smoke.py
```

## Quickstart

## 1) Install dependencies (Python 3.11+)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e ".[dev]"
```

## 2) Discover entities + metrics + coverage

```bash
python scripts/ioda_discover.py
```

Fast smoke discovery (1 country, no coverage probing):

```bash
python scripts/ioda_discover.py --limit-entities 1 --no-probe-coverage --no-regions
```

## 3) Fetch raw IODA signals

Fetch both country + region data using all discovered metrics (default):

```bash
python scripts/ioda_fetch.py --level both --metrics auto
```

Dry run (show planned requests only):

```bash
python scripts/ioda_fetch.py --level both --metrics auto --dry-run
```

Quick smoke fetch (1 country, 2 days, one metric):

```bash
python scripts/ioda_fetch.py --level country --metrics ping-slash24 --limit-entities 1 --start 2026-02-20 --end 2026-02-22
```

Incremental fetch from processed coverage:

```bash
python scripts/ioda_fetch.py --since-last-run
```

## 4) Build normalized panels

```bash
python scripts/ioda_build_panel.py
```

This writes:

- `data/processed/ioda_long.parquet`
- `data/processed/ioda_country_panel.parquet`
- `data/processed/ioda_region_panel.parquet` (if region data exists)

## 5) Run QA

```bash
python scripts/ioda_qa.py
```

This writes:

- `docs/qa_report.md`
- `data/processed/qa_summary.parquet`

## Monthly Closeout Workflow (March, April, etc.)

Use the helper script to fetch exactly one calendar month (UTC), then rebuild panels and QA.

Default behavior:

- If `--month` is omitted, it targets the previous UTC month
- It uses an exclusive end boundary at the first day of the next month
- It blocks the current month by default (to avoid accidental partial-month closeout)

Example: close out March 2026 (run in April 2026)

```bash
python scripts/ioda_monthly_closeout.py --month 2026-03 --overwrite
```

Example: close out April 2026

```bash
python scripts/ioda_monthly_closeout.py --month 2026-04 --overwrite
```

Preview planned requests only:

```bash
python scripts/ioda_monthly_closeout.py --month 2026-03 --dry-run
```

Notes:

- `--overwrite` is recommended for month closeout reruns so the month is refreshed cleanly.
- The script runs fetch -> build_panel -> qa in one command (unless `--no-build` or `--no-qa` is used).
- If you changed the configured entity set (for example enabling/disabling Mauritania), run discovery first:

```bash
python scripts/ioda_discover.py --no-probe-coverage
```

## Configuration

Edit `config/west_africa.yaml`:

- `region_definition.countries`: editable target country list
- `region_definition.include_mauritania`: toggles Mauritania inclusion
- `fetch_defaults.request`: user-agent, timeout, retry, pacing
- `fetch_defaults.chunking`: chunk size defaults and response-size threshold
- `fetch_defaults.window`: default baseline backfill window used when `ioda_fetch.py` is run without `--start/--end`
- `discovery`: coverage probing defaults and cache location

Discovery updates only the `generated:` section and preserves manual edits in other sections.

## Reproducibility Notes

- Raw API responses are stored per request chunk under `data/raw/...`.
- Request logs are written to `data/logs/requests.ndjson` (timestamp, URL, params, status, bytes, duration).
- Processed outputs are deterministic for the same raw input set and normalization logic.
- `--start` / `--end` allow fixed time windows. If omitted, `--end` defaults to runtime UTC now.
- `--since-last-run` uses processed coverage (`qa_summary.parquet` or `ioda_long.parquet`) to append newer data.
- `scripts/ioda_monthly_closeout.py` is the recommended month-close path after the initial baseline backfill.

## Expected Runtime (Guidance)

- Full West Africa discovery with coverage probing (countries + all regions + all metrics) can be slow because coverage is inferred by probing.
- Full-history fetches can take a long time depending on:
  - number of entities
  - number of datasources
  - API response sizes
  - chunk fallback frequency
- Use `--limit-entities`, `--metrics`, and narrower date ranges for smoke tests and staged runs.

## Troubleshooting

## 429 / Timeouts / 5xx

- The client already retries transient errors with exponential backoff + jitter.
- Increase pacing:
  - `--min-interval-seconds 1.0` (or higher)
- Reduce response size:
  - lower `--max-points`
  - lower `--max-response-bytes`
  - use smaller `--initial-chunk-mode day`

## Partial Coverage / Empty Responses

- Some entity/metric combinations may have no recent data (`coverage_status = no_recent_data`).
- The API often returns valid empty envelopes (e.g., `data: [[]]`) instead of errors.
- Re-run discovery with `--refresh-coverage` if you suspect coverage cache staleness.

## Schema Changes

- The IODA API returns datasource-specific `values` payloads (scalar and nested).
- Nested `agg_values` are expanded into derived metrics in the long table.
- If the API changes payload shape, inspect `data/raw/...` first, then adjust `src/ioda/transform.py`.

## No Secrets / Auth

- The tested IODA v2 endpoints used here do not require API keys.
- Do not commit secrets if future endpoints require authentication.

## Notes

- API details and endpoint summaries: `docs/api_notes.md`
- Output schemas: `docs/data_dictionary.md`
