# IODA API v2 Notes (Official API)

## Scope

These notes summarize the official IODA v2 HTTP API at:

- `https://api.ioda.inetintel.cc.gatech.edu/v2/`

They are based on:

- the API docs page (OpenAPI spec embedded in the HTML at `/v2/`)
- direct sample requests to the documented endpoints

No unofficial scraping beyond the official docs page was used.

## Base URL / Auth / Rate Limits

- Base URL: `https://api.ioda.inetintel.cc.gatech.edu/v2`
- Authentication: no API key or auth header was required for the endpoints tested (`/datasources`, `/entities/query`, `/signals/raw/...`).
- Rate limits: no explicit rate-limit policy or response headers were observed in the docs page or sample responses.
  - Practical implication: client should self-throttle (rate limit, retries, backoff, chunking).

## Common Response Envelope

Most endpoints return a JSON envelope with fields like:

- `type`
- `metadata` (`requestTime`, `responseTime`)
- `requestParameters`
- `error`
- `perf` (sometimes present, often `null`)
- `data`
- `copyright`

Example patterns seen:

- `datasources`: `data` is a list of datasource objects
- `entities/query`: `data` is a list of entity metadata objects
- `signals/raw/...`: `data` is a nested list, where the innermost items are timeseries objects

## Endpoints Relevant to This Pipeline

## 1) List available signals / metrics (datasources)

- Endpoint: `GET /datasources/`
- Purpose: list datasources used by IODA and their units (used here as discoverable signal/metric names)

Observed fields per datasource:

- `datasource` (e.g. `bgp`, `ping-slash24`, `gtr`, `mozilla`)
- `name`
- `units`

Observed datasources (sample on 2026-02-24 UTC):

- `gtr`
- `gtr-sarima`
- `gtr-norm`
- `merit-nt`
- `bgp`
- `ping-slash24`
- `ping-slash24-loss`
- `ping-slash24-latency`
- `upstream-delay-penult-asns`
- `upstream-delay-penult-e2e-latency`
- `mozilla`

Notes:

- Some datasources return scalar values (simple numeric timeseries).
- Some return nested objects per time bucket (`agg_values`, etc.), not a single scalar.

## 2) List entities (countries, regions) and metadata

- Endpoint: `GET /entities/query`
- Purpose: discover countries and subnational regions and map regions to parent countries

Important query params (all optional unless noted by use case):

- `entityType` (e.g. `country`, `region`, `asn`)
- `entityCode` (single code or comma-separated codes)
- `relatedTo` (format: `entityType/entityCode`, e.g. `country/GH`)
- `search`
- `limit`
- `page`

Useful patterns for this project:

- List countries: `entityType=country`
- Resolve known country codes: `entityType=country&entityCode=GH,NG,CI`
- List regions within a country: `entityType=region&relatedTo=country/GH`

Observed entity object fields:

- `code`
- `name`
- `type`
- `subnames` (list)
- `attrs` (object)

Observed region `attrs` include parent-country mapping fields:

- `country_code`
- `country_name`
- `fqid`
- `ne_region_id`

Pagination behavior (observed):

- `limit` and `page` are accepted.
- `page` appears to be 1-based in examples tested (`page=1`, `page=2` both valid).
- No total-count field was observed in the response envelope; client should iterate until empty page (or page shorter than `limit`).

## 3) Retrieve outage-related time series signals

- Endpoint: `GET /signals/raw/{entityType}/{entityCode}`
- Purpose: fetch raw time-series signals used by IODA visualizations (country/region/asn/etc.)

Path params:

- `entityType` (observed supported values include `country`, `region`, `asn`, etc.)
- `entityCode` (country code like `GH`, region code like `1473`)

Important query params:

- `from` (required): Unix epoch seconds
- `until` (required): Unix epoch seconds
- `datasource` (optional): filter to one datasource
- `sourceParams` (optional): datasource-specific params
- `maxPoints` (optional): max points per returned timeseries

Observed `signals/raw` response shape:

- envelope field `data` is a nested list
- for single-entity requests tested, `data` was a list of length 1
- `data[0]` is a list of timeseries objects (one per datasource/subtype returned)

Observed timeseries object fields:

- `entityType`
- `entityCode`
- `entityName`
- `entityFqid`
- `datasource`
- `subtype` (may be empty string; non-empty for e.g. `gtr` product variants)
- `from` (bucket-aligned series start)
- `until` (bucket-aligned series end boundary)
- `step`
- `nativeStep`
- `values`

`values` shape is datasource-dependent:

- Scalar series: `values` is a list of numbers (e.g. `ping-slash24`, `bgp`)
- Nested series: `values` is a list where each element can be a list of objects (often containing `agg_values`)
  - examples observed: `ping-slash24-loss`, `ping-slash24-latency`, `gtr-sarima`

Empty-window behavior (important for earliest-date probing):

- Querying an old window can return a valid envelope with empty series data, e.g. `data: [[]]` rather than an HTTP error.

## Coverage metadata (earliest/latest availability)

No dedicated coverage metadata endpoint was identified in the published v2 docs / OpenAPI for per-entity, per-datasource earliest availability.

Practical implication:

- earliest coverage must be inferred by probing `signals/raw` and checking for non-empty responses
- latest coverage can be inferred from recent successful responses (series timestamps / `from`-`until`)

## Other Relevant (but Secondary) Endpoints

These are documented and may be useful later, but are not required for the time-series panel pipeline:

- `GET /outages/alerts`
- `GET /outages/events`
- `GET /outages/summary`
- `GET /topo/{entityType}` (topographic metadata for mapping)

## Query / Schema Notes for Implementation

- Time parameters use Unix epoch seconds.
- `signals/raw` response may include multiple series for a single datasource due to `subtype` (e.g. GTR product names).
- `signals/raw` nested `values` require defensive parsing; do not assume a scalar list.
- `error` field in the envelope should be checked even on HTTP 200 responses.
- `perf` is useful for diagnostics but should be treated as optional.

## cURL Examples

## List entities (countries)

```bash
curl -sS 'https://api.ioda.inetintel.cc.gatech.edu/v2/entities/query?entityType=country&limit=5&page=1'
```

## List regions for a country (Ghana)

```bash
curl -sS 'https://api.ioda.inetintel.cc.gatech.edu/v2/entities/query?entityType=region&relatedTo=country/GH&limit=10&page=1'
```

## Fetch a short time window for one entity (country GH, ping-slash24)

```bash
NOW=$(date -u +%s)
FROM=$((NOW - 2*86400))
curl -sS "https://api.ioda.inetintel.cc.gatech.edu/v2/signals/raw/country/GH?from=${FROM}&until=${NOW}&datasource=ping-slash24&maxPoints=50"
```

## Fetch available datasources

```bash
curl -sS 'https://api.ioda.inetintel.cc.gatech.edu/v2/datasources/'
```

## Open Questions / Limitations to Handle in Code

- No explicit per-endpoint rate limits documented.
- No explicit coverage-index endpoint found for earliest availability.
- Some datasources have complex nested payloads, so panel-building must either:
  - expand nested metrics into derived metric columns, or
  - preserve nested fields while emitting a scalar-focused panel.
