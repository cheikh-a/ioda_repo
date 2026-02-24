from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from .api import IODAClient, IODATransientError
from .utils import (
    REPO_ROOT,
    coerce_utc_series,
    dataframe_to_parquet,
    ensure_dir,
    epoch_to_utc_string,
    from_epoch_seconds,
    isoformat_utc,
    json_dump,
    json_load,
    load_yaml,
    parse_entities_from_config,
    read_parquet_if_exists,
    save_yaml,
    to_epoch_seconds,
    update_generated_config_sections,
    utc_now,
)


SERIES_REQUIRED_KEYS = {"entityType", "entityCode", "datasource", "from", "until", "step", "values"}


@dataclass(frozen=True)
class CoverageResult:
    entity_type: str
    entity_code: str
    metric: str
    earliest_ts: int | None
    latest_ts: int | None
    status: str
    method: str
    checked_at_utc: str


def _iter_series_objects(node: Any):
    if isinstance(node, dict):
        if SERIES_REQUIRED_KEYS.issubset(node.keys()):
            yield node
            return
        for v in node.values():
            yield from _iter_series_objects(v)
    elif isinstance(node, list):
        for item in node:
            yield from _iter_series_objects(item)


def _point_has_data(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return True
    if isinstance(value, (int, float)):
        return True
    if isinstance(value, dict):
        return len(value) > 0
    if isinstance(value, list):
        return any(_point_has_data(v) for v in value)
    return False


def payload_has_data(payload: dict[str, Any]) -> bool:
    for series in _iter_series_objects(payload.get("data")):
        values = series.get("values") or []
        if any(_point_has_data(v) for v in values):
            return True
    return False


def payload_time_bounds(payload: dict[str, Any]) -> tuple[int | None, int | None]:
    min_ts: int | None = None
    max_ts: int | None = None
    for series in _iter_series_objects(payload.get("data")):
        values = series.get("values") or []
        step = int(series.get("step") or 0)
        start = int(series.get("from") or 0)
        if step <= 0:
            continue
        for idx, val in enumerate(values):
            if not _point_has_data(val):
                continue
            ts = start + idx * step
            min_ts = ts if min_ts is None else min(min_ts, ts)
            max_ts = ts if max_ts is None else max(max_ts, ts)
    return min_ts, max_ts


def list_entities(
    client: IODAClient,
    *,
    entity_type: str,
    related_to: str | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    page = 0
    page_size = max(1, min(int(limit), 100))
    out: list[dict[str, Any]] = []
    while True:
        payload = client.query_entities(entityType=entity_type, relatedTo=related_to, limit=page_size, page=page)
        rows = payload.get("data") or []
        if not isinstance(rows, list):
            raise ValueError(f"Unexpected entities data shape for {entity_type}: {type(rows).__name__}")
        if not rows:
            break
        out.extend(rows)
        if len(rows) < page_size:
            break
        page += 1
    return out


def list_datasources(client: IODAClient) -> list[dict[str, Any]]:
    payload = client.get_datasources()
    rows = payload.get("data") or []
    if not isinstance(rows, list):
        raise ValueError(f"Unexpected datasources data shape: {type(rows).__name__}")
    normalized: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        normalized.append(
            {
                "datasource": str(row.get("datasource", "")),
                "name": row.get("name"),
                "units": row.get("units"),
            }
        )
    normalized.sort(key=lambda x: x["datasource"])
    return normalized


def resolve_target_countries(
    *,
    config: dict[str, Any],
    all_countries: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    requested = [r for r in parse_entities_from_config(config) if r.get("enabled")]
    requested_by_iso = {r["iso2"]: r for r in requested}
    resolved: list[dict[str, Any]] = []
    for row in all_countries:
        code = str(row.get("code", "")).upper()
        if code not in requested_by_iso:
            continue
        attrs = row.get("attrs") if isinstance(row.get("attrs"), dict) else {}
        resolved.append(
            {
                "entity_type": "country",
                "entity_id": code,
                "entity_code": code,
                "entity_name": row.get("name"),
                "iso2": code,
                "parent_country_id": None,
                "parent_country_name": None,
                "attrs": attrs,
                "config_name": requested_by_iso[code].get("name"),
            }
        )
    resolved.sort(key=lambda x: x["entity_id"])
    missing = sorted(set(requested_by_iso) - {r["entity_id"] for r in resolved})
    if missing:
        raise ValueError(f"Requested country codes not found in IODA metadata: {missing}")
    return resolved


def discover_regions_for_countries(
    client: IODAClient,
    countries: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for c in countries:
        country_code = str(c["entity_id"])
        rows = list_entities(client, entity_type="region", related_to=f"country/{country_code}", limit=500)
        for row in rows:
            attrs = row.get("attrs") if isinstance(row.get("attrs"), dict) else {}
            out.append(
                {
                    "entity_type": "region",
                    "entity_id": str(row.get("code")),
                    "entity_code": str(row.get("code")),
                    "entity_name": row.get("name"),
                    "iso2": None,
                    "parent_country_id": attrs.get("country_code") or country_code,
                    "parent_country_name": attrs.get("country_name"),
                    "attrs": attrs,
                }
            )
    out.sort(key=lambda x: (str(x.get("parent_country_id") or ""), str(x["entity_id"])))
    return out


def _window_has_data(
    client: IODAClient,
    *,
    entity_type: str,
    entity_code: str,
    datasource: str,
    start: datetime,
    end: datetime,
    max_points: int = 16,
) -> tuple[bool, tuple[int | None, int | None]]:
    resp = client.get_signals_raw(
        entity_type=entity_type,
        entity_code=entity_code,
        from_ts=to_epoch_seconds(start),
        until_ts=to_epoch_seconds(end),
        datasource=datasource,
        max_points=max_points,
    )
    payload = resp.json_data
    has_data = payload_has_data(payload)
    bounds = payload_time_bounds(payload) if has_data else (None, None)
    return has_data, bounds


def _range_has_data_chunked(
    client: IODAClient,
    *,
    entity_type: str,
    entity_code: str,
    datasource: str,
    start: datetime,
    end: datetime,
    probe_chunk_days: int = 90,
    max_points: int = 8,
) -> bool:
    cur = start
    while cur < end:
        nxt = min(cur + timedelta(days=probe_chunk_days), end)
        ok, _ = _window_has_data(
            client,
            entity_type=entity_type,
            entity_code=entity_code,
            datasource=datasource,
            start=cur,
            end=nxt,
            max_points=max_points,
        )
        if ok:
            return True
        cur = nxt
    return False


def _binary_search_first_true(items: list[Any], predicate) -> Any | None:
    lo = 0
    hi = len(items) - 1
    ans_idx: int | None = None
    while lo <= hi:
        mid = (lo + hi) // 2
        if predicate(items[mid]):
            ans_idx = mid
            hi = mid - 1
        else:
            lo = mid + 1
    if ans_idx is None:
        return None
    return items[ans_idx]


def infer_coverage_for_entity_metric(
    client: IODAClient,
    *,
    entity_type: str,
    entity_code: str,
    datasource: str,
    recent_days: int = 30,
    earliest_floor_year: int = 2000,
) -> CoverageResult:
    now = utc_now()
    checked_at = isoformat_utc(now) or ""
    recent_start = now - timedelta(days=recent_days)
    try:
        recent_has_data, recent_bounds = _window_has_data(
            client,
            entity_type=entity_type,
            entity_code=entity_code,
            datasource=datasource,
            start=recent_start,
            end=now,
            max_points=256,
        )
    except IODATransientError:
        return CoverageResult(
            entity_type=entity_type,
            entity_code=entity_code,
            metric=datasource,
            earliest_ts=None,
            latest_ts=None,
            status="transient_error",
            method="probe_recent_failed",
            checked_at_utc=checked_at,
        )

    if not recent_has_data:
        return CoverageResult(
            entity_type=entity_type,
            entity_code=entity_code,
            metric=datasource,
            earliest_ts=None,
            latest_ts=None,
            status="no_recent_data",
            method="probe_recent_empty",
            checked_at_utc=checked_at,
        )

    latest_ts = recent_bounds[1]
    current_year = now.year
    years = list(range(earliest_floor_year, current_year + 1))

    year_cache: dict[int, bool] = {}

    def year_has_data(year: int) -> bool:
        if year in year_cache:
            return year_cache[year]
        start = datetime(year, 1, 1, tzinfo=UTC)
        end = datetime(year + 1, 1, 1, tzinfo=UTC) if year < current_year else now
        ok = _range_has_data_chunked(
            client,
            entity_type=entity_type,
            entity_code=entity_code,
            datasource=datasource,
            start=start,
            end=end,
            probe_chunk_days=90,
            max_points=8,
        )
        year_cache[year] = ok
        return ok

    first_year = _binary_search_first_true(years, year_has_data)
    if first_year is None:
        # Fallback if monotonic assumption fails: linear scan.
        for y in years:
            if year_has_data(y):
                first_year = y
                break
    if first_year is None:
        return CoverageResult(
            entity_type=entity_type,
            entity_code=entity_code,
            metric=datasource,
            earliest_ts=None,
            latest_ts=latest_ts,
            status="latest_only",
            method="probe_years_no_start",
            checked_at_utc=checked_at,
        )

    months = list(range(1, 13))
    month_cache: dict[int, bool] = {}

    def month_has_data(month: int) -> bool:
        if month in month_cache:
            return month_cache[month]
        start = datetime(first_year, month, 1, tzinfo=UTC)
        if month == 12:
            end = datetime(first_year + 1, 1, 1, tzinfo=UTC)
        else:
            end = datetime(first_year, month + 1, 1, tzinfo=UTC)
        if start >= now:
            month_cache[month] = False
            return False
        if end > now:
            end = now
        ok, _ = _window_has_data(
            client,
            entity_type=entity_type,
            entity_code=entity_code,
            datasource=datasource,
            start=start,
            end=end,
            max_points=8,
        )
        month_cache[month] = ok
        return ok

    first_month = _binary_search_first_true(months, month_has_data)
    if first_month is None:
        for m in months:
            if month_has_data(m):
                first_month = m
                break
    if first_month is None:
        return CoverageResult(
            entity_type=entity_type,
            entity_code=entity_code,
            metric=datasource,
            earliest_ts=None,
            latest_ts=latest_ts,
            status="latest_only",
            method="probe_months_no_start",
            checked_at_utc=checked_at,
        )

    if first_month == 12:
        month_end = datetime(first_year + 1, 1, 1, tzinfo=UTC)
    else:
        month_end = datetime(first_year, first_month + 1, 1, tzinfo=UTC)
    if month_end > now:
        month_end = now
    month_start = datetime(first_year, first_month, 1, tzinfo=UTC)
    num_days = (month_end.date() - month_start.date()).days or 1
    days = list(range(1, num_days + 1))
    day_cache: dict[int, bool] = {}

    def day_has_data(day: int) -> bool:
        if day in day_cache:
            return day_cache[day]
        start = datetime(first_year, first_month, day, tzinfo=UTC)
        end = min(start + timedelta(days=1), now)
        ok, _ = _window_has_data(
            client,
            entity_type=entity_type,
            entity_code=entity_code,
            datasource=datasource,
            start=start,
            end=end,
            max_points=32,
        )
        day_cache[day] = ok
        return ok

    first_day = _binary_search_first_true(days, day_has_data)
    if first_day is None:
        for d in days:
            if day_has_data(d):
                first_day = d
                break
    if first_day is None:
        return CoverageResult(
            entity_type=entity_type,
            entity_code=entity_code,
            metric=datasource,
            earliest_ts=None,
            latest_ts=latest_ts,
            status="latest_only",
            method="probe_days_no_start",
            checked_at_utc=checked_at,
        )

    final_day_start = datetime(first_year, first_month, first_day, tzinfo=UTC)
    final_day_end = min(final_day_start + timedelta(days=1), now)
    _, day_bounds = _window_has_data(
        client,
        entity_type=entity_type,
        entity_code=entity_code,
        datasource=datasource,
        start=final_day_start,
        end=final_day_end,
        max_points=10000,
    )
    earliest_ts = day_bounds[0] if day_bounds[0] is not None else to_epoch_seconds(final_day_start)
    return CoverageResult(
        entity_type=entity_type,
        entity_code=entity_code,
        metric=datasource,
        earliest_ts=earliest_ts,
        latest_ts=latest_ts,
        status="ok",
        method="probe_year_month_day",
        checked_at_utc=checked_at,
    )


def _coverage_cache_key(entity_type: str, entity_code: str, metric: str) -> str:
    return f"{entity_type}|{entity_code}|{metric}"


def load_coverage_cache(path: Path) -> dict[str, dict[str, Any]]:
    data = json_load(path, default={}) or {}
    if not isinstance(data, dict):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for k, v in data.items():
        if isinstance(v, dict):
            out[str(k)] = v
    return out


def save_coverage_cache(path: Path, cache: dict[str, dict[str, Any]]) -> None:
    json_dump(path, cache, indent=2)


def discover_coverage(
    client: IODAClient,
    *,
    entities: list[dict[str, Any]],
    metrics: list[str],
    cache_path: Path,
    recent_days: int = 30,
    earliest_floor_year: int = 2000,
    refresh: bool = False,
) -> list[dict[str, Any]]:
    cache = load_coverage_cache(cache_path)
    rows: list[dict[str, Any]] = []
    for entity in entities:
        entity_type = entity["entity_type"]
        entity_code = str(entity["entity_code"])
        for metric in metrics:
            key = _coverage_cache_key(entity_type, entity_code, metric)
            if (not refresh) and key in cache:
                cached = cache[key]
                rows.append(
                    {
                        "entity_type": entity_type,
                        "entity_id": entity_code,
                        "metric": metric,
                        "coverage_min_ts": cached.get("earliest_ts"),
                        "coverage_max_ts": cached.get("latest_ts"),
                        "coverage_status": cached.get("status"),
                        "coverage_method": cached.get("method"),
                        "coverage_checked_at_utc": cached.get("checked_at_utc"),
                        "coverage_source": "cache",
                    }
                )
                continue
            result = infer_coverage_for_entity_metric(
                client,
                entity_type=entity_type,
                entity_code=entity_code,
                datasource=metric,
                recent_days=recent_days,
                earliest_floor_year=earliest_floor_year,
            )
            cache[key] = {
                "earliest_ts": result.earliest_ts,
                "latest_ts": result.latest_ts,
                "status": result.status,
                "method": result.method,
                "checked_at_utc": result.checked_at_utc,
            }
            rows.append(
                {
                    "entity_type": entity_type,
                    "entity_id": entity_code,
                    "metric": metric,
                    "coverage_min_ts": result.earliest_ts,
                    "coverage_max_ts": result.latest_ts,
                    "coverage_status": result.status,
                    "coverage_method": result.method,
                    "coverage_checked_at_utc": result.checked_at_utc,
                    "coverage_source": "probe",
                }
            )
            save_coverage_cache(cache_path, cache)
    save_coverage_cache(cache_path, cache)
    return rows


def build_entity_catalog_dataframe(
    *,
    countries: list[dict[str, Any]],
    regions: list[dict[str, Any]],
    datasources: list[dict[str, Any]],
    coverage_rows: list[dict[str, Any]],
) -> pd.DataFrame:
    metric_meta = {d["datasource"]: d for d in datasources}
    entity_rows = countries + regions
    coverage_index = {
        (row["entity_type"], str(row["entity_id"]), row["metric"]): row
        for row in coverage_rows
    }
    all_metrics = [d["datasource"] for d in datasources]
    rows: list[dict[str, Any]] = []
    for e in entity_rows:
        for metric in all_metrics:
            cov = coverage_index.get((e["entity_type"], str(e["entity_id"]), metric), {})
            mm = metric_meta.get(metric, {})
            rows.append(
                {
                    "level": "country" if e["entity_type"] == "country" else "region",
                    "entity_type": e["entity_type"],
                    "entity_id": str(e["entity_id"]),
                    "entity_code": str(e["entity_code"]),
                    "entity_name": e.get("entity_name"),
                    "iso2": e.get("iso2"),
                    "parent_country_id": e.get("parent_country_id"),
                    "parent_country_name": e.get("parent_country_name"),
                    "attrs_json": json.dumps(e.get("attrs") or {}, sort_keys=True),
                    "metric": metric,
                    "metric_name": mm.get("name"),
                    "unit": mm.get("units"),
                    "coverage_min_ts": cov.get("coverage_min_ts"),
                    "coverage_max_ts": cov.get("coverage_max_ts"),
                    "coverage_min_utc": epoch_to_utc_string(cov.get("coverage_min_ts")),
                    "coverage_max_utc": epoch_to_utc_string(cov.get("coverage_max_ts")),
                    "coverage_status": cov.get("coverage_status"),
                    "coverage_method": cov.get("coverage_method"),
                    "coverage_checked_at_utc": cov.get("coverage_checked_at_utc"),
                    "coverage_source": cov.get("coverage_source"),
                }
            )
    df = pd.DataFrame(rows)
    if not df.empty:
        for col in ["coverage_min_utc", "coverage_max_utc", "coverage_checked_at_utc"]:
            df[col] = coerce_utc_series(df[col])
        df = df.sort_values(["level", "entity_id", "metric"]).reset_index(drop=True)
    return df


def write_entity_catalog_markdown(
    *,
    path: Path,
    countries: list[dict[str, Any]],
    regions: list[dict[str, Any]],
    datasources: list[dict[str, Any]],
    catalog_df: pd.DataFrame,
) -> None:
    ensure_dir(path.parent)
    region_counts: dict[str, int] = {}
    for r in regions:
        key = str(r.get("parent_country_id") or "")
        region_counts[key] = region_counts.get(key, 0) + 1

    lines: list[str] = []
    lines.append("# IODA Entity Catalog (West Africa)")
    lines.append("")
    lines.append(f"- Generated at: `{isoformat_utc(utc_now())}`")
    lines.append(f"- Countries discovered (target set): `{len(countries)}`")
    lines.append(f"- Regions discovered (target set): `{len(regions)}`")
    lines.append(f"- Datasources discovered: `{len(datasources)}`")
    lines.append("")
    lines.append("## Datasources")
    lines.append("")
    for d in datasources:
        lines.append(f"- `{d['datasource']}`: {d.get('name','')} (units: {d.get('units','')})")
    lines.append("")
    lines.append("## Countries and Region Counts")
    lines.append("")
    lines.append("| Country Code | Country Name | Regions |")
    lines.append("|---|---|---:|")
    for c in countries:
        cc = str(c["entity_id"])
        lines.append(f"| {cc} | {c.get('entity_name','')} | {region_counts.get(cc, 0)} |")
    lines.append("")
    if not catalog_df.empty:
        cov = catalog_df.copy()
        total_rows = len(cov)
        ok_rows = int((cov["coverage_status"] == "ok").sum()) if "coverage_status" in cov else 0
        lines.append("## Coverage Summary")
        lines.append("")
        lines.append(f"- Catalog rows (entity x metric): `{total_rows}`")
        lines.append(f"- Coverage rows with status `ok`: `{ok_rows}`")
        if "coverage_status" in cov:
            status_counts = cov["coverage_status"].fillna("null").value_counts().to_dict()
            lines.append("- Coverage status counts:")
            for k, v in sorted(status_counts.items()):
                lines.append(f"  - `{k}`: {v}")
        sample = cov[
            [
                "level",
                "entity_id",
                "entity_name",
                "metric",
                "coverage_min_utc",
                "coverage_max_utc",
                "coverage_status",
            ]
        ].head(20)
        if not sample.empty:
            lines.append("")
            lines.append("### Sample Rows (first 20)")
            lines.append("")
            lines.append("| level | entity_id | entity_name | metric | min | max | status |")
            lines.append("|---|---|---|---|---|---|---|")
            for _, row in sample.iterrows():
                minv = row["coverage_min_utc"]
                maxv = row["coverage_max_utc"]
                if pd.notna(minv):
                    minv = pd.Timestamp(minv).isoformat()
                else:
                    minv = ""
                if pd.notna(maxv):
                    maxv = pd.Timestamp(maxv).isoformat()
                else:
                    maxv = ""
                lines.append(
                    f"| {row['level']} | {row['entity_id']} | {row['entity_name']} | {row['metric']} | {minv} | {maxv} | {row['coverage_status'] or ''} |"
                )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_discovery(
    *,
    config_path: Path = REPO_ROOT / "config" / "west_africa.yaml",
    entity_catalog_parquet: Path = REPO_ROOT / "data" / "processed" / "entity_catalog.parquet",
    entity_catalog_markdown: Path = REPO_ROOT / "docs" / "entity_catalog.md",
    request_log_path: Path = REPO_ROOT / "data" / "logs" / "requests.ndjson",
    user_agent: str = "ioda-west-africa-pipeline/0.1",
    timeout_seconds: float = 60.0,
    min_interval_seconds: float = 0.5,
    max_retries: int = 5,
    include_regions: bool = True,
    probe_coverage_flag: bool = True,
    metrics: list[str] | None = None,
    limit_entities: int | None = None,
    refresh_coverage: bool = False,
) -> pd.DataFrame:
    config = load_yaml(config_path)
    discovery_cfg = config.get("discovery") or {}
    cache_rel = discovery_cfg.get("coverage_cache_path", "data/intermediate/coverage_cache.json")
    cache_path = (REPO_ROOT / cache_rel).resolve() if not str(cache_rel).startswith("/") else Path(cache_rel)
    recent_days = int(discovery_cfg.get("recent_days_check", 30))
    earliest_floor_year = int(discovery_cfg.get("earliest_search_floor_year", 2000))

    request_cfg = ((config.get("fetch_defaults") or {}).get("request") or {})
    ua = user_agent or str(request_cfg.get("user_agent") or "ioda-west-africa-pipeline/0.1")
    timeout_seconds = float(timeout_seconds or request_cfg.get("timeout_seconds") or 60.0)
    min_interval_seconds = float(min_interval_seconds or request_cfg.get("min_interval_seconds") or 0.5)
    max_retries = int(max_retries or request_cfg.get("max_retries") or 5)

    with IODAClient(
        user_agent=ua,
        timeout_seconds=timeout_seconds,
        min_interval_seconds=min_interval_seconds,
        max_retries=max_retries,
        request_log_path=request_log_path,
    ) as client:
        all_countries = list_entities(client, entity_type="country", limit=500)
        datasources = list_datasources(client)
        target_countries = resolve_target_countries(config=config, all_countries=all_countries)
        if limit_entities is not None:
            target_countries = target_countries[: max(0, int(limit_entities))]
        target_regions = discover_regions_for_countries(client, target_countries) if include_regions else []
        if limit_entities is not None and include_regions:
            allowed = {c["entity_id"] for c in target_countries}
            target_regions = [r for r in target_regions if r.get("parent_country_id") in allowed]

        metric_list = [d["datasource"] for d in datasources] if not metrics else sorted(set(metrics))
        entities_for_coverage = target_countries + target_regions
        coverage_rows: list[dict[str, Any]] = []
        if probe_coverage_flag:
            coverage_rows = discover_coverage(
                client,
                entities=entities_for_coverage,
                metrics=metric_list,
                cache_path=cache_path,
                recent_days=recent_days,
                earliest_floor_year=earliest_floor_year,
                refresh=refresh_coverage,
            )
        else:
            coverage_rows = []

    catalog_df = build_entity_catalog_dataframe(
        countries=target_countries,
        regions=target_regions,
        datasources=datasources,
        coverage_rows=coverage_rows,
    )
    dataframe_to_parquet(catalog_df, entity_catalog_parquet)
    write_entity_catalog_markdown(
        path=entity_catalog_markdown,
        countries=target_countries,
        regions=target_regions,
        datasources=datasources,
        catalog_df=catalog_df,
    )

    generated_cov = [
        {
            "entity_type": r.get("entity_type"),
            "entity_id": r.get("entity_id"),
            "metric": r.get("metric"),
            "coverage_min_ts": r.get("coverage_min_ts"),
            "coverage_max_ts": r.get("coverage_max_ts"),
            "coverage_status": r.get("coverage_status"),
            "coverage_method": r.get("coverage_method"),
            "coverage_checked_at_utc": r.get("coverage_checked_at_utc"),
            "coverage_source": r.get("coverage_source"),
        }
        for r in coverage_rows
    ]
    update_generated_config_sections(
        config,
        datasources=datasources,
        resolved_countries=[
            {
                "entity_id": c["entity_id"],
                "entity_name": c.get("entity_name"),
                "iso2": c.get("iso2"),
                "attrs": c.get("attrs"),
            }
            for c in target_countries
        ],
        resolved_regions=[
            {
                "entity_id": r["entity_id"],
                "entity_name": r.get("entity_name"),
                "parent_country_id": r.get("parent_country_id"),
                "parent_country_name": r.get("parent_country_name"),
                "attrs": r.get("attrs"),
            }
            for r in target_regions
        ],
        coverage_rows=generated_cov,
    )
    save_yaml(config_path, config)
    return catalog_df


def load_entity_catalog(path: Path | None = None) -> pd.DataFrame:
    if path is None:
        path = REPO_ROOT / "data" / "processed" / "entity_catalog.parquet"
    df = read_parquet_if_exists(path)
    if df is None:
        raise FileNotFoundError(f"Entity catalog not found: {path}")
    return df
