from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from .api import IODAAPIError, IODAClient, IODATransientError
from .discover import load_entity_catalog
from .utils import (
    REPO_ROOT,
    TimeWindow,
    chunk_range,
    ensure_dir,
    parse_dateish,
    sanitize_path_component,
    to_epoch_seconds,
    utc_now,
)


CHUNK_ORDER = ["month", "week", "day"]


class ChunkTooLargeError(RuntimeError):
    pass


@dataclass(frozen=True)
class FetchTarget:
    level: str
    entity_type: str
    entity_id: str
    entity_name: str
    parent_country_id: str | None
    metric: str
    unit: str | None
    coverage_min_ts: int | None
    coverage_max_ts: int | None


@dataclass
class FetchSummary:
    targets: int = 0
    planned_chunks: int = 0
    written_chunks: int = 0
    skipped_existing: int = 0
    dry_run_chunks: int = 0
    errors: int = 0


def _norm_level(entity_type: str) -> str:
    return "country" if entity_type == "country" else "region"


def _select_targets_from_catalog(
    catalog_df: pd.DataFrame,
    *,
    level: str,
    metrics: list[str] | None,
    limit_entities: int | None = None,
) -> list[FetchTarget]:
    df = catalog_df.copy()
    if df.empty:
        return []
    if level != "both":
        df = df[df["level"] == level].copy()
    if metrics:
        df = df[df["metric"].isin(metrics)].copy()
    # one row per entity-metric already expected, but keep deterministic.
    df = df.sort_values(["level", "entity_id", "metric"]).drop_duplicates(
        subset=["level", "entity_id", "metric"], keep="first"
    )
    if limit_entities is not None:
        n = max(0, int(limit_entities))
        entities = df[["level", "entity_id"]].drop_duplicates().sort_values(["level", "entity_id"])
        if level == "both":
            limited_entities = (
                entities.groupby("level", group_keys=False, sort=True)
                .head(n)
                .reset_index(drop=True)
            )
        else:
            limited_entities = entities.head(n)
        df = df.merge(limited_entities.assign(_keep=1), on=["level", "entity_id"], how="inner")
        df = df.drop(columns=["_keep"])
    out: list[FetchTarget] = []
    for _, r in df.iterrows():
        out.append(
            FetchTarget(
                level=str(r["level"]),
                entity_type=str(r["entity_type"]),
                entity_id=str(r["entity_id"]),
                entity_name=str(r.get("entity_name") or r["entity_id"]),
                parent_country_id=(None if pd.isna(r.get("parent_country_id")) else str(r.get("parent_country_id"))),
                metric=str(r["metric"]),
                unit=None if pd.isna(r.get("unit")) else str(r.get("unit")),
                coverage_min_ts=None if pd.isna(r.get("coverage_min_ts")) else int(r.get("coverage_min_ts")),
                coverage_max_ts=None if pd.isna(r.get("coverage_max_ts")) else int(r.get("coverage_max_ts")),
            )
        )
    return out


def _load_last_run_max_timestamps(
    *,
    long_path: Path = REPO_ROOT / "data" / "processed" / "ioda_long.parquet",
    qa_summary_path: Path = REPO_ROOT / "data" / "processed" / "qa_summary.parquet",
) -> dict[tuple[str, str, str], pd.Timestamp]:
    out: dict[tuple[str, str, str], pd.Timestamp] = {}
    df: pd.DataFrame | None = None
    if qa_summary_path.exists():
        df = pd.read_parquet(qa_summary_path)
        expected = {"level", "entity_id", "metric", "max_timestamp_utc"}
        if not expected.issubset(df.columns):
            df = None
    if df is None and long_path.exists():
        long_df = pd.read_parquet(long_path, columns=["level", "entity_id", "metric", "timestamp_utc"])
        if not long_df.empty:
            long_df["timestamp_utc"] = pd.to_datetime(long_df["timestamp_utc"], utc=True, errors="coerce")
            df = (
                long_df.groupby(["level", "entity_id", "metric"], dropna=False)["timestamp_utc"]
                .max()
                .reset_index()
                .rename(columns={"timestamp_utc": "max_timestamp_utc"})
            )
    if df is None or df.empty:
        return out
    df["max_timestamp_utc"] = pd.to_datetime(df["max_timestamp_utc"], utc=True, errors="coerce")
    for _, r in df.iterrows():
        ts = r.get("max_timestamp_utc")
        if pd.isna(ts):
            continue
        out[(str(r["level"]), str(r["entity_id"]), str(r["metric"]))] = pd.Timestamp(ts)
    return out


def _resolve_bounds(
    target: FetchTarget,
    *,
    start_dt: datetime | None,
    end_dt: datetime | None,
    since_last_run: bool,
    last_run_lookup: dict[tuple[str, str, str], pd.Timestamp],
) -> tuple[datetime, datetime] | None:
    end = end_dt or utc_now()
    if since_last_run:
        prev = last_run_lookup.get((target.level, target.entity_id, target.metric))
        if prev is not None and pd.notna(prev):
            start = prev.to_pydatetime().astimezone(UTC) + timedelta(seconds=1)
        else:
            start = start_dt
    else:
        start = start_dt

    if start is None:
        if target.coverage_min_ts is not None:
            start = datetime.fromtimestamp(target.coverage_min_ts, tz=UTC)
        else:
            start = end - timedelta(days=30)

    if target.coverage_max_ts is not None:
        cov_end = datetime.fromtimestamp(target.coverage_max_ts, tz=UTC) + timedelta(days=1)
        end = min(end, cov_end)
    if end <= start:
        return None
    return start, end


def _raw_output_path(base_dir: Path, target: FetchTarget, window: TimeWindow) -> Path:
    metric_dir = sanitize_path_component(target.metric)
    entity_dir = sanitize_path_component(target.entity_id)
    return (
        base_dir
        / target.level
        / metric_dir
        / entity_dir
        / f"{window.filename_stem()}.json"
    )


def _write_bytes(path: Path, body: bytes) -> None:
    ensure_dir(path.parent)
    path.write_bytes(body)


def _fetch_single_chunk(
    client: IODAClient,
    *,
    target: FetchTarget,
    window: TimeWindow,
    max_points: int,
    max_response_bytes: int,
) -> bytes:
    resp = client.get_signals_raw(
        entity_type=target.entity_type,
        entity_code=target.entity_id,
        from_ts=to_epoch_seconds(window.start),
        until_ts=to_epoch_seconds(window.end),
        datasource=target.metric,
        max_points=max_points,
    )
    if resp.size_bytes > max_response_bytes:
        raise ChunkTooLargeError(
            f"Response too large ({resp.size_bytes} bytes > {max_response_bytes}) for "
            f"{target.entity_type}/{target.entity_id}/{target.metric} {window.filename_stem()}"
        )
    return resp.body_bytes


def _recursive_fetch_window(
    client: IODAClient,
    *,
    target: FetchTarget,
    window: TimeWindow,
    chunk_mode: str,
    base_raw_dir: Path,
    max_points: int,
    max_response_bytes: int,
    dry_run: bool,
    overwrite: bool,
    summary: FetchSummary,
) -> None:
    if chunk_mode not in CHUNK_ORDER:
        raise ValueError(f"Unsupported chunk mode: {chunk_mode}")

    path = _raw_output_path(base_raw_dir, target, window)
    summary.planned_chunks += 1

    if dry_run:
        print(
            f"PLAN {target.level}/{target.entity_id}/{target.metric} "
            f"{window.start.isoformat()} -> {window.end.isoformat()} [{chunk_mode}]"
        )
        summary.dry_run_chunks += 1
        return

    if path.exists() and not overwrite:
        summary.skipped_existing += 1
        return

    try:
        body = _fetch_single_chunk(
            client,
            target=target,
            window=window,
            max_points=max_points,
            max_response_bytes=max_response_bytes,
        )
        _write_bytes(path, body)
        summary.written_chunks += 1
        return
    except ChunkTooLargeError:
        recoverable = True
    except IODATransientError:
        recoverable = True
    except IODAAPIError:
        # Most 4xx are non-retriable and usually indicate a bad parameter.
        summary.errors += 1
        raise

    if not recoverable:
        return

    next_idx = CHUNK_ORDER.index(chunk_mode) + 1
    if next_idx >= len(CHUNK_ORDER):
        summary.errors += 1
        raise
    next_mode = CHUNK_ORDER[next_idx]
    for sub_start, sub_end in chunk_range(window.start, window.end, next_mode):
        _recursive_fetch_window(
            client,
            target=target,
            window=TimeWindow(sub_start, sub_end),
            chunk_mode=next_mode,
            base_raw_dir=base_raw_dir,
            max_points=max_points,
            max_response_bytes=max_response_bytes,
            dry_run=dry_run,
            overwrite=overwrite,
            summary=summary,
        )


def run_fetch(
    *,
    config_path: Path = REPO_ROOT / "config" / "west_africa.yaml",
    entity_catalog_path: Path = REPO_ROOT / "data" / "processed" / "entity_catalog.parquet",
    raw_dir: Path = REPO_ROOT / "data" / "raw",
    request_log_path: Path = REPO_ROOT / "data" / "logs" / "requests.ndjson",
    level: str = "both",
    metrics_arg: str | None = None,
    start: str | None = None,
    end: str | None = None,
    dry_run: bool = False,
    limit_entities: int | None = None,
    since_last_run: bool = False,
    overwrite: bool = False,
    user_agent: str | None = None,
    timeout_seconds: float | None = None,
    min_interval_seconds: float | None = None,
    max_retries: int | None = None,
    max_points: int | None = None,
    max_response_bytes: int | None = None,
    initial_chunk_mode: str | None = None,
) -> FetchSummary:
    import yaml

    with open(config_path, "r", encoding="utf-8") as fh:
        config = yaml.safe_load(fh) or {}

    fetch_defaults = config.get("fetch_defaults") or {}
    req_cfg = fetch_defaults.get("request") or {}
    chunk_cfg = fetch_defaults.get("chunking") or {}
    window_cfg = fetch_defaults.get("window") or {}

    ua = user_agent or str(req_cfg.get("user_agent") or "ioda-west-africa-pipeline/0.1")
    timeout_seconds = float(timeout_seconds or req_cfg.get("timeout_seconds") or 60.0)
    min_interval_seconds = float(min_interval_seconds or req_cfg.get("min_interval_seconds") or 0.5)
    max_retries = int(max_retries or req_cfg.get("max_retries") or 5)
    max_points = int(max_points or chunk_cfg.get("max_points") or 10000)
    max_response_bytes = int(max_response_bytes or chunk_cfg.get("max_response_bytes") or 5_000_000)
    initial_chunk_mode = str(initial_chunk_mode or chunk_cfg.get("initial") or "month")

    if level not in {"country", "region", "both"}:
        raise ValueError("--level must be one of: country, region, both")

    catalog_df = load_entity_catalog(entity_catalog_path)
    metrics: list[str] | None
    if not metrics_arg or metrics_arg == "auto":
        metrics = None
    else:
        metrics = [m.strip() for m in metrics_arg.split(",") if m.strip()]
        if not metrics:
            metrics = None

    targets = _select_targets_from_catalog(
        catalog_df,
        level=level,
        metrics=metrics,
        limit_entities=limit_entities,
    )

    default_start_cfg = window_cfg.get("default_start")
    default_end_cfg = window_cfg.get("default_end")
    start_input = start if start is not None else default_start_cfg
    end_input = end if end is not None else default_end_cfg

    start_dt = parse_dateish(start_input, end_of_day=False)
    end_dt = parse_dateish(end_input, end_of_day=True)
    if end_dt is None:
        end_dt = utc_now()

    last_run_lookup = _load_last_run_max_timestamps() if since_last_run else {}
    summary = FetchSummary(targets=len(targets))

    with IODAClient(
        user_agent=ua,
        timeout_seconds=timeout_seconds,
        min_interval_seconds=min_interval_seconds,
        max_retries=max_retries,
        request_log_path=request_log_path,
    ) as client:
        for target in targets:
            bounds = _resolve_bounds(
                target,
                start_dt=start_dt,
                end_dt=end_dt,
                since_last_run=since_last_run,
                last_run_lookup=last_run_lookup,
            )
            if bounds is None:
                continue
            win = TimeWindow(*bounds)
            for chunk_start, chunk_end in chunk_range(win.start, win.end, initial_chunk_mode):
                _recursive_fetch_window(
                    client,
                    target=target,
                    window=TimeWindow(chunk_start, chunk_end),
                    chunk_mode=initial_chunk_mode,
                    base_raw_dir=raw_dir,
                    max_points=max_points,
                    max_response_bytes=max_response_bytes,
                    dry_run=dry_run,
                    overwrite=overwrite,
                    summary=summary,
                )
    return summary
