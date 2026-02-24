from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from .discover import _iter_series_objects
from .utils import (
    REPO_ROOT,
    dataframe_to_parquet,
    ensure_dir,
    is_number,
    sanitize_path_component,
    stable_json_dumps,
)


@dataclass
class TransformOutputs:
    long_df: pd.DataFrame
    country_panel_df: pd.DataFrame
    region_panel_df: pd.DataFrame


def _iter_raw_json_files(raw_dir: Path) -> list[Path]:
    if not raw_dir.exists():
        return []
    return sorted(raw_dir.rglob("*.json"))


def _parse_window_from_filename(path: Path) -> tuple[int | None, int | None]:
    stem = path.stem
    parts = stem.split("_")
    if len(parts) != 2:
        return None, None
    if not all(p.isdigit() for p in parts):
        return None, None
    return int(parts[0]), int(parts[1])


def _dims_to_variant(dims: dict[str, Any], item_index: int | None = None) -> str:
    parts: list[str] = []
    for k in sorted(dims):
        v = dims[k]
        if isinstance(v, (dict, list)):
            continue
        parts.append(f"{sanitize_path_component(k)}={sanitize_path_component(v)}")
    if item_index is not None and not parts:
        parts.append(f"item={item_index}")
    return "__".join(parts)


def _series_metric_base(series: dict[str, Any]) -> tuple[str, str, str]:
    datasource = str(series.get("datasource") or "unknown")
    subtype = str(series.get("subtype") or "").strip()
    metric = datasource if not subtype else f"{datasource}__{sanitize_path_component(subtype)}"
    return datasource, subtype, metric


def _expand_nested_item(
    *,
    item: Any,
    metric_base: str,
    item_index: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if item is None:
        rows.append({"metric": metric_base, "value": None, "series_variant": f"item={item_index}", "source_fields_json": "{}"})
        return rows
    if is_number(item):
        rows.append({"metric": f"{metric_base}__item", "value": float(item), "series_variant": f"item={item_index}", "source_fields_json": "{}"})
        return rows
    if not isinstance(item, dict):
        rows.append(
            {
                "metric": metric_base,
                "value": None,
                "series_variant": f"item={item_index}",
                "source_fields_json": stable_json_dumps({"raw_type": type(item).__name__}),
            }
        )
        return rows

    if isinstance(item.get("agg_values"), dict):
        agg_values = item["agg_values"]
        dims = {k: v for k, v in item.items() if k != "agg_values"}
        variant = _dims_to_variant(dims, item_index=item_index)
        source_fields_json = stable_json_dumps(dims) if dims else "{}"
        produced = False
        for agg_key in sorted(agg_values):
            agg_val = agg_values[agg_key]
            if agg_val is None or is_number(agg_val):
                rows.append(
                    {
                        "metric": f"{metric_base}__{sanitize_path_component(agg_key)}",
                        "value": (None if agg_val is None else float(agg_val)),
                        "series_variant": variant,
                        "source_fields_json": source_fields_json,
                    }
                )
                produced = True
        if not produced:
            rows.append(
                {
                    "metric": metric_base,
                    "value": None,
                    "series_variant": variant,
                    "source_fields_json": source_fields_json,
                }
            )
        return rows

    numeric_keys = [k for k, v in item.items() if v is None or is_number(v)]
    dims = {k: v for k, v in item.items() if k not in numeric_keys}
    variant = _dims_to_variant(dims, item_index=item_index)
    source_fields_json = stable_json_dumps(dims) if dims else "{}"
    if not numeric_keys:
        rows.append({"metric": metric_base, "value": None, "series_variant": variant, "source_fields_json": source_fields_json})
        return rows
    for k in sorted(numeric_keys):
        v = item[k]
        rows.append(
            {
                "metric": f"{metric_base}__{sanitize_path_component(k)}",
                "value": (None if v is None else float(v)),
                "series_variant": variant,
                "source_fields_json": source_fields_json,
            }
        )
    return rows


def _expand_value(value: Any, metric_base: str) -> list[dict[str, Any]]:
    if value is None:
        return [{"metric": metric_base, "value": None, "series_variant": "", "source_fields_json": "{}"}]
    if is_number(value):
        return [{"metric": metric_base, "value": float(value), "series_variant": "", "source_fields_json": "{}"}]
    if isinstance(value, list):
        if not value:
            return [{"metric": metric_base, "value": None, "series_variant": "", "source_fields_json": "{}"}]
        rows: list[dict[str, Any]] = []
        for idx, item in enumerate(value):
            rows.extend(_expand_nested_item(item=item, metric_base=metric_base, item_index=idx))
        return rows
    if isinstance(value, dict):
        return _expand_nested_item(item=value, metric_base=metric_base, item_index=0)
    return [{"metric": metric_base, "value": None, "series_variant": "", "source_fields_json": stable_json_dumps({"raw_type": type(value).__name__})}]


def _load_entity_meta_from_catalog(entity_catalog_path: Path) -> pd.DataFrame:
    if not entity_catalog_path.exists():
        return pd.DataFrame(
            columns=[
                "level",
                "entity_id",
                "entity_name",
                "parent_country_id",
                "parent_country_name",
            ]
        )
    catalog = pd.read_parquet(entity_catalog_path)
    cols = ["level", "entity_id", "entity_name", "parent_country_id", "parent_country_name"]
    for c in cols:
        if c not in catalog.columns:
            catalog[c] = None
    return catalog[cols].drop_duplicates(subset=["level", "entity_id"]).copy()


def build_long_dataframe(
    *,
    raw_dir: Path = REPO_ROOT / "data" / "raw",
    entity_catalog_path: Path = REPO_ROOT / "data" / "processed" / "entity_catalog.parquet",
) -> pd.DataFrame:
    files = _iter_raw_json_files(raw_dir)
    rows: list[dict[str, Any]] = []
    for path in files:
        payload = json.loads(path.read_text(encoding="utf-8"))
        raw_start_ts, raw_end_ts = _parse_window_from_filename(path)
        for series in _iter_series_objects(payload.get("data")):
            datasource, subtype, metric_base = _series_metric_base(series)
            step = int(series.get("step") or 0)
            native_step = int(series.get("nativeStep") or 0)
            series_from = int(series.get("from") or 0)
            values = series.get("values") or []
            entity_type = str(series.get("entityType") or "")
            level = "country" if entity_type == "country" else "region" if entity_type == "region" else entity_type
            entity_id = str(series.get("entityCode") or "")
            entity_name = series.get("entityName")
            for idx, value in enumerate(values):
                if step <= 0:
                    continue
                ts = pd.Timestamp(series_from + idx * step, unit="s", tz="UTC")
                for expanded in _expand_value(value, metric_base):
                    rows.append(
                        {
                            "timestamp_utc": ts,
                            "level": level,
                            "entity_type": entity_type,
                            "entity_id": entity_id,
                            "entity_name": entity_name,
                            "parent_country_id": None,
                            "parent_country_name": None,
                            "datasource": datasource,
                            "subtype": subtype,
                            "metric": expanded["metric"],
                            "series_variant": expanded["series_variant"],
                            "value": expanded["value"],
                            "unit": None,
                            "source_fields_json": expanded["source_fields_json"],
                            "step_seconds": step,
                            "native_step_seconds": native_step,
                            "raw_file": str(path.relative_to(REPO_ROOT)),
                            "raw_window_start_ts": raw_start_ts,
                            "raw_window_end_ts": raw_end_ts,
                        }
                    )
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    entity_meta = _load_entity_meta_from_catalog(entity_catalog_path)
    for c in ["entity_id", "level"]:
        entity_meta[c] = entity_meta[c].astype(str)
    df["entity_id"] = df["entity_id"].astype(str)
    df["level"] = df["level"].astype(str)

    unit_map: dict[str, str | None] = {}
    if entity_catalog_path.exists():
        cat = pd.read_parquet(entity_catalog_path, columns=["metric", "unit"])
        if not cat.empty:
            cat = cat.dropna(subset=["metric"]).drop_duplicates(subset=["metric"], keep="first")
            unit_map = {str(r["metric"]): (None if pd.isna(r["unit"]) else str(r["unit"])) for _, r in cat.iterrows()}
    # For derived metrics (e.g. ping-slash24-latency__mean_latency), use base datasource unit.
    def _unit_for_metric(metric: str, datasource: str) -> str | None:
        return unit_map.get(metric) or unit_map.get(datasource)

    df = df.merge(entity_meta, on=["level", "entity_id"], how="left", suffixes=("", "_cat"))
    for col in ["entity_name", "parent_country_id", "parent_country_name"]:
        cat_col = f"{col}_cat"
        if cat_col in df.columns:
            df[col] = df[col].where(df[col].notna(), df[cat_col])
            df = df.drop(columns=[cat_col])

    df["unit"] = [
        _unit_for_metric(str(m), str(ds))
        for m, ds in zip(df["metric"], df["datasource"])
    ]

    # Exact duplicate collapse.
    df = df.drop_duplicates().copy()

    # Key-level dedupe rule: keep first deterministic row by source_fields/raw_file; mark collisions.
    dedupe_key = ["timestamp_utc", "level", "entity_id", "metric", "series_variant"]
    df = df.sort_values(
        dedupe_key + ["source_fields_json", "raw_file", "step_seconds", "native_step_seconds"]
    ).reset_index(drop=True)
    df["duplicate_key_count"] = df.groupby(dedupe_key, dropna=False)["metric"].transform("size")
    df["dedupe_rank"] = df.groupby(dedupe_key, dropna=False).cumcount()
    df = df[df["dedupe_rank"] == 0].drop(columns=["dedupe_rank"])

    df = df.sort_values(["level", "entity_id", "metric", "series_variant", "timestamp_utc"]).reset_index(drop=True)
    return df


def _wide_column_name(metric: str, series_variant: str) -> str:
    if not series_variant:
        return metric
    return f"{metric}__{series_variant}"


def build_wide_panels(long_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if long_df.empty:
        return pd.DataFrame(), pd.DataFrame()
    work = long_df.copy()
    for col in ["entity_name", "parent_country_id", "parent_country_name", "series_variant"]:
        if col in work.columns:
            work[col] = work[col].fillna("")
    work["panel_metric"] = [
        _wide_column_name(str(m), str(v) if pd.notna(v) else "")
        for m, v in zip(work["metric"], work["series_variant"])
    ]

    index_cols = ["timestamp_utc", "entity_id", "entity_name", "parent_country_id", "parent_country_name"]
    value_cols = ["level"] + index_cols + ["panel_metric", "value"]
    work = work[value_cols].copy()

    def _pivot(level_name: str) -> pd.DataFrame:
        sub = work[work["level"] == level_name].copy()
        if sub.empty:
            return pd.DataFrame()
        panel = sub.pivot_table(
            index=index_cols,
            columns="panel_metric",
            values="value",
            aggfunc="first",
            observed=False,
        ).reset_index()
        panel.columns.name = None
        return panel.sort_values(["entity_id", "timestamp_utc"]).reset_index(drop=True)

    return _pivot("country"), _pivot("region")


def write_processed_outputs(
    *,
    long_df: pd.DataFrame,
    country_panel_df: pd.DataFrame,
    region_panel_df: pd.DataFrame,
    processed_dir: Path = REPO_ROOT / "data" / "processed",
) -> None:
    ensure_dir(processed_dir)
    dataframe_to_parquet(long_df, processed_dir / "ioda_long.parquet")
    dataframe_to_parquet(country_panel_df, processed_dir / "ioda_country_panel.parquet")
    if not region_panel_df.empty:
        dataframe_to_parquet(region_panel_df, processed_dir / "ioda_region_panel.parquet")


def run_build_panel(
    *,
    raw_dir: Path = REPO_ROOT / "data" / "raw",
    entity_catalog_path: Path = REPO_ROOT / "data" / "processed" / "entity_catalog.parquet",
    processed_dir: Path = REPO_ROOT / "data" / "processed",
) -> TransformOutputs:
    long_df = build_long_dataframe(raw_dir=raw_dir, entity_catalog_path=entity_catalog_path)
    country_panel_df, region_panel_df = build_wide_panels(long_df)
    write_processed_outputs(
        long_df=long_df,
        country_panel_df=country_panel_df,
        region_panel_df=region_panel_df,
        processed_dir=processed_dir,
    )
    return TransformOutputs(
        long_df=long_df,
        country_panel_df=country_panel_df,
        region_panel_df=region_panel_df,
    )


def validation_samples(long_df: pd.DataFrame, max_entities_each: int = 2) -> list[str]:
    if long_df.empty:
        return ["No rows in long dataframe."]
    lines: list[str] = []
    for level in ["country", "region"]:
        sub = long_df[long_df["level"] == level].copy()
        if sub.empty:
            lines.append(f"{level}: no data")
            continue
        entities = (
            sub[["entity_id", "entity_name"]]
            .drop_duplicates()
            .sort_values(["entity_id"])
            .head(max_entities_each)
        )
        for _, ent in entities.iterrows():
            e_sub = sub[sub["entity_id"] == ent["entity_id"]].copy()
            lines.append(
                f"{level} {ent['entity_id']} ({ent['entity_name']}): "
                f"count={len(e_sub)} min_ts={e_sub['timestamp_utc'].min()} max_ts={e_sub['timestamp_utc'].max()}"
            )
            lines.append("head:")
            lines.append(
                e_sub[
                    ["timestamp_utc", "metric", "series_variant", "value"]
                ]
                .head(3)
                .to_string(index=False)
            )
            lines.append("tail:")
            lines.append(
                e_sub[
                    ["timestamp_utc", "metric", "series_variant", "value"]
                ]
                .tail(3)
                .to_string(index=False)
            )
    return lines
