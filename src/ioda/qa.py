from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from .utils import REPO_ROOT, dataframe_to_parquet, ensure_dir, isoformat_utc, utc_now


def _group_metric_key(df: pd.DataFrame) -> pd.Series:
    variants = df["series_variant"].fillna("").astype(str)
    return df["metric"].astype(str) + variants.map(lambda x: "" if not x else f"__{x}")


def _compute_group_stats(group: pd.DataFrame) -> pd.Series:
    g = group.sort_values("timestamp_utc").copy()
    values = pd.to_numeric(g["value"], errors="coerce")
    ts = pd.to_datetime(g["timestamp_utc"], utc=True, errors="coerce")
    non_null = values.notna()
    ts_non_null = ts[non_null]
    diffs = ts_non_null.diff().dt.total_seconds().dropna()
    median_step = float(diffs.median()) if not diffs.empty else None
    max_gap = float(diffs.max()) if not diffs.empty else None
    gap_count = 0
    if median_step and median_step > 0 and not diffs.empty:
        gap_count = int((diffs > (1.5 * median_step)).sum())

    negative_count = int((values < 0).fillna(False).sum())

    unit_text = str(g["unit"].dropna().iloc[0]) if ("unit" in g.columns and g["unit"].notna().any()) else ""
    metric_text = str(g["metric"].iloc[0]) if "metric" in g.columns and len(g) else ""
    upper_bound_violations = 0
    if "Percentage" in unit_text:
        upper_bound_violations = int(((values > 100) | (values < 0)).fillna(False).sum())
    elif "Normalized" in unit_text or "norm" in metric_text:
        upper_bound_violations = int(((values > 10) | (values < 0)).fillna(False).sum())

    spike_count = 0
    vn = values.dropna()
    if len(vn) >= 8:
        q1 = vn.quantile(0.25)
        q3 = vn.quantile(0.75)
        iqr = q3 - q1
        if iqr > 0:
            spike_thresh = q3 + 10 * iqr
            spike_count = int((vn > spike_thresh).sum())

    duplicate_rows = 0
    if "duplicate_key_count" in g.columns:
        duplicate_rows = int((g["duplicate_key_count"].fillna(1) > 1).sum())

    return pd.Series(
        {
            "min_timestamp_utc": ts.min(),
            "max_timestamp_utc": ts.max(),
            "n_rows": int(len(g)),
            "n_non_null": int(non_null.sum()),
            "n_null": int((~non_null).sum()),
            "null_fraction": float((~non_null).mean()) if len(g) else 0.0,
            "median_step_seconds": median_step,
            "max_gap_seconds": max_gap,
            "gap_count": gap_count,
            "negative_count": negative_count,
            "bounded_range_violations": upper_bound_violations,
            "spike_count": spike_count,
            "duplicate_rows": duplicate_rows,
        }
    )


def build_qa_summary(long_df: pd.DataFrame) -> pd.DataFrame:
    if long_df.empty:
        return pd.DataFrame()
    df = long_df.copy()
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
    df["metric_key"] = _group_metric_key(df)
    group_cols = ["level", "entity_id", "entity_name", "metric", "series_variant", "metric_key", "unit"]
    summary = df.groupby(group_cols, dropna=False, observed=False).apply(_compute_group_stats).reset_index()
    summary = summary.sort_values(["level", "entity_id", "metric_key"]).reset_index(drop=True)
    # Compatibility columns for since-last-run lookup on base metric.
    summary["max_timestamp_utc"] = pd.to_datetime(summary["max_timestamp_utc"], utc=True, errors="coerce")
    return summary


def write_qa_report(path: Path, summary_df: pd.DataFrame) -> None:
    ensure_dir(path.parent)
    lines: list[str] = []
    lines.append("# QA Report")
    lines.append("")
    lines.append(f"- Generated at: `{isoformat_utc(utc_now())}`")
    if summary_df.empty:
        lines.append("- No data available (`ioda_long.parquet` empty or missing).")
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return

    lines.append(f"- QA groups (entity x metric x variant): `{len(summary_df)}`")
    lines.append(f"- Total rows represented: `{int(summary_df['n_rows'].sum())}`")
    lines.append("")

    status_cols = ["negative_count", "bounded_range_violations", "spike_count", "duplicate_rows", "gap_count"]
    lines.append("## Totals")
    lines.append("")
    for c in status_cols:
        lines.append(f"- `{c}` total: `{int(summary_df[c].fillna(0).sum())}`")
    lines.append("")

    def _add_table(title: str, df: pd.DataFrame, cols: list[str], n: int = 10) -> None:
        lines.append(f"## {title}")
        lines.append("")
        if df.empty:
            lines.append("- None")
            lines.append("")
            return
        sub = df[cols].head(n).copy()
        lines.append("| " + " | ".join(cols) + " |")
        lines.append("|" + "|".join(["---"] * len(cols)) + "|")
        for _, row in sub.iterrows():
            vals: list[str] = []
            for c in cols:
                v: Any = row[c]
                if pd.isna(v):
                    vals.append("")
                elif isinstance(v, pd.Timestamp):
                    vals.append(v.isoformat())
                elif isinstance(v, float):
                    vals.append(f"{v:.4f}")
                else:
                    vals.append(str(v))
            lines.append("| " + " | ".join(vals) + " |")
        lines.append("")

    _add_table(
        "Highest Missingness",
        summary_df.sort_values(["null_fraction", "n_rows"], ascending=[False, False]),
        ["level", "entity_id", "metric_key", "n_rows", "n_null", "null_fraction", "min_timestamp_utc", "max_timestamp_utc"],
    )
    _add_table(
        "Largest Gaps",
        summary_df.sort_values(["max_gap_seconds", "gap_count"], ascending=[False, False]),
        ["level", "entity_id", "metric_key", "median_step_seconds", "max_gap_seconds", "gap_count"],
    )
    _add_table(
        "Potential Anomalies",
        summary_df.assign(
            anomaly_total=summary_df[["negative_count", "bounded_range_violations", "spike_count"]].fillna(0).sum(axis=1)
        ).sort_values(["anomaly_total", "spike_count"], ascending=[False, False]),
        ["level", "entity_id", "metric_key", "negative_count", "bounded_range_violations", "spike_count"],
    )
    _add_table(
        "Duplicates",
        summary_df[summary_df["duplicate_rows"].fillna(0) > 0].sort_values("duplicate_rows", ascending=False),
        ["level", "entity_id", "metric_key", "duplicate_rows"],
    )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_qa(
    *,
    long_path: Path = REPO_ROOT / "data" / "processed" / "ioda_long.parquet",
    qa_summary_path: Path = REPO_ROOT / "data" / "processed" / "qa_summary.parquet",
    qa_report_path: Path = REPO_ROOT / "docs" / "qa_report.md",
) -> pd.DataFrame:
    if not long_path.exists():
        summary_df = pd.DataFrame(
            columns=[
                "level",
                "entity_id",
                "entity_name",
                "metric",
                "series_variant",
                "metric_key",
                "unit",
                "min_timestamp_utc",
                "max_timestamp_utc",
                "n_rows",
                "n_non_null",
                "n_null",
                "null_fraction",
                "median_step_seconds",
                "max_gap_seconds",
                "gap_count",
                "negative_count",
                "bounded_range_violations",
                "spike_count",
                "duplicate_rows",
            ]
        )
        write_qa_report(qa_report_path, summary_df)
        dataframe_to_parquet(summary_df, qa_summary_path)
        return summary_df
    long_df = pd.read_parquet(long_path)
    summary_df = build_qa_summary(long_df)
    dataframe_to_parquet(summary_df, qa_summary_path)
    write_qa_report(qa_report_path, summary_df)
    return summary_df
