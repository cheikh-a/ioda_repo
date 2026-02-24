#!/usr/bin/env python3
from __future__ import annotations

import argparse
import calendar
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ioda.fetch import run_fetch  # noqa: E402
from ioda.qa import run_qa  # noqa: E402
from ioda.transform import run_build_panel  # noqa: E402


def _parse_month(value: str) -> tuple[int, int]:
    s = value.strip()
    try:
        dt = datetime.strptime(s, "%Y-%m")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Expected YYYY-MM, got: {value!r}") from exc
    return dt.year, dt.month


def _month_window_utc(year: int, month: int) -> tuple[datetime, datetime]:
    start = datetime(year, month, 1, 0, 0, 0, tzinfo=UTC)
    if month == 12:
        end = datetime(year + 1, 1, 1, 0, 0, 0, tzinfo=UTC)
    else:
        end = datetime(year, month + 1, 1, 0, 0, 0, tzinfo=UTC)
    return start, end


def _prev_month_from_now() -> tuple[int, int]:
    now = datetime.now(UTC)
    y, m = now.year, now.month
    if m == 1:
        return y - 1, 12
    return y, m - 1


def _fmt(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Fetch one closed calendar month of IODA data (default: previous month UTC), "
            "then rebuild processed panels and QA outputs."
        )
    )
    p.add_argument(
        "--month",
        default=None,
        help="Target month in YYYY-MM (default: previous month in UTC). Example: 2026-03",
    )
    p.add_argument("--config", default=str(ROOT / "config" / "west_africa.yaml"), help="YAML config path.")
    p.add_argument(
        "--entity-catalog",
        default=str(ROOT / "data" / "processed" / "entity_catalog.parquet"),
        help="Entity catalog parquet (should be refreshed before closeout if config changed).",
    )
    p.add_argument("--raw-dir", default=str(ROOT / "data" / "raw"), help="Raw JSON root directory.")
    p.add_argument("--processed-dir", default=str(ROOT / "data" / "processed"), help="Processed outputs directory.")
    p.add_argument("--request-log", default=str(ROOT / "data" / "logs" / "requests.ndjson"), help="Request log NDJSON path.")
    p.add_argument("--level", default="both", choices=["country", "region", "both"], help="Entity level(s) to fetch.")
    p.add_argument("--metrics", default="ping-slash24", help="Comma-separated metrics (default: ping-slash24).")
    p.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite raw files for the month. Recommended for closed-month reruns.",
    )
    p.add_argument("--no-build", action="store_true", help="Skip build_panel after fetch.")
    p.add_argument("--no-qa", action="store_true", help="Skip QA after build_panel.")
    p.add_argument("--dry-run", action="store_true", help="Print planned month window/fetch requests only.")
    p.add_argument("--min-interval-seconds", type=float, default=None, help="Override request pacing.")
    p.add_argument("--timeout-seconds", type=float, default=None, help="Override HTTP timeout.")
    p.add_argument("--max-retries", type=int, default=None, help="Override retries.")
    p.add_argument("--max-points", type=int, default=None, help="Override maxPoints.")
    p.add_argument("--max-response-bytes", type=int, default=None, help="Override chunk size byte threshold.")
    p.add_argument("--initial-chunk-mode", choices=["month", "week", "day"], default="month", help="Initial chunk size.")
    p.add_argument(
        "--allow-current-month",
        action="store_true",
        help="Allow running a closeout on the current UTC month (normally blocked because month is not closed).",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()

    if args.month:
        year, month = _parse_month(args.month)
    else:
        year, month = _prev_month_from_now()

    start_dt, end_dt = _month_window_utc(year, month)
    now = datetime.now(UTC)
    current_month_start = datetime(now.year, now.month, 1, tzinfo=UTC)
    if (start_dt == current_month_start) and (not args.allow_current_month):
        raise SystemExit(
            "Refusing to close out the current UTC month by default. "
            "Use --allow-current-month if you explicitly want a partial-month fetch."
        )
    if start_dt > now:
        raise SystemExit(f"Target month {year:04d}-{month:02d} is in the future.")

    month_label = f"{year:04d}-{month:02d}"
    print(f"Monthly closeout target: {month_label}")
    print(f"Window UTC (exclusive end): {_fmt(start_dt)} -> {_fmt(end_dt)}")
    print(f"Calendar days in month: {calendar.monthrange(year, month)[1]}")
    if args.overwrite:
        print("Raw overwrite mode: ON")
    else:
        print("Raw overwrite mode: OFF (existing month chunk files will be skipped)")

    summary = run_fetch(
        config_path=Path(args.config),
        entity_catalog_path=Path(args.entity_catalog),
        raw_dir=Path(args.raw_dir),
        request_log_path=Path(args.request_log),
        level=args.level,
        metrics_arg=args.metrics,
        start=_fmt(start_dt),
        end=_fmt(end_dt),
        dry_run=args.dry_run,
        overwrite=args.overwrite,
        min_interval_seconds=args.min_interval_seconds,
        timeout_seconds=args.timeout_seconds,
        max_retries=args.max_retries,
        max_points=args.max_points,
        max_response_bytes=args.max_response_bytes,
        initial_chunk_mode=args.initial_chunk_mode,
    )
    print(
        "Fetch complete. "
        f"targets={summary.targets} planned_chunks={summary.planned_chunks} "
        f"written_chunks={summary.written_chunks} skipped_existing={summary.skipped_existing} "
        f"dry_run_chunks={summary.dry_run_chunks} errors={summary.errors}"
    )

    if args.dry_run:
        return 0

    if not args.no_build:
        outputs = run_build_panel(
            raw_dir=Path(args.raw_dir),
            entity_catalog_path=Path(args.entity_catalog),
            processed_dir=Path(args.processed_dir),
        )
        print(
            "Build panel complete. "
            f"long_rows={len(outputs.long_df)} "
            f"country_panel_rows={len(outputs.country_panel_df)} "
            f"region_panel_rows={len(outputs.region_panel_df)}"
        )
    else:
        print("Build panel skipped (--no-build).")

    if (not args.no_qa) and (not args.no_build):
        qa_df = run_qa(
            long_path=Path(args.processed_dir) / "ioda_long.parquet",
            qa_summary_path=Path(args.processed_dir) / "qa_summary.parquet",
            qa_report_path=ROOT / "docs" / "qa_report.md",
        )
        print(f"QA complete. summary_rows={len(qa_df)}")
    elif args.no_qa:
        print("QA skipped (--no-qa).")
    else:
        print("QA skipped because build was skipped.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
