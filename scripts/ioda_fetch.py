#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ioda.fetch import run_fetch  # noqa: E402


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch IODA raw signals for configured West Africa entities.")
    p.add_argument("--config", default=str(ROOT / "config" / "west_africa.yaml"), help="Path to YAML config.")
    p.add_argument(
        "--entity-catalog",
        default=str(ROOT / "data" / "processed" / "entity_catalog.parquet"),
        help="Entity catalog parquet (from discovery).",
    )
    p.add_argument("--raw-dir", default=str(ROOT / "data" / "raw"), help="Root directory for raw chunked JSON outputs.")
    p.add_argument("--request-log", default=str(ROOT / "data" / "logs" / "requests.ndjson"), help="Request log NDJSON path.")
    p.add_argument("--level", default="both", choices=["country", "region", "both"], help="Entity level(s) to fetch.")
    p.add_argument(
        "--metrics",
        default="auto",
        help="Comma-separated datasource list, or 'auto' to use all catalog metrics.",
    )
    p.add_argument("--start", default=None, help="Start date/time (YYYY-MM-DD, ISO8601, or epoch). Defaults to discovery earliest.")
    p.add_argument("--end", default=None, help="End date/time (YYYY-MM-DD, ISO8601, or epoch). Defaults to now UTC.")
    p.add_argument("--dry-run", action="store_true", help="Print planned requests without executing.")
    p.add_argument("--limit-entities", type=int, default=None, help="Limit number of entities for smoke tests.")
    p.add_argument("--since-last-run", action="store_true", help="Use processed coverage to fetch only newer data.")
    p.add_argument("--overwrite", action="store_true", help="Overwrite existing raw chunk files.")
    p.add_argument("--user-agent", default=None, help="Override HTTP User-Agent.")
    p.add_argument("--timeout-seconds", type=float, default=None, help="HTTP timeout seconds.")
    p.add_argument("--min-interval-seconds", type=float, default=None, help="Minimum delay between requests.")
    p.add_argument("--max-retries", type=int, default=None, help="Max retries for transient failures.")
    p.add_argument("--max-points", type=int, default=None, help="maxPoints query parameter for signals endpoint.")
    p.add_argument("--max-response-bytes", type=int, default=None, help="Fallback to smaller chunks if response exceeds this size.")
    p.add_argument("--initial-chunk-mode", choices=["month", "week", "day"], default=None, help="Initial chunking mode.")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    summary = run_fetch(
        config_path=Path(args.config),
        entity_catalog_path=Path(args.entity_catalog),
        raw_dir=Path(args.raw_dir),
        request_log_path=Path(args.request_log),
        level=args.level,
        metrics_arg=args.metrics,
        start=args.start,
        end=args.end,
        dry_run=args.dry_run,
        limit_entities=args.limit_entities,
        since_last_run=args.since_last_run,
        overwrite=args.overwrite,
        user_agent=args.user_agent,
        timeout_seconds=args.timeout_seconds,
        min_interval_seconds=args.min_interval_seconds,
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
