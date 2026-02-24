#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ioda.discover import run_discovery  # noqa: E402


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Discover IODA entities/metrics and build a West Africa entity catalog.")
    p.add_argument("--config", default=str(ROOT / "config" / "west_africa.yaml"), help="Path to YAML config.")
    p.add_argument(
        "--entity-catalog-parquet",
        default=str(ROOT / "data" / "processed" / "entity_catalog.parquet"),
        help="Output parquet for machine-readable catalog.",
    )
    p.add_argument(
        "--entity-catalog-markdown",
        default=str(ROOT / "docs" / "entity_catalog.md"),
        help="Output markdown catalog.",
    )
    p.add_argument(
        "--request-log",
        default=str(ROOT / "data" / "logs" / "requests.ndjson"),
        help="Request log NDJSON path.",
    )
    p.add_argument("--user-agent", default="ioda-west-africa-pipeline/0.1", help="HTTP User-Agent header.")
    p.add_argument("--timeout-seconds", type=float, default=60.0, help="HTTP timeout seconds.")
    p.add_argument("--min-interval-seconds", type=float, default=0.5, help="Minimum delay between requests.")
    p.add_argument("--max-retries", type=int, default=5, help="Max retries for transient failures.")
    p.add_argument("--no-regions", action="store_true", help="Skip region discovery and only catalog countries.")
    p.add_argument("--no-probe-coverage", action="store_true", help="Skip earliest/latest coverage probing.")
    p.add_argument(
        "--metrics",
        default="auto",
        help="Comma-separated datasource list for coverage probing (default: auto from /datasources).",
    )
    p.add_argument("--limit-entities", type=int, default=None, help="Limit target countries for quick smoke runs.")
    p.add_argument("--refresh-coverage", action="store_true", help="Ignore coverage cache and re-probe.")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    metrics = None if args.metrics == "auto" else [m.strip() for m in args.metrics.split(",") if m.strip()]
    df = run_discovery(
        config_path=Path(args.config),
        entity_catalog_parquet=Path(args.entity_catalog_parquet),
        entity_catalog_markdown=Path(args.entity_catalog_markdown),
        request_log_path=Path(args.request_log),
        user_agent=args.user_agent,
        timeout_seconds=args.timeout_seconds,
        min_interval_seconds=args.min_interval_seconds,
        max_retries=args.max_retries,
        include_regions=not args.no_regions,
        probe_coverage_flag=not args.no_probe_coverage,
        metrics=metrics,
        limit_entities=args.limit_entities,
        refresh_coverage=args.refresh_coverage,
    )
    print(f"Discovery complete. entity_catalog rows={len(df)}")
    print(f"Wrote: {Path(args.entity_catalog_parquet)}")
    print(f"Wrote: {Path(args.entity_catalog_markdown)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
