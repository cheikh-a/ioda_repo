#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ioda.qa import run_qa  # noqa: E402


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate QA summaries and markdown report for processed IODA panels.")
    p.add_argument("--long-path", default=str(ROOT / "data" / "processed" / "ioda_long.parquet"), help="Path to ioda_long.parquet")
    p.add_argument(
        "--qa-summary-path",
        default=str(ROOT / "data" / "processed" / "qa_summary.parquet"),
        help="Output parquet path for QA summary.",
    )
    p.add_argument("--qa-report-path", default=str(ROOT / "docs" / "qa_report.md"), help="Output markdown path for QA report.")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    df = run_qa(
        long_path=Path(args.long_path),
        qa_summary_path=Path(args.qa_summary_path),
        qa_report_path=Path(args.qa_report_path),
    )
    print(f"QA complete. summary rows={len(df)}")
    print(f"Wrote: {Path(args.qa_summary_path)}")
    print(f"Wrote: {Path(args.qa_report_path)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
