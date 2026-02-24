#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ioda.transform import run_build_panel, validation_samples  # noqa: E402


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Normalize raw IODA JSON into long + wide parquet panels.")
    p.add_argument("--raw-dir", default=str(ROOT / "data" / "raw"), help="Raw JSON root directory.")
    p.add_argument(
        "--entity-catalog",
        default=str(ROOT / "data" / "processed" / "entity_catalog.parquet"),
        help="Entity catalog parquet for metadata joins.",
    )
    p.add_argument("--processed-dir", default=str(ROOT / "data" / "processed"), help="Processed output directory.")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    outputs = run_build_panel(
        raw_dir=Path(args.raw_dir),
        entity_catalog_path=Path(args.entity_catalog),
        processed_dir=Path(args.processed_dir),
    )
    print(f"Built long rows: {len(outputs.long_df)}")
    print(f"Country panel rows: {len(outputs.country_panel_df)}")
    print(f"Region panel rows: {len(outputs.region_panel_df)}")
    print("Validation samples:")
    for line in validation_samples(outputs.long_df, max_entities_each=2):
        print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
