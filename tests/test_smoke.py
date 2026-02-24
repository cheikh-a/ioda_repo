from __future__ import annotations

import os
from pathlib import Path

import pytest


def test_imports_and_config_parse():
    from ioda.utils import load_yaml, parse_entities_from_config

    repo_root = Path(__file__).resolve().parents[1]
    config_path = repo_root / "config" / "west_africa.yaml"
    cfg = load_yaml(config_path)
    countries = parse_entities_from_config(cfg)
    assert countries, "Expected West Africa country list in config"
    assert any(c["iso2"] == "GH" for c in countries)


@pytest.mark.skipif(os.getenv("IODA_LIVE_TEST") != "1", reason="Set IODA_LIVE_TEST=1 to run live API smoke test")
def test_live_tiny_fetch(tmp_path: Path):
    from ioda.discover import run_discovery
    from ioda.fetch import run_fetch

    repo_root = Path(__file__).resolve().parents[1]
    config_path = repo_root / "config" / "west_africa.yaml"

    entity_catalog = tmp_path / "entity_catalog.parquet"
    entity_catalog_md = tmp_path / "entity_catalog.md"
    request_log = tmp_path / "requests.ndjson"
    raw_dir = tmp_path / "raw"

    run_discovery(
        config_path=config_path,
        entity_catalog_parquet=entity_catalog,
        entity_catalog_markdown=entity_catalog_md,
        request_log_path=request_log,
        include_regions=False,
        probe_coverage_flag=False,
        metrics=["ping-slash24"],
        limit_entities=1,
    )

    summary = run_fetch(
        config_path=config_path,
        entity_catalog_path=entity_catalog,
        raw_dir=raw_dir,
        request_log_path=request_log,
        level="country",
        metrics_arg="ping-slash24",
        start="2026-02-20",
        end="2026-02-22",
        limit_entities=1,
    )
    assert summary.written_chunks >= 1 or summary.skipped_existing >= 1
    assert any(raw_dir.rglob("*.json"))
