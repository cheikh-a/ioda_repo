from __future__ import annotations

import json
import math
import re
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Iterator

import pandas as pd
import yaml
from dateutil.relativedelta import relativedelta


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_BASE_URL = "https://api.ioda.inetintel.cc.gatech.edu/v2"


def utc_now() -> datetime:
    return datetime.now(UTC)


def utc_today() -> date:
    return utc_now().date()


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def isoformat_utc(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_dateish(value: str | None, end_of_day: bool = False) -> datetime | None:
    if value is None:
        return None
    s = value.strip()
    if not s:
        return None
    if s.isdigit():
        return datetime.fromtimestamp(int(s), tz=UTC)
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except ValueError:
        pass
    try:
        d = date.fromisoformat(s)
    except ValueError as exc:
        raise ValueError(f"Unsupported date/time format: {value!r}") from exc
    if end_of_day:
        return datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=UTC)
    return datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=UTC)


def to_epoch_seconds(dt: datetime) -> int:
    return int(dt.astimezone(UTC).timestamp())


def from_epoch_seconds(ts: int | float) -> datetime:
    return datetime.fromtimestamp(int(ts), tz=UTC)


def epoch_to_utc_string(ts: int | float | None) -> str | None:
    if ts is None or (isinstance(ts, float) and math.isnan(ts)):
        return None
    return isoformat_utc(from_epoch_seconds(int(ts)))


def sanitize_path_component(value: str) -> str:
    value = re.sub(r"[^A-Za-z0-9._=-]+", "_", str(value)).strip("_")
    return value or "unknown"


def stable_json_dumps(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, ensure_ascii=True, separators=(",", ":"))


def json_dump(path: Path, obj: Any, indent: int | None = 2) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(obj, fh, indent=indent, ensure_ascii=False, sort_keys=True)
        fh.write("\n")


def json_load(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise ValueError(f"YAML config must be a mapping: {path}")
    return data


def save_yaml(path: Path, data: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(data, fh, sort_keys=False, allow_unicode=False)


def append_ndjson(path: Path, record: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False, sort_keys=True))
        fh.write("\n")


def chunk_range(start: datetime, end: datetime, mode: str) -> Iterator[tuple[datetime, datetime]]:
    if end <= start:
        return
    cur = start
    while cur < end:
        if mode == "month":
            nxt = min(cur + relativedelta(months=1), end)
        elif mode == "week":
            nxt = min(cur + timedelta(days=7), end)
        elif mode == "day":
            nxt = min(cur + timedelta(days=1), end)
        else:
            raise ValueError(f"Unsupported chunk mode: {mode}")
        yield cur, nxt
        cur = nxt


def iter_nested(obj: Any) -> Iterator[Any]:
    if isinstance(obj, list):
        for item in obj:
            yield from iter_nested(item)
    else:
        yield obj


def is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def parse_entities_from_config(config: dict[str, Any]) -> list[dict[str, Any]]:
    region_def = config.get("region_definition") or {}
    countries = region_def.get("countries") or []
    if not isinstance(countries, list):
        raise ValueError("config.region_definition.countries must be a list")
    out: list[dict[str, Any]] = []
    include_mauritania = bool(region_def.get("include_mauritania", False))
    for item in countries:
        if not isinstance(item, dict):
            continue
        iso2 = str(item.get("iso2", "")).upper().strip()
        if not iso2:
            continue
        enabled = bool(item.get("enabled", True))
        optional = bool(item.get("optional", False))
        if iso2 == "MR" and optional:
            enabled = include_mauritania
        out.append(
            {
                "iso2": iso2,
                "name": item.get("name") or iso2,
                "enabled": enabled,
                "optional": optional,
            }
        )
    return out


def update_generated_config_sections(
    config: dict[str, Any],
    *,
    datasources: list[dict[str, Any]] | None = None,
    resolved_countries: list[dict[str, Any]] | None = None,
    resolved_regions: list[dict[str, Any]] | None = None,
    coverage_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    generated = config.setdefault("generated", {})
    if not isinstance(generated, dict):
        generated = {}
        config["generated"] = generated
    generated["last_discovered_at_utc"] = isoformat_utc(utc_now())
    if datasources is not None:
        generated["datasources"] = datasources
    resolved = generated.setdefault("resolved", {})
    if not isinstance(resolved, dict):
        resolved = {}
        generated["resolved"] = resolved
    if resolved_countries is not None:
        resolved["countries"] = resolved_countries
    if resolved_regions is not None:
        resolved["regions"] = resolved_regions
    if coverage_rows is not None:
        generated["coverage"] = coverage_rows
    return config


def dataframe_to_parquet(df: pd.DataFrame, path: Path) -> None:
    ensure_dir(path.parent)
    df.to_parquet(path, index=False)


def read_parquet_if_exists(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    return pd.read_parquet(path)


def coerce_utc_series(s: pd.Series) -> pd.Series:
    if s.empty:
        return s
    return pd.to_datetime(s, utc=True, errors="coerce")


def elapsed_ms(start_perf: float) -> float:
    return (time.perf_counter() - start_perf) * 1000.0


@dataclass(frozen=True)
class TimeWindow:
    start: datetime
    end: datetime

    def as_epoch_params(self) -> tuple[int, int]:
        return to_epoch_seconds(self.start), to_epoch_seconds(self.end)

    def filename_stem(self) -> str:
        return f"{to_epoch_seconds(self.start)}_{to_epoch_seconds(self.end)}"
