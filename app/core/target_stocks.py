from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import date, datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable

import yaml
from dateutil.parser import isoparse


@dataclass(frozen=True, slots=True)
class TargetStockSpec:
    symbol: str
    enabled: bool = True
    group: str | None = None
    active_from: date | None = None


def _as_date(value: str | date | datetime | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        dt = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).date()
    s = str(value).strip()
    if not s:
        return None
    try:
        # Accept YYYY-MM-DD as well as full ISO timestamps.
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            return date.fromisoformat(s)
        dt = isoparse(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).date()
    except Exception:
        return None


def _normalize_symbol(symbol: Any) -> str:
    s = str(symbol or "").strip().upper()
    return s


def _config_path() -> Path:
    override = str(os.getenv("TARGET_STOCKS_CONFIG", "") or "").strip()
    if override:
        return Path(override)
    return Path("config/target_stocks.yaml")


def _load_yaml_rows(path: Path) -> list[Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing Target Stocks config at {path}")
    with path.open("r", encoding="utf-8") as f:
        payload = yaml.safe_load(f) or []
    if not isinstance(payload, list):
        raise ValueError("Target Stocks config must be a YAML list")
    return payload


def _spec_from_row(row: Any) -> TargetStockSpec | None:
    if isinstance(row, str):
        sym = _normalize_symbol(row)
        return None if not sym else TargetStockSpec(symbol=sym)

    if isinstance(row, dict):
        sym = _normalize_symbol(row.get("symbol"))
        if not sym:
            return None
        enabled = row.get("enabled", True)
        group = row.get("group")
        active_from = _as_date(row.get("active_from"))
        return TargetStockSpec(
            symbol=sym,
            enabled=bool(enabled),
            group=str(group).strip() if group is not None and str(group).strip() else None,
            active_from=active_from,
        )

    return None


def _is_active(spec: TargetStockSpec, *, asof: date) -> bool:
    if not spec.enabled:
        return False
    if spec.active_from is not None and asof < spec.active_from:
        return False
    return True


def _stable_version(specs: Iterable[TargetStockSpec]) -> str:
    # Deterministic version: normalize + sort, then hash a canonical JSON representation.
    rows = []
    for s in specs:
        rows.append(
            {
                "symbol": s.symbol,
                "enabled": bool(s.enabled),
                "group": s.group,
                "active_from": (s.active_from.isoformat() if s.active_from else None),
            }
        )
    rows = sorted(rows, key=lambda r: str(r["symbol"]))
    blob = json.dumps(rows, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha1(blob).hexdigest()


class TargetStocksRegistry:
    def __init__(self, *, path: Path | None = None) -> None:
        self.path = Path(path) if path is not None else _config_path()
        raw_rows = _load_yaml_rows(self.path)

        parsed: list[TargetStockSpec] = []
        for row in raw_rows:
            spec = _spec_from_row(row)
            if spec is None:
                continue
            parsed.append(spec)

        if not parsed:
            raise ValueError("Target Stocks list is empty after normalization")

        # De-dupe by symbol (last one wins).
        by_symbol: dict[str, TargetStockSpec] = {}
        for spec in parsed:
            by_symbol[spec.symbol] = spec

        self._all_specs = list(by_symbol.values())
        self._version = _stable_version(self._all_specs)

    @property
    def target_universe_version(self) -> str:
        return self._version

    def list_specs(self, *, asof: date | datetime | None = None) -> list[TargetStockSpec]:
        asof_d = _as_date(asof) or datetime.now(timezone.utc).date()
        active = [s for s in self._all_specs if _is_active(s, asof=asof_d)]
        if not active:
            raise ValueError("Target Stocks list has no enabled/active symbols for the requested asof date")
        return sorted(active, key=lambda s: s.symbol)

    def list_symbols(self, *, asof: date | datetime | None = None) -> list[str]:
        return [s.symbol for s in self.list_specs(asof=asof)]


@lru_cache(maxsize=1)
def get_target_stocks_registry() -> TargetStocksRegistry:
    return TargetStocksRegistry()


def get_target_stocks(*, asof: date | datetime | None = None) -> list[str]:
    return get_target_stocks_registry().list_symbols(asof=asof)


def get_target_stock_rows(*, asof: date | datetime | None = None) -> list[TargetStockSpec]:
    return get_target_stocks_registry().list_specs(asof=asof)


def _dump_yaml_rows(specs: list[TargetStockSpec]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for s in sorted(specs, key=lambda x: x.symbol):
        row: dict[str, Any] = {"symbol": s.symbol, "enabled": bool(s.enabled)}
        if s.group:
            row["group"] = s.group
        if s.active_from:
            row["active_from"] = s.active_from.isoformat()
        out.append(row)
    return out


def load_target_stock_specs(*, path: Path | None = None) -> list[TargetStockSpec]:
    p = Path(path) if path is not None else _config_path()
    rows = _load_yaml_rows(p)
    parsed: list[TargetStockSpec] = []
    for row in rows:
        spec = _spec_from_row(row)
        if spec is not None:
            parsed.append(spec)
    # De-dupe by symbol (last wins).
    by_symbol: dict[str, TargetStockSpec] = {}
    for s in parsed:
        by_symbol[s.symbol] = s
    return list(by_symbol.values())


def save_target_stock_specs(*, specs: list[TargetStockSpec], path: Path | None = None) -> None:
    p = Path(path) if path is not None else _config_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    rows = _dump_yaml_rows(specs)
    with p.open("w", encoding="utf-8") as f:
        yaml.safe_dump(rows, f, sort_keys=False)


def add_target_stock(
    symbol: str,
    *,
    group: str | None = None,
    active_from: str | date | datetime | None = None,
    enabled: bool = True,
    path: Path | None = None,
) -> str:
    specs = load_target_stock_specs(path=path)
    sym = _normalize_symbol(symbol)
    if not sym:
        raise ValueError("SYMBOL is required")
    new = TargetStockSpec(symbol=sym, enabled=bool(enabled), group=(str(group).strip() if group else None), active_from=_as_date(active_from))
    by_symbol = {s.symbol: s for s in specs}
    by_symbol[sym] = new
    save_target_stock_specs(specs=list(by_symbol.values()), path=path)
    get_target_stocks_registry.cache_clear()
    return get_target_stocks_registry().target_universe_version


def remove_target_stock(symbol: str, *, path: Path | None = None) -> str:
    specs = load_target_stock_specs(path=path)
    sym = _normalize_symbol(symbol)
    kept = [s for s in specs if s.symbol != sym]
    save_target_stock_specs(specs=kept, path=path)
    get_target_stocks_registry.cache_clear()
    return get_target_stocks_registry().target_universe_version


def set_target_stock_enabled(symbol: str, *, enabled: bool, path: Path | None = None) -> str:
    specs = load_target_stock_specs(path=path)
    sym = _normalize_symbol(symbol)
    out: list[TargetStockSpec] = []
    found = False
    for s in specs:
        if s.symbol == sym:
            found = True
            out.append(TargetStockSpec(symbol=s.symbol, enabled=bool(enabled), group=s.group, active_from=s.active_from))
        else:
            out.append(s)
    if not found:
        out.append(TargetStockSpec(symbol=sym, enabled=bool(enabled)))
    save_target_stock_specs(specs=out, path=path)
    get_target_stocks_registry.cache_clear()
    return get_target_stocks_registry().target_universe_version

