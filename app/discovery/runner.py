from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any

from app.db.repository import AlphaRepository
from app.core.target_stocks import get_target_stocks, get_target_stocks_registry
from app.core.environment import build_env_snapshot_v3
from app.core.environment_v3 import bucket_env_v3
from app.discovery.feature_snapshot import build_feature_snapshot
from app.discovery.strategies import STRATEGIES, score_candidates, to_repo_rows
from app.discovery.types import FeatureRow
from app.discovery.industry_filter import get_industry_universe
from app.discovery.adaptive_industry import get_industry_adaptive_configs
from app.discovery.adaptive_selection import select_adaptive_config, enable_adaptive_globally


def _insert_sniper_near_misses(
    db_path: str | Path,
    as_of_date: str,
    near_misses: list[dict[str, Any]],
) -> None:
    """Persist sniper gate-pass near-misses (score below threshold) for adaptive mining."""
    if not near_misses:
        return
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sniper_near_misses (
                symbol        TEXT NOT NULL,
                as_of_date    TEXT NOT NULL,
                score         REAL,
                price_extreme REAL,
                vol_extreme   REAL,
                spike_extreme REAL,
                trend_extreme REAL,
                fear_regime   INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (symbol, as_of_date)
            )
        """)
        rows = [
            (
                nm["symbol"],
                as_of_date,
                nm.get("score"),
                nm.get("price"),
                nm.get("vol"),
                nm.get("spike"),
                nm.get("trend"),
                nm.get("fear_regime", 0),
            )
            for nm in near_misses
        ]
        conn.executemany(
            """
            INSERT OR REPLACE INTO sniper_near_misses
              (symbol, as_of_date, score, price_extreme, vol_extreme, spike_extreme, trend_extreme, fear_regime)
            VALUES (?,?,?,?,?,?,?,?)
            """,
            rows,
        )
        conn.commit()
    finally:
        conn.close()


def _get_fear_regime(db_path: str | Path, as_of_date: str) -> bool:
    """Return True if VIX > VIX3M on or before as_of_date (inverted term structure = fear)."""
    conn = sqlite3.connect(str(db_path))
    try:
        vix = conn.execute(
            "SELECT close FROM price_bars WHERE ticker = ? AND timestamp <= ? ORDER BY timestamp DESC LIMIT 1",
            ("^VIX", as_of_date),
        ).fetchone()
        vix3m = conn.execute(
            "SELECT close FROM price_bars WHERE ticker = ? AND timestamp <= ? ORDER BY timestamp DESC LIMIT 1",
            ("^VIX3M", as_of_date),
        ).fetchone()
        if vix and vix3m:
            return float(vix[0]) > float(vix3m[0])
        return False
    finally:
        conn.close()


def _parse_date(s: str | date) -> str:
    if isinstance(s, date):
        return s.isoformat()
    return date.fromisoformat(str(s).strip()).isoformat()


def _load_features_from_snapshot(
    db_path: str | Path,
    as_of_date: str,
    symbols: list[str] | None = None,
) -> dict[str, FeatureRow]:
    """Load features directly from feature_snapshot table."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # Get latest row per symbol
    try:
        if symbols:
            placeholders = ",".join(["?"] * len(symbols))
            query = f"""
                SELECT f1.symbol, f1.as_of_date, f1.close, f1.return_63d, f1.volatility_20d,
                       f1.price_percentile_252d, f1.dollar_volume, f1.volume_zscore_20d
                FROM feature_snapshot f1
                INNER JOIN (
                    SELECT symbol, MAX(as_of_date) as max_date
                    FROM feature_snapshot
                    WHERE symbol IN ({placeholders})
                    AND as_of_date <= ?
                    GROUP BY symbol
                ) f2 ON f1.symbol = f2.symbol AND f1.as_of_date = f2.max_date
            """
            rows = conn.execute(query, (*symbols, as_of_date)).fetchall()
        else:
            query = """
                SELECT f1.symbol, f1.as_of_date, f1.close, f1.return_63d, f1.volatility_20d,
                       f1.price_percentile_252d, f1.dollar_volume, f1.volume_zscore_20d
                FROM feature_snapshot f1
                INNER JOIN (
                    SELECT symbol, MAX(as_of_date) as max_date
                    FROM feature_snapshot
                    WHERE as_of_date <= ?
                    GROUP BY symbol
                ) f2 ON f1.symbol = f2.symbol AND f1.as_of_date = f2.max_date
            """
            rows = conn.execute(query, (as_of_date,)).fetchall()
    except sqlite3.OperationalError:
        # New/ephemeral DBs (like unit tests) may not have a populated feature_snapshot table.
        rows = []

    conn.close()

    features = {}
    for r in rows:
        features[str(r["symbol"])] = FeatureRow(
            symbol=str(r["symbol"]),
            as_of_date=str(r["as_of_date"]),
            close=float(r["close"]) if r["close"] else None,
            volume=None,
            dollar_volume=float(r["dollar_volume"]) if r["dollar_volume"] else None,
            avg_dollar_volume_20d=float(r["dollar_volume"]) if r["dollar_volume"] else None,
            return_1d=None,
            return_5d=None,
            peer_relative_return_63d=None,
            price_bucket=None,
            return_20d=None,
            return_63d=float(r["return_63d"]) if r["return_63d"] else None,
            return_252d=None,
            volatility_20d=float(r["volatility_20d"]) if r["volatility_20d"] else None,
            max_drawdown_252d=None,
            price_percentile_252d=float(r["price_percentile_252d"]) if r["price_percentile_252d"] else None,
            volume_zscore_20d=float(r["volume_zscore_20d"]) if r["volume_zscore_20d"] else None,
            dollar_volume_zscore_20d=None,
            revenue_ttm=None,
            revenue_growth=None,
            shares_outstanding=None,
            shares_growth=None,
            sector=None,
            industry=None,
            sector_return_63d=None,
        )

    return features


def run_discovery(
    *,
    db_path: str | Path = "data/alpha.db",
    tenant_id: str = "default",
    as_of: str | date,
    top_n: int = 50,
    min_avg_dollar_volume_20d: float | None = None,
    timeframe: str = "1d",
    use_target_universe: bool = False,
    symbols: list[str] | None = None,
    use_feature_snapshot: bool = True,
    include_experimental: bool = False,
) -> dict[str, Any]:
    """
    Run all discovery strategies and persist top candidates per strategy.

    Returns a JSON-serializable summary:
      { "as_of_date": "...", "strategies": {strategy: {"top": [...], "top_lt5": [...]}} }
    """
    as_of_date = _parse_date(as_of)
    universe_version = None
    universe_symbols: list[str] | None = symbols
    if use_target_universe and symbols is None:
        universe_symbols = get_target_stocks(asof=date.fromisoformat(as_of_date))
        universe_version = get_target_stocks_registry().target_universe_version

    repo = AlphaRepository(db_path=db_path)
    try:
        features: dict[str, FeatureRow] = {}
        if use_feature_snapshot:
            features = _load_features_from_snapshot(db_path, as_of_date, universe_symbols)

        # Fallback: compute point-in-time features directly from bars when the snapshot table
        # is missing or empty (common in unit tests and fresh DBs).
        if not features:
            features = build_feature_snapshot(
                db_path=db_path,
                as_of=as_of_date,
                tenant_id=tenant_id,
                timeframe=timeframe,
                symbols=universe_symbols,
            )

        if min_avg_dollar_volume_20d is not None:
            features = {
                s: fr
                for s, fr in features.items()
                if fr.avg_dollar_volume_20d is not None and fr.avg_dollar_volume_20d >= float(min_avg_dollar_volume_20d)
            }

        # If the caller explicitly disables liquidity gating, also disable the hard
        # universe quality gates in `score_candidates` for discovery (unit-test friendly).
        score_gate_overrides: dict[str, Any] = {}
        if min_avg_dollar_volume_20d is not None and float(min_avg_dollar_volume_20d) <= 0.0:
            score_gate_overrides["min_close"] = 0.0
            score_gate_overrides["min_dollar_volume"] = 0.0

        # Phase 1: Environment snapshot and bucketing (upgraded to v3 with industry)
        env = build_env_snapshot_v3(db_path=db_path, as_of=as_of_date, vix_value=None)
        env_bucket = bucket_env_v3(env)

        # Regime context: fear_regime gates the sniper_coil strategy
        fear_regime = _get_fear_regime(db_path, as_of_date)
        regime_ctx: dict[str, Any] = {"fear_regime": fear_regime}

        # Add environment details to summary for logging
        summary: dict[str, Any] = {
            "as_of_date": as_of_date,
            "tenant_id": tenant_id,
            "universe_version": universe_version,
            "universe_size": len(universe_symbols) if universe_symbols is not None else None,
            "feature_rows": len(features),
            "environment": {
                "market_vol_pct": env.market_vol_pct,
                "trend_strength": env.trend_strength,
                "cross_sectional_disp": env.cross_sectional_disp,
                "liquidity_regime": env.liquidity_regime,
                "sector_regime": env.sector_regime,
                "industry_dispersion": env.industry_dispersion,
                "size_regime": env.size_regime,
                "env_bucket": env_bucket,
                "fear_regime": fear_regime,
            },
            "strategies": {},
        }

        # Critical logging for validation
        print(f"ENV_BUCKET_V3: {env_bucket} (sector: {env.sector_regime}, industry_disp: {env.industry_dispersion:.2f})")
        print(f"FEAR_REGIME: {fear_regime}")

        # Enable adaptive mode for industry-aware selection
        enable_adaptive_globally()

        discovery_strategies = [
            "realness_repricer",
            "silent_compounder",
            "narrative_lag",
            "balance_sheet_survivor",
            "ownership_vacuum",
        ]
        if include_experimental:
            # Keep experimental strategies opt-in to preserve stable behavior in unit tests
            # and small-sample environments.
            for name in ("sniper_coil", "volatility_breakout"):
                if name in STRATEGIES and name not in discovery_strategies:
                    discovery_strategies.append(name)

        for strat in discovery_strategies:
            is_sniper = strat == "sniper_coil"

            if is_sniper:
                # Sniper runs on full universe (not industry-filtered) with regime context.
                # No adaptive config — gates and thresholds are fixed by design.
                # Absolute scoring (no cross-sectional rank normalization).
                # Near-misses (all gates passed, score < threshold) are logged separately.
                near_misses: list[dict[str, Any]] = []
                cands = score_candidates(
                    features,
                    strategy_type=strat,
                    regime_context=regime_ctx,
                    use_rank_normalization=False,
                    near_miss_collector=near_misses,
                    **score_gate_overrides,
                )
                strat_top_n = 3
                if near_misses:
                    _insert_sniper_near_misses(db_path, as_of_date, near_misses)
                print(f"  {strat}: {len(cands)} candidates, {len(near_misses)} near-misses (fear_regime={fear_regime})")
            else:
                # Build industry-specific universe for this environment
                industry_features = get_industry_universe(features, env_bucket)
                print(f"  {strat}: {len(industry_features)} stocks after industry filtering")

                # Get industry-aware adaptive config for this environment
                try:
                    adaptive_config = select_adaptive_config(
                        strategy_type=strat,
                        env_bucket=env_bucket,
                        db_path=db_path,
                        enable_adaptive=True,
                    )
                    print(f"  {strat}: Selected config: {adaptive_config.get('config_name', 'default')}")
                except Exception as e:
                    # Fallback to industry-adaptive configs
                    industry_configs = get_industry_adaptive_configs(strat, env.sector_regime)
                    adaptive_config = industry_configs[0] if industry_configs else {}
                    print(f"  {strat}: Fallback config: {adaptive_config.get('config_name', 'default')}")

                cands = score_candidates(
                    industry_features,
                    strategy_type=strat,
                    config=adaptive_config,
                    **score_gate_overrides,
                )
                strat_top_n = int(top_n)

            top = cands[:strat_top_n]
            top_lt5 = [c for c in cands if (c.metadata.get("close") is not None and float(c.metadata["close"]) < 5.0)][
                :strat_top_n
            ]

            repo_rows = to_repo_rows(top)
            repo.upsert_discovery_candidates(as_of_date=as_of_date, candidates=repo_rows, tenant_id=tenant_id)

            strat_summary: dict[str, Any] = {
                "top": [asdict(c) for c in top],
                "top_lt5": [asdict(c) for c in top_lt5],
            }
            if is_sniper:
                strat_summary["fear_regime"] = fear_regime
                strat_summary["near_misses"] = len(near_misses)
            else:
                strat_summary["industry_filtered_stocks"] = len(industry_features)
                strat_summary["selected_config"] = adaptive_config.get("config_name", "default")
                strat_summary["sector_regime"] = env.sector_regime

            summary["strategies"][strat] = strat_summary

        return summary
    finally:
        repo.close()


def format_summary_json(summary: dict[str, Any]) -> str:
    return json.dumps(summary, indent=2, sort_keys=True)
