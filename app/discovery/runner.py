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
        if use_feature_snapshot:
            features = _load_features_from_snapshot(db_path, as_of_date, universe_symbols)
        else:
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

        # Phase 1: Environment snapshot and bucketing (upgraded to v3 with industry)
        env = build_env_snapshot_v3(db_path=db_path, as_of=as_of_date, vix_value=None)
        env_bucket = bucket_env_v3(env)
        
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
            },
            "strategies": {},
        }
        
        # Critical logging for validation
        print(f"ENV_BUCKET_V3: {env_bucket} (sector: {env.sector_regime}, industry_disp: {env.industry_dispersion:.2f})")
        
        # Enable adaptive mode for industry-aware selection
        enable_adaptive_globally()
        
        for strat in STRATEGIES.keys():
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
            
            # Score candidates with industry-specific universe and adaptive config
            cands = score_candidates(industry_features, strategy_type=strat, config=adaptive_config)
            top = cands[: int(top_n)]
            top_lt5 = [c for c in cands if (c.metadata.get("close") is not None and float(c.metadata["close"]) < 5.0)][
                : int(top_n)
            ]

            repo_rows = to_repo_rows(top)
            repo.upsert_discovery_candidates(as_of_date=as_of_date, candidates=repo_rows, tenant_id=tenant_id)

            summary["strategies"][strat] = {
                "top": [asdict(c) for c in top],
                "top_lt5": [asdict(c) for c in top_lt5],
                "industry_filtered_stocks": len(industry_features),
                "selected_config": adaptive_config.get('config_name', 'default'),
                "sector_regime": env.sector_regime,
            }
        return summary
    finally:
        repo.close()


def format_summary_json(summary: dict[str, Any]) -> str:
    return json.dumps(summary, indent=2, sort_keys=True)
