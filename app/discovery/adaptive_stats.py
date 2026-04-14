from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from app.discovery.outcomes import OutcomeRow


@dataclass(frozen=True)
class AdaptiveStatsRow:
    strategy: str
    config_hash: str
    env_bucket: tuple[str, str, str]
    sample_size: int
    win_rate: float
    avg_return: float
    total_return: float
    last_updated: str


def _config_hash(config: dict[str, Any]) -> str:
    """Generate consistent hash for config dictionary."""
    # Sort keys to ensure consistent hashing
    config_str = json.dumps(config, sort_keys=True, separators=(",", ":"))
    return str(hash(config_str))


def store_adaptive_stats(
    *,
    db_path: str | Path,
    strategy: str,
    config: dict[str, Any],
    env_bucket: tuple[str, str, str],
    outcomes: list[OutcomeRow],
) -> None:
    """
    Store performance stats for (strategy, config, env_bucket) combination.
    
    Phase 2: Dual tracking - keep both legacy and adaptive stats.
    """
    if not outcomes:
        return
    
    config_hash = _config_hash(config)
    env_bucket_str = json.dumps(env_bucket)
    
    # Calculate metrics
    wins = sum(1 for o in outcomes if o.return_pct and o.return_pct > 0)
    returns = [o.return_pct for o in outcomes if o.return_pct is not None]
    
    sample_size = len(outcomes)
    win_rate = wins / sample_size if sample_size > 0 else 0.0
    avg_return = sum(returns) / len(returns) if returns else 0.0
    total_return = sum(returns) if returns else 0.0
    
    conn = sqlite3.connect(str(db_path))
    try:
        # Create table if not exists
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS adaptive_stats (
                strategy TEXT NOT NULL,
                config_hash TEXT NOT NULL,
                env_bucket TEXT NOT NULL,
                sample_size INTEGER NOT NULL,
                win_rate REAL NOT NULL,
                avg_return REAL NOT NULL,
                total_return REAL NOT NULL,
                last_updated TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (strategy, config_hash, env_bucket)
            )
            """
        )
        
        # Add config_json column if it doesn't exist (migration)
        try:
            conn.execute("ALTER TABLE adaptive_stats ADD COLUMN config_json TEXT")
        except sqlite3.OperationalError:
            # Column already exists
            pass
        
        # Insert or update stats
        row = conn.execute(
            """
            INSERT OR REPLACE INTO adaptive_stats 
            (strategy, config_hash, env_bucket, sample_size, win_rate, avg_return, total_return, config_json, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                strategy,
                config_hash,
                env_bucket_str,
                sample_size,
                win_rate,
                avg_return,
                total_return,
                json.dumps(config) if config else "{}",
                date.today().isoformat(),
            ),
        )
        
        conn.commit()
    finally:
        conn.close()


def lookup_best_config(
    db_path: str,
    strategy: str,
    env_bucket: tuple[str, ...],
    min_samples: int = 30,
) -> dict[str, Any] | None:
    """
    Look up best performing config for given strategy and environment.
    
    Returns actual config for adaptive selection.
    """
    conn = sqlite3.connect(db_path)
    
    env_bucket_str = json.dumps(env_bucket)
    
    row = conn.execute(
        """
        SELECT config_hash, win_rate, avg_return, sample_size, config_json
        FROM adaptive_stats
        WHERE strategy = ? AND env_bucket = ? AND sample_size >= ?
        ORDER BY win_rate DESC, avg_return DESC
        LIMIT 1
        """,
        (strategy, env_bucket_str, min_samples),
    ).fetchone()
    
    if row is None:
        return None
    
    # Parse stored config
    try:
        config = json.loads(row[4]) if row[4] else {}
    except:
        config = {}
    
    return {
        "config": config,
        "config_hash": row[0],
        "win_rate": row[1],
        "avg_return": row[2],
        "sample_size": row[3],
    }


def get_adaptive_stats_summary(
    *,
    db_path: str | Path,
    strategy: str | None = None,
) -> list[dict[str, Any]]:
    """Get summary of adaptive stats for monitoring."""
    conn = sqlite3.connect(str(db_path))
    try:
        query = """
            SELECT strategy, env_bucket, COUNT(*) as config_count,
                   SUM(sample_size) as total_samples,
                   AVG(win_rate) as avg_win_rate,
                   AVG(avg_return) as avg_return
            FROM adaptive_stats
        """
        params = []
        if strategy:
            query += " WHERE strategy = ?"
            params.append(strategy)
            
        query += " GROUP BY strategy, env_bucket ORDER BY total_samples DESC"
        
        rows = conn.execute(query, params).fetchall()
        
        summary = []
        for r in rows:
            env_bucket = json.loads(r[1]) if r[1] else ("UNKNOWN", "UNKNOWN", "UNKNOWN")
            summary.append({
                "strategy": r[0],
                "env_bucket": env_bucket,
                "config_count": r[2],
                "total_samples": r[3],
                "avg_win_rate": r[4],
                "avg_return": r[5],
            })
        
        return summary
    finally:
        conn.close()
