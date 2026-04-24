"""
Paper portfolio tracker for the three-condition mean-reversion strategy.

Position lifecycle
------------------
Entry  : next open after signal candle (approximated as signal close + slippage)
Exit A : 5 trading days after entry  (time-based)
Exit B : VIX term structure normalizes while position is open (regime-change exit)

Sizing
------
Equal-weight 1/N across all open positions, capped at MAX_POSITIONS.
On a day with more qualifying setups than remaining capacity, sort by
most-negative candle_body (deepest capitulation first).

Schema (stored in same alpha.db, paper_ prefix)
-----------------------------------------------
paper_positions  — open positions
paper_trades     — closed trades (full history)
paper_params     — key/value config (capital, thresholds)
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

_ROOT = Path(__file__).resolve().parent.parent.parent
DB = str(_ROOT / "data" / "alpha.db")

# ── Default parameters ───────────────────────────────────────────────────────
INITIAL_CAPITAL = 1_000_000.0   # paper portfolio notional ($)
MAX_POSITIONS   = 20             # hard cap on simultaneous open positions
HOLD_DAYS       = 5              # calendar-based target hold (in trading days)
ROUND_TRIP_BP   = 10.0           # assumed transaction cost, basis points round-trip

# ── Schema ───────────────────────────────────────────────────────────────────
_DDL = """
CREATE TABLE IF NOT EXISTS paper_positions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT    NOT NULL,
    entry_date      TEXT    NOT NULL,
    entry_price     REAL    NOT NULL,
    shares          REAL    NOT NULL,
    position_value  REAL    NOT NULL,
    target_exit_date TEXT   NOT NULL,
    entry_vix_term  REAL,
    entry_candle_body REAL,
    entry_vzscore   REAL,
    size_quintile   TEXT
);

CREATE TABLE IF NOT EXISTS paper_trades (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT    NOT NULL,
    entry_date      TEXT    NOT NULL,
    exit_date       TEXT    NOT NULL,
    entry_price     REAL    NOT NULL,
    exit_price      REAL    NOT NULL,
    shares          REAL    NOT NULL,
    position_value  REAL    NOT NULL,
    gross_pnl_pct   REAL    NOT NULL,
    gross_pnl_bp    REAL    NOT NULL,
    net_pnl_bp      REAL    NOT NULL,
    exit_reason     TEXT    NOT NULL,
    entry_vix_term  REAL,
    entry_candle_body REAL,
    entry_vzscore   REAL,
    size_quintile   TEXT
);

CREATE TABLE IF NOT EXISTS paper_params (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


@dataclass
class Position:
    id: int
    ticker: str
    entry_date: str
    entry_price: float
    shares: float
    position_value: float
    target_exit_date: str
    entry_vix_term: float
    entry_candle_body: float
    entry_vzscore: float
    size_quintile: str


@dataclass
class Trade:
    ticker: str
    entry_date: str
    exit_date: str
    entry_price: float
    exit_price: float
    shares: float
    position_value: float
    gross_pnl_pct: float
    gross_pnl_bp: float
    net_pnl_bp: float
    exit_reason: str
    size_quintile: str


class PaperPortfolio:
    """
    SQLite-backed paper portfolio.

    Usage
    -----
    portfolio = PaperPortfolio()
    portfolio.initialize()          # creates tables if needed

    exits = portfolio.process_exits(today, price_map, vix_term)
    new   = portfolio.enter_positions(setups, today, price_map)
    portfolio.print_daily_summary(today, exits, new)
    """

    def __init__(
        self,
        db: str = DB,
        initial_capital: float = INITIAL_CAPITAL,
        max_positions: int = MAX_POSITIONS,
        hold_days: int = HOLD_DAYS,
        round_trip_bp: float = ROUND_TRIP_BP,
    ) -> None:
        self.db = db
        self.initial_capital = initial_capital
        self.max_positions = max_positions
        self.hold_days = hold_days
        self.round_trip_bp = round_trip_bp

    # ── Setup ────────────────────────────────────────────────────────────────

    def initialize(self) -> None:
        """Create tables and write default params if not already present."""
        conn = sqlite3.connect(self.db)
        conn.executescript(_DDL)
        conn.execute(
            "INSERT OR IGNORE INTO paper_params VALUES ('initial_capital', ?)",
            (str(self.initial_capital),),
        )
        conn.execute(
            "INSERT OR IGNORE INTO paper_params VALUES ('max_positions', ?)",
            (str(self.max_positions),),
        )
        conn.execute(
            "INSERT OR IGNORE INTO paper_params VALUES ('hold_days', ?)",
            (str(self.hold_days),),
        )
        conn.execute(
            "INSERT OR IGNORE INTO paper_params VALUES ('round_trip_bp', ?)",
            (str(self.round_trip_bp),),
        )
        conn.commit()
        conn.close()

    # ── Queries ──────────────────────────────────────────────────────────────

    def get_open_positions(self) -> list[Position]:
        conn = sqlite3.connect(self.db)
        rows = conn.execute(
            "SELECT id, ticker, entry_date, entry_price, shares, position_value, "
            "target_exit_date, entry_vix_term, entry_candle_body, entry_vzscore, size_quintile "
            "FROM paper_positions ORDER BY entry_date, ticker"
        ).fetchall()
        conn.close()
        return [Position(*r) for r in rows]

    def open_position_count(self) -> int:
        conn = sqlite3.connect(self.db)
        n = conn.execute("SELECT COUNT(*) FROM paper_positions").fetchone()[0]
        conn.close()
        return n

    def open_tickers(self) -> set[str]:
        conn = sqlite3.connect(self.db)
        rows = conn.execute("SELECT DISTINCT ticker FROM paper_positions").fetchall()
        conn.close()
        return {r[0] for r in rows}

    def get_trade_history(self) -> list[Trade]:
        conn = sqlite3.connect(self.db)
        rows = conn.execute(
            "SELECT ticker, entry_date, exit_date, entry_price, exit_price, "
            "shares, position_value, gross_pnl_pct, gross_pnl_bp, net_pnl_bp, "
            "exit_reason, size_quintile FROM paper_trades ORDER BY exit_date"
        ).fetchall()
        conn.close()
        return [Trade(*r) for r in rows]

    # ── Trading days helper ──────────────────────────────────────────────────

    def _nth_trading_day_after(self, entry_date: str, n: int) -> str:
        """
        Look up the n-th trading day (calendar date) after entry_date
        by scanning available dates in the price_bars table.
        Falls back to adding ~n*1.5 calendar days if no price data available.
        """
        conn = sqlite3.connect(self.db)
        # Use VIX table (small) rather than scanning 25M price_bars rows.
        # VIX has a row for every US market trading day.
        rows = conn.execute(
            "SELECT DISTINCT DATE(timestamp) FROM price_bars "
            "WHERE tenant_id='default' AND ticker='^VIX' AND timeframe='1d' "
            "AND timestamp > ? ORDER BY timestamp LIMIT ?",
            (entry_date, n + 2),
        ).fetchall()
        conn.close()

        dates = [r[0] for r in rows]
        if len(dates) >= n:
            return dates[n - 1]

        # fallback: calendar approximation (~7 calendar days per 5 trading days)
        from datetime import datetime, timedelta
        d = datetime.strptime(entry_date, "%Y-%m-%d") + timedelta(days=int(n * 1.4) + 1)
        return d.strftime("%Y-%m-%d")

    # ── Exits ────────────────────────────────────────────────────────────────

    def process_exits(
        self,
        today: str,
        price_map: dict[str, float],
        vix_term: Optional[float],
    ) -> list[Trade]:
        """
        Check all open positions for exit conditions:
          A) target_exit_date reached
          B) VIX term structure normalized (vix_term <= 0) — regime exit

        Returns list of Trade objects for positions that were closed.
        """
        positions = self.get_open_positions()
        if not positions:
            return []

        regime_exit = vix_term is not None and vix_term <= 0
        closed: list[Trade] = []
        conn = sqlite3.connect(self.db)

        for pos in positions:
            time_exit = today >= pos.target_exit_date
            should_exit = time_exit or regime_exit
            if not should_exit:
                continue

            exit_price = price_map.get(pos.ticker)
            if exit_price is None or exit_price <= 0:
                # Can't price — extend hold by 1 day (missing price)
                continue

            reason = "time" if time_exit else "regime_change"
            gross_pct = (exit_price / pos.entry_price) - 1.0
            gross_bp  = gross_pct * 10_000
            net_bp    = gross_bp - self.round_trip_bp

            trade = Trade(
                ticker=pos.ticker,
                entry_date=pos.entry_date,
                exit_date=today,
                entry_price=pos.entry_price,
                exit_price=exit_price,
                shares=pos.shares,
                position_value=pos.position_value,
                gross_pnl_pct=gross_pct,
                gross_pnl_bp=gross_bp,
                net_pnl_bp=net_bp,
                exit_reason=reason,
                size_quintile=pos.size_quintile or "",
            )
            closed.append(trade)

            conn.execute(
                "INSERT INTO paper_trades "
                "(ticker, entry_date, exit_date, entry_price, exit_price, shares, "
                "position_value, gross_pnl_pct, gross_pnl_bp, net_pnl_bp, exit_reason, "
                "entry_vix_term, entry_candle_body, entry_vzscore, size_quintile) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    pos.ticker, pos.entry_date, today,
                    pos.entry_price, exit_price, pos.shares, pos.position_value,
                    gross_pct, gross_bp, net_bp, reason,
                    pos.entry_vix_term, pos.entry_candle_body,
                    pos.entry_vzscore, pos.size_quintile,
                ),
            )
            conn.execute("DELETE FROM paper_positions WHERE id=?", (pos.id,))

        conn.commit()
        conn.close()
        return closed

    # ── Entries ──────────────────────────────────────────────────────────────

    def enter_positions(
        self,
        setups,               # list[scanner.Setup]
        today: str,
        price_map: dict[str, float],
    ) -> list[Position]:
        """
        Enter up to (max_positions - open_count) new positions from setups.

        Setups are already sorted by candle_body ascending (most bearish first).
        Skip tickers already held.
        Position value = initial_capital / max_positions (equal-weight fixed-notional).
        """
        already_open = self.open_tickers()
        capacity = self.max_positions - self.open_position_count()
        if capacity <= 0 or not setups:
            return []

        position_value = self.initial_capital / self.max_positions
        # Pre-compute exit date once — same for all entries on a given day.
        # Must be done BEFORE opening the write connection to avoid nested
        # connection contention in SQLite WAL mode.
        target = self._nth_trading_day_after(today, self.hold_days)

        conn = sqlite3.connect(self.db, timeout=30)
        entered: list[Position] = []

        for setup in setups:
            if capacity <= 0:
                break
            if setup.ticker in already_open:
                continue

            entry_price = price_map.get(setup.ticker)
            if entry_price is None or entry_price <= 0:
                continue

            shares = position_value / entry_price

            conn.execute(
                "INSERT INTO paper_positions "
                "(ticker, entry_date, entry_price, shares, position_value, "
                "target_exit_date, entry_vix_term, entry_candle_body, "
                "entry_vzscore, size_quintile) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    setup.ticker, today, entry_price, shares, position_value,
                    target, setup.vix_term, setup.candle_body,
                    setup.volume_zscore, setup.size_quintile,
                ),
            )
            row_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            entered.append(Position(
                id=row_id,
                ticker=setup.ticker,
                entry_date=today,
                entry_price=entry_price,
                shares=shares,
                position_value=position_value,
                target_exit_date=target,
                entry_vix_term=setup.vix_term,
                entry_candle_body=setup.candle_body,
                entry_vzscore=setup.volume_zscore,
                size_quintile=setup.size_quintile,
            ))
            already_open.add(setup.ticker)
            capacity -= 1

        conn.commit()
        conn.close()
        return entered

    # ── Reporting ────────────────────────────────────────────────────────────

    def print_daily_summary(
        self,
        today: str,
        exits: list[Trade],
        entries: list[Position],
        vix_term: Optional[float],
        regime_label: str = "",
    ) -> None:
        open_positions = self.get_open_positions()
        trades = self.get_trade_history()

        print(f"\n{'='*60}")
        print(f"PAPER TRADE DAILY SUMMARY  {today}")
        print(f"{'='*60}")

        regime = f"VIX term={vix_term:+.2f}" if vix_term is not None else "VIX term=N/A"
        fear = "(FEAR REGIME)" if vix_term is not None and vix_term > 0 else "(calm)"
        print(f"Regime : {regime}  {fear}  {regime_label}")

        print(f"\n--- EXITS today ({len(exits)}) ---")
        if exits:
            for t in exits:
                flag = "✓" if t.net_pnl_bp > 0 else "✗"
                print(f"  {flag} {t.ticker:<8} {t.exit_reason:<14} "
                      f"gross={t.gross_pnl_bp:>+6.0f}bp  net={t.net_pnl_bp:>+6.0f}bp  "
                      f"({t.entry_date} → {t.exit_date})")
        else:
            print("  (none)")

        print(f"\n--- ENTRIES today ({len(entries)}) ---")
        if entries:
            for p in entries:
                print(f"  + {p.ticker:<8} @ {p.entry_price:>9.2f}  "
                      f"body={p.entry_candle_body:>+.3f}  "
                      f"vz={p.entry_vzscore:>+.2f}  "
                      f"exit≤{p.target_exit_date}  [{p.size_quintile}]")
        else:
            print("  (none)")

        print(f"\n--- OPEN POSITIONS ({len(open_positions)}/{self.max_positions}) ---")
        if open_positions:
            for p in open_positions:
                print(f"  {p.ticker:<8} entered {p.entry_date}  "
                      f"@ {p.entry_price:>9.2f}  exit≤{p.target_exit_date}")
        else:
            print("  (none)")

        if trades:
            net_bps = [t.net_pnl_bp for t in trades]
            win_rate = sum(1 for b in net_bps if b > 0) / len(net_bps)
            mean_net = sum(net_bps) / len(net_bps)
            print(f"\n--- ALL-TIME STATS ({len(trades)} closed trades) ---")
            print(f"  Win rate : {win_rate:.1%}")
            print(f"  Mean net : {mean_net:>+.1f} bp/trade")
            print(f"  Total net: {sum(net_bps):>+.0f} bp")
            by_reason: dict[str, list[float]] = {}
            for t in trades:
                by_reason.setdefault(t.exit_reason, []).append(t.net_pnl_bp)
            for reason, bps in sorted(by_reason.items()):
                print(f"  {reason:<16} n={len(bps):>4}  mean={sum(bps)/len(bps):>+.1f}bp")

        print(f"{'='*60}\n")

    def print_cost_model(self) -> None:
        """
        Print an explicit transaction-cost analysis against expected IC ranges.
        Call once during setup to understand the profitability threshold.
        """
        print("\n--- TRANSACTION COST MODEL ---")
        print(f"  Assumed round-trip cost : {self.round_trip_bp:.1f} bp")
        print(f"  Hold period             : {self.hold_days} trading days")
        print()

        cross_sect_vol_bp = 200   # ~2% daily cross-sectional vol for large/mega cap
        for ic, regime in [(-0.013, "2010s baseline (conservative)"),
                           (-0.034, "full-history mean"),
                           (-0.090, "2020s peak (regime gift)")]:
            expected_gross_bp = abs(ic) * cross_sect_vol_bp * self.hold_days
            net_bp = expected_gross_bp - self.round_trip_bp
            breakeven_ic = self.round_trip_bp / (cross_sect_vol_bp * self.hold_days)
            viable = "VIABLE" if net_bp > 0 else "UNDERWATER"
            print(f"  IC={ic:+.3f}  [{regime}]")
            print(f"    Gross alpha   : ~{expected_gross_bp:>5.0f} bp over {self.hold_days}d")
            print(f"    Round-trip    :  -{self.round_trip_bp:.0f} bp")
            print(f"    Net expected  : ~{net_bp:>+5.0f} bp  -- {viable}")
            print()

        breakeven_ic = self.round_trip_bp / (cross_sect_vol_bp * self.hold_days)
        print(f"  Break-even IC   : {breakeven_ic:+.4f}  "
              f"(need |IC| > {breakeven_ic:.4f} to cover costs)")
        print(f"  Note: 2010s IC of -0.013 {'EXCEEDS' if 0.013 > breakeven_ic else 'DOES NOT EXCEED'} "
              f"break-even at {self.round_trip_bp:.0f}bp round-trip.")
        print()
