"""
Signal Audit — flat-table diagnostic view for manual inspection.

Tabs:
  1. Adapter Activity  – ingest sources, event counts, idempotency health
  2. Event Stream      – raw vs emitted toggle, dropped counts
  3. Strategy Leaderboard – alpha / sharpe-like / weighted ranking
  4. Prediction Log    – every scored prediction + signal strength
  5. Pipeline Health   – coverage, staleness, zero-emission, schema drift

Run with:
  streamlit run app/ui/audit.py
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st

# ── constants ─────────────────────────────────────────────────────────────────

DB_PATH = Path("data/alpha.db")

ALL_ADAPTERS = [
    "alpaca_news",
    "yahoo_finance",
    "fred_macro",
    "reddit_social",
    "custom_bundle",
    "google_trends",
    "etf_flows",
    "earnings_calendar",
    "options_flow",
    "fear_greed",
    "cross_asset",
    "market_breadth",
    "market_baseline",
    "yfinance_macro",
]

# Core tables and a representative column set expected in a healthy DB.
SCHEMA_EXPECTATIONS: dict[str, list[str]] = {
    "events":            ["id", "source", "timestamp", "ticker", "text"],
    "ingest_runs":       ["source_id", "fetched_count", "emitted_count", "empty_count", "status", "ok"],
    "backfill_slice_markers": ["source_id", "start_ts", "end_ts", "ok"],
    "strategies":        ["id", "name", "track", "status", "is_champion"],
    "prediction_scores": ["strategy_id", "ticker", "direction_hit_rate", "efficiency_rating"],
    "prediction_runs":   ["id", "tenant_id", "timeframe", "created_at"],
}

# ── helpers ───────────────────────────────────────────────────────────────────


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c


def _query(sql: str, params: tuple = ()) -> pd.DataFrame:
    try:
        with _conn() as conn:
            rows = conn.execute(sql, params).fetchall()
            if not rows:
                return pd.DataFrame()
            return pd.DataFrame([dict(r) for r in rows])
    except Exception as exc:
        st.warning(f"Query failed: {exc}")
        return pd.DataFrame()


def _scalar(sql: str, params: tuple = (), default=0):
    try:
        with _conn() as conn:
            row = conn.execute(sql, params).fetchone()
            return row[0] if row and row[0] is not None else default
    except Exception:
        return default


def _arrow_safe_display_df(df: pd.DataFrame, *, numeric_cols: list[str] | None = None) -> pd.DataFrame:
    """
    Streamlit transports dataframes via Arrow and is strict about mixed dtypes.
    Never fill numeric columns with sentinel strings like "—".
    """
    out = df.copy()
    numeric_cols = numeric_cols or []
    for c in numeric_cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    for c in out.columns:
        if c in numeric_cols:
            continue
        if pd.api.types.is_numeric_dtype(out[c]):
            continue
        out[c] = out[c].where(out[c].notna(), "—")
    return out


def _table_exists(name: str) -> bool:
    try:
        with _conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
            ).fetchone()
            return row is not None
    except Exception:
        return False


def _columns_for(table: str) -> set[str]:
    try:
        with _conn() as conn:
            rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
            return {r["name"] for r in rows}
    except Exception:
        return set()


def _hit_color(rate: float) -> str:
    if rate >= 0.56:
        return "color: #2E7D32"
    if rate >= 0.48:
        return "color: #F57C00"
    return "color: #C62828"


def _parse_ts(val) -> datetime | None:
    if not val:
        return None
    try:
        s = str(val).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
    except Exception:
        return None


def _hours_since(val) -> float | None:
    dt = _parse_ts(val)
    if dt is None:
        return None
    return (datetime.now(timezone.utc) - dt).total_seconds() / 3600


def _days_between(ts_min, ts_max) -> float:
    """Return calendar days between two ISO timestamps; minimum 1 to avoid div/0."""
    a, b = _parse_ts(ts_min), _parse_ts(ts_max)
    if a is None or b is None:
        return 1.0
    diff = (b - a).total_seconds() / 86400
    return max(diff, 1.0)


# ── page config ───────────────────────────────────────────────────────────────



def _render_audit_controls(*, using_sidebar: bool) -> int:
    if using_sidebar:
        with st.sidebar:
            st.header("Audit Controls")
            st.caption(f"DB: `{DB_PATH}`")
            if st.button("Refresh", use_container_width=True):
                st.cache_data.clear()
                st.rerun()
            st.divider()
            stale_hours = st.number_input(
                "Stale threshold (hours)",
                min_value=1, max_value=168, value=24, step=1,
                help="Adapters not heard from in this many hours are flagged as stale.",
            )
        return int(stale_hours)

    col1, col2, col3 = st.columns([2, 2, 6])
    with col1:
        st.caption(f"DB: `{DB_PATH}`")
    with col2:
        if st.button("Refresh", key="audit_refresh_inline", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
    with col3:
        stale_hours = st.number_input(
            "Stale threshold (hours)",
            min_value=1, max_value=168, value=24, step=1,
            help="Adapters not heard from in this many hours are flagged as stale.",
            key="audit_stale_hours_inline",
        )
    return int(stale_hours)


def audit_main(
    *,
    db_path: str | Path = "data/alpha.db",
    show_page_header: bool = True,
    use_sidebar_controls: bool = True,
) -> None:
    """Render the Signal Audit UI."""
    global DB_PATH
    DB_PATH = Path(db_path)

    if show_page_header:
        st.title("Signal Audit")
        st.caption("Flat-table inspection of adapters, events, strategies, predictions, and pipeline health.")
    else:
        st.markdown("## Signal Audit")
        st.caption("Flat-table inspection of adapters, events, strategies, predictions, and pipeline health.")

    if not DB_PATH.exists():
        st.error(f"Database not found at `{DB_PATH}`. Run a backfill first.")
        st.stop()

    stale_hours = _render_audit_controls(using_sidebar=use_sidebar_controls)

    # ── tabs ──────────────────────────────────────────────────────────────────────

    tab_adapters, tab_events, tab_leaderboard, tab_predictions, tab_health = st.tabs([
        "📡 Adapter Activity",
        "📋 Event Stream",
        "🏆 Strategy Leaderboard",
        "📊 Prediction Log",
        "⚙ Pipeline Health",
    ])

    # ═════════════════════════════════════════════════════════════════════════════
    # TAB 1 — Adapter Activity
    # ═════════════════════════════════════════════════════════════════════════════

    with tab_adapters:
        st.subheader("Adapter Activity")
        st.caption("One row per adapter — ingestion health, idempotency stats, drop rate.")

        # ── ingest_runs aggregate ────────────────────────────────────────────────
        if _table_exists("ingest_runs"):
            runs_df = _query("""
                SELECT
                    source_id                                               AS adapter,
                    COUNT(*)                                                AS total_runs,
                    SUM(CASE WHEN ok = 1 THEN 1 ELSE 0 END)                AS ok_runs,
                    SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END)    AS skipped_windows,
                    SUM(COALESCE(empty_count, 0))                           AS empty_windows,
                    SUM(COALESCE(fetched_count, 0))                         AS events_fetched,
                    SUM(COALESCE(emitted_count, 0))                         AS events_emitted,
                    SUM(COALESCE(retry_count, 0))                           AS total_retries,
                    MIN(start_ts)                                           AS coverage_from,
                    MAX(end_ts)                                             AS coverage_to,
                    MAX(CASE WHEN ok = 1 THEN completed_at END)             AS last_ok_at,
                    MAX(CASE WHEN ok = 0 THEN last_error END)               AS last_error
                FROM ingest_runs
                GROUP BY source_id
                ORDER BY events_fetched DESC
            """)
        else:
            runs_df = pd.DataFrame()

        # ── events store counts ──────────────────────────────────────────────────
        if _table_exists("events"):
            evt_counts_df = _query("""
                SELECT source AS adapter, COUNT(*) AS events_in_store
                FROM events GROUP BY source ORDER BY events_in_store DESC
            """)
        else:
            evt_counts_df = pd.DataFrame()

        # ── merge with known adapter list ────────────────────────────────────────
        known = pd.DataFrame({"adapter": ALL_ADAPTERS})
        null_cols = ["total_runs", "ok_runs", "skipped_windows", "empty_windows",
                     "events_fetched", "events_emitted", "total_retries",
                     "coverage_from", "coverage_to", "last_ok_at", "last_error"]

        if not runs_df.empty:
            merged = known.merge(runs_df, on="adapter", how="left")
        else:
            merged = known.copy()
            for col in null_cols:
                merged[col] = None

        if not evt_counts_df.empty:
            merged = merged.merge(evt_counts_df, on="adapter", how="left")
        else:
            merged["events_in_store"] = None

        # ── derived columns ───────────────────────────────────────────────────────
        def _safe_div(num, denom):
            try:
                n, d = float(num), float(denom)
                return round(n / d, 3) if d > 0 else None
            except Exception:
                return None

        def _arrow_safe_display_df(df: pd.DataFrame, *, numeric_cols: list[str] | None = None) -> pd.DataFrame:
            """
            Streamlit transports dataframes via Arrow and is strict about mixed dtypes.
            Never fill numeric columns with sentinel strings like "—".
            """
            out = df.copy()
            numeric_cols = numeric_cols or []
            for c in numeric_cols:
                if c in out.columns:
                    out[c] = pd.to_numeric(out[c], errors="coerce")
            for c in out.columns:
                if c in numeric_cols:
                    continue
                if pd.api.types.is_numeric_dtype(out[c]):
                    continue
                out[c] = out[c].where(out[c].notna(), "—")
            return out

        merged["cache_hit_rate"] = merged.apply(
            lambda r: _safe_div(r.get("skipped_windows") or 0, r.get("total_runs") or 0), axis=1
        )
        merged["drop_rate"] = merged.apply(
            lambda r: _safe_div(
                (r.get("events_fetched") or 0) - (r.get("events_emitted") or 0),
                r.get("events_fetched") or 0,
            ), axis=1
        )
        merged["events_per_day"] = merged.apply(
            lambda r: round(
                (r.get("events_fetched") or 0) /
                _days_between(r.get("coverage_from"), r.get("coverage_to")), 1
            ) if r.get("coverage_from") else None,
            axis=1,
        )
        merged["emitted_per_day"] = merged.apply(
            lambda r: round(
                (r.get("events_emitted") or 0) /
                _days_between(r.get("coverage_from"), r.get("coverage_to")), 1
            ) if r.get("coverage_from") else None,
            axis=1,
        )
        merged["status"] = merged.apply(
            lambda r: "✅ active" if (r.get("ok_runs") or 0) > 0
            else ("❌ errors" if (r.get("total_runs") or 0) > 0 else "⬜ never run"),
            axis=1,
        )

        # ── summary metrics ───────────────────────────────────────────────────────
        active  = (merged["status"] == "✅ active").sum()
        errored = (merged["status"] == "❌ errors").sum()
        never   = (merged["status"] == "⬜ never run").sum()
        total_fetched  = int(merged["events_fetched"].fillna(0).sum())
        total_emitted  = int(merged["events_emitted"].fillna(0).sum())
        overall_drop   = round((total_fetched - total_emitted) / total_fetched, 3) if total_fetched else 0

        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Total Adapters", len(ALL_ADAPTERS))
        m2.metric("Active", int(active))
        m3.metric("Errored", int(errored))
        m4.metric("Never run", int(never))
        m5.metric("Overall drop rate", f"{overall_drop:.1%}")

        st.divider()

        display_cols = [
            "adapter", "status",
            "events_per_day", "emitted_per_day",
            "total_runs", "ok_runs", "skipped_windows", "empty_windows",
            "events_fetched", "events_emitted", "events_in_store",
            "cache_hit_rate", "drop_rate", "total_retries",
            "last_ok_at", "last_error",
        ]
        show = merged[[c for c in display_cols if c in merged.columns]].copy()
        show = _arrow_safe_display_df(
            show,
            numeric_cols=[
                "events_per_day",
                "emitted_per_day",
                "total_runs",
                "ok_runs",
                "skipped_windows",
                "empty_windows",
                "events_fetched",
                "events_emitted",
                "events_in_store",
                "cache_hit_rate",
                "drop_rate",
                "total_retries",
            ],
        )
        st.dataframe(show, use_container_width=True, hide_index=True)

        errors = merged[merged["last_error"].notna() & (merged["last_error"] != "—")]
        if not errors.empty:
            with st.expander("Error details", expanded=False):
                for _, row in errors.iterrows():
                    st.markdown(f"**{row['adapter']}**: `{row['last_error']}`")


    # ═════════════════════════════════════════════════════════════════════════════
    # TAB 2 — Event Stream
    # ═════════════════════════════════════════════════════════════════════════════

    with tab_events:
        st.subheader("Event Stream")

        view_mode = st.radio(
            "View mode",
            ["Emitted events (events table)", "Raw vs emitted comparison (ingest_runs)"],
            horizontal=True,
            key="evt_view_mode",
        )

        st.divider()

        # ── RAW VS EMITTED comparison ─────────────────────────────────────────────
        if view_mode.startswith("Raw"):
            st.caption("Per-adapter counts from the ingest ledger — shows what was fetched, emitted, and dropped.")

            if not _table_exists("ingest_runs"):
                st.info("No `ingest_runs` table found. Run a backfill first.")
            else:
                rv_df = _query("""
                    SELECT
                        source_id                               AS adapter,
                        COUNT(*)                                AS runs,
                        SUM(COALESCE(fetched_count, 0))         AS raw_fetched,
                        SUM(COALESCE(emitted_count, 0))         AS emitted,
                        SUM(COALESCE(fetched_count, 0))
                            - SUM(COALESCE(emitted_count, 0))   AS dropped,
                        SUM(COALESCE(empty_count, 0))           AS empty_windows,
                        MIN(start_ts)                           AS coverage_from,
                        MAX(end_ts)                             AS coverage_to
                    FROM ingest_runs
                    WHERE ok = 1
                    GROUP BY source_id
                    ORDER BY raw_fetched DESC
                """)

                if rv_df.empty:
                    st.info("No completed ingest runs found.")
                else:
                    rv_df["days_covered"] = rv_df.apply(
                        lambda r: round(_days_between(r.get("coverage_from"), r.get("coverage_to")), 1), axis=1
                    )
                    rv_df["fetched_per_day"] = rv_df.apply(
                        lambda r: round(r["raw_fetched"] / max(_days_between(r.get("coverage_from"), r.get("coverage_to")), 1), 1), axis=1
                    )
                    rv_df["emitted_per_day"] = rv_df.apply(
                        lambda r: round(r["emitted"] / max(_days_between(r.get("coverage_from"), r.get("coverage_to")), 1), 1), axis=1
                    )
                    rv_df["drop_rate"] = rv_df.apply(
                        lambda r: f"{(r['dropped'] / r['raw_fetched']):.1%}"
                        if r["raw_fetched"] > 0 else "—",
                        axis=1,
                    )
                    rv_df["emit_rate"] = rv_df.apply(
                        lambda r: f"{(r['emitted'] / r['raw_fetched']):.1%}"
                        if r["raw_fetched"] > 0 else "—",
                        axis=1,
                    )

                    # summary bar
                    total_raw  = int(rv_df["raw_fetched"].sum())
                    total_emit = int(rv_df["emitted"].sum())
                    total_drop = int(rv_df["dropped"].sum())

                    s1, s2, s3 = st.columns(3)
                    s1.metric("Total raw fetched", f"{total_raw:,}")
                    s2.metric("Total emitted", f"{total_emit:,}")
                    s3.metric("Total dropped", f"{total_drop:,}", delta=f"-{total_drop/total_raw:.1%}" if total_raw else None, delta_color="inverse")

                st.dataframe(rv_df, use_container_width=True, hide_index=True)

                # ── per-run detail ────────────────────────────────────────────────
                with st.expander("Per-run detail (all ingest_runs rows)", expanded=False):
                    run_detail = _query("""
                        SELECT
                            source_id AS adapter, start_ts, end_ts, status,
                            ok, fetched_count, emitted_count, empty_count,
                            retry_count, last_error, completed_at
                        FROM ingest_runs
                        ORDER BY completed_at DESC
                        LIMIT 500
                    """)
                    if not run_detail.empty:
                        run_detail_show = _arrow_safe_display_df(
                            run_detail,
                            numeric_cols=["ok", "fetched_count", "emitted_count", "empty_count", "retry_count"],
                        )
                st.dataframe(run_detail_show, use_container_width=True, hide_index=True)

        # ── EMITTED EVENTS view ───────────────────────────────────────────────────
        else:
            st.caption("Raw events in the event store. Filter by adapter or ticker.")

            if not _table_exists("events"):
                st.info("No `events` table found. Run a backfill to populate.")
            else:
                total_n = _scalar("SELECT COUNT(*) FROM events")

                sources_df = _query("SELECT DISTINCT source FROM events ORDER BY source")
                sources = ["(all)"] + (sources_df["source"].tolist() if not sources_df.empty else [])
                tickers_df = _query("SELECT DISTINCT ticker FROM events WHERE ticker IS NOT NULL ORDER BY ticker")
                tickers = ["(all)"] + (tickers_df["ticker"].tolist() if not tickers_df.empty else [])

                f1, f2, f3 = st.columns(3)
                sel_source = f1.selectbox("Source / Adapter", sources, key="evt_source")
                sel_ticker = f2.selectbox("Ticker", tickers, key="evt_ticker")
                limit      = f3.slider("Max rows", 50, 2000, 500, step=50, key="evt_limit")

                st.caption(f"Total events in store: **{total_n:,}**")

                where_clauses: list[str] = []
                params: list = []
                if sel_source != "(all)":
                    where_clauses.append("source = ?")
                    params.append(sel_source)
                if sel_ticker != "(all)":
                    where_clauses.append("ticker = ?")
                    params.append(sel_ticker)
                where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

                events_df = _query(
                    f"""
                    SELECT timestamp, source, ticker,
                           SUBSTR(text, 1, 120) AS text_preview, tags, weight
                    FROM events {where_sql}
                    ORDER BY timestamp DESC LIMIT ?
                    """,
                    tuple(params) + (limit,),
                )

                if events_df.empty:
                    st.info("No events match the current filters.")
                else:
                    st.dataframe(events_df, use_container_width=True, hide_index=True)

                st.divider()
                st.markdown("**Events by source**")
                breakdown = _query("""
                    SELECT source, COUNT(*) AS count,
                           MIN(timestamp) AS earliest, MAX(timestamp) AS latest
                    FROM events GROUP BY source ORDER BY count DESC
                """)
                if not breakdown.empty:
                    st.dataframe(breakdown, use_container_width=True, hide_index=True)


    # ═════════════════════════════════════════════════════════════════════════════
    # TAB 3 — Strategy Leaderboard
    # ═════════════════════════════════════════════════════════════════════════════

    with tab_leaderboard:
        st.subheader("Strategy Leaderboard")
        st.caption("All strategies with alpha score, Sharpe-like ratio, and weighted composite rank.")

        has_scores     = _table_exists("prediction_scores")
        has_strategies = _table_exists("strategies")

        if not has_scores and not has_strategies:
            st.info("No strategy or scoring data found. Run the full pipeline first.")
        else:
            # ── strategy definitions ────────────────────────────────────────────
            if has_strategies:
                strat_df = _query("""
                    SELECT id AS strategy_id, name, track, strategy_type,
                           status, is_champion, backtest_score, forward_score, stability_score
                    FROM strategies
                """)
            else:
                strat_df = pd.DataFrame()

            # ── aggregated scores ───────────────────────────────────────────────
            # Sharpe-like: avg_return / std_return  (SQLite variance via E[x^2]-E[x]^2)
            if has_scores:
                score_agg = _query("""
                    SELECT
                        strategy_id,
                        COUNT(DISTINCT ticker)                                   AS tickers,
                        COUNT(*)                                                 AS samples,
                        ROUND(AVG(direction_hit_rate), 3)                        AS avg_hit_rate,
                        ROUND(AVG(efficiency_rating), 3)                         AS avg_efficiency,
                        ROUND(AVG(sync_rate), 3)                                 AS avg_sync_rate,
                        ROUND(AVG(total_return_actual), 4)                       AS avg_actual_return,
                        ROUND(AVG(total_return_pred), 4)                         AS avg_pred_return,
                        ROUND(
                            AVG(total_return_actual) /
                            NULLIF(
                                SQRT(MAX(0,
                                    AVG(total_return_actual * total_return_actual)
                                    - AVG(total_return_actual) * AVG(total_return_actual)
                                )),
                            0),
                        3)                                                       AS sharpe_like,
                        MIN(created_at)                                          AS first_scored_at,
                        MAX(created_at)                                          AS last_scored_at
                    FROM prediction_scores
                    GROUP BY strategy_id
                """)
            else:
                score_agg = pd.DataFrame()

            # ── merge ────────────────────────────────────────────────────────────
            if not strat_df.empty and not score_agg.empty:
                combined = strat_df.merge(score_agg, on="strategy_id", how="outer")
            elif not score_agg.empty:
                combined = score_agg.copy()
            elif not strat_df.empty:
                combined = strat_df.copy()
            else:
                combined = pd.DataFrame()

            if combined.empty:
                st.info("No strategy data yet.")
            else:
                # ── weighted composite score ──────────────────────────────────────
                # weights: hit_rate is king early; efficiency and sync support
                # penalty: min(1, samples/20) — strategies with < 20 samples are discounted
                def _weighted(row):
                    try:
                        hr      = float(row.get("avg_hit_rate")  or 0)
                        eff     = float(row.get("avg_efficiency") or 0)
                        syn     = float(row.get("avg_sync_rate")  or 0)
                        samples = float(row.get("samples")        or 0)
                        raw     = 0.50 * hr + 0.30 * eff + 0.20 * syn
                        penalty = min(1.0, samples / 20.0)
                        return round(raw * penalty, 4)
                    except Exception:
                        return None

                combined["weighted_score"] = combined.apply(_weighted, axis=1)

                # ── predictions per day ───────────────────────────────────────────
                combined["predictions_per_day"] = combined.apply(
                    lambda r: round(
                        (r.get("samples") or 0) /
                        _days_between(r.get("first_scored_at"), r.get("last_scored_at")),
                        2,
                    ) if r.get("first_scored_at") else None,
                    axis=1,
                )

                # ── alpha_score: forward_score if available, else avg_efficiency ──
                if "forward_score" in combined.columns:
                    combined["alpha_score"] = combined.apply(
                        lambda r: r["forward_score"] if pd.notna(r.get("forward_score")) and r["forward_score"] != 0
                        else r.get("avg_efficiency"),
                        axis=1,
                    )
                else:
                    combined["alpha_score"] = combined.get("avg_efficiency")

                # ── summary ───────────────────────────────────────────────────────
                n_total     = len(combined)
                n_champion  = int(combined["is_champion"].fillna(0).sum()) if "is_champion" in combined else 0
                n_scored    = int(combined["samples"].notna().sum()) if "samples" in combined else 0
                total_samp  = int(combined["samples"].fillna(0).sum()) if "samples" in combined else 0

                m1, m2, m3, m4 = st.columns(4)
                m1.metric("Strategies defined", n_total)
                m2.metric("Champions", n_champion)
                m3.metric("With scored predictions", n_scored)
                m4.metric("Total scored samples", total_samp)

                st.divider()

                # ── filters + sort ────────────────────────────────────────────────
                lf1, lf2, lf3 = st.columns(3)
                tracks = ["(all)"] + (
                    [t for t in combined["track"].dropna().unique().tolist() if t]
                    if "track" in combined.columns else []
                )
                sel_track   = lf1.selectbox("Track", tracks, key="lb_track")
                min_samples = lf2.number_input("Min samples", min_value=0, value=0, step=1, key="lb_min_samples")
                sort_col    = lf3.selectbox(
                    "Sort by",
                    ["weighted_score", "alpha_score", "avg_hit_rate", "sharpe_like",
                     "avg_efficiency", "avg_sync_rate", "samples"],
                    key="lb_sort",
                )

                view = combined.copy()
                if sel_track != "(all)" and "track" in view.columns:
                    view = view[view["track"] == sel_track]
                if min_samples > 0 and "samples" in view.columns:
                    view = view[view["samples"].fillna(0) >= min_samples]
                if sort_col in view.columns:
                    sort_keys = (
                        ["weighted_score", "predictions_per_day"]
                        if sort_col == "weighted_score"
                        else [sort_col]
                    )
                    sort_keys = [k for k in sort_keys if k in view.columns]
                    if sort_keys:
                        view = view.sort_values(sort_keys, ascending=False, na_position="last")

                display_order = [
                    "strategy_id", "track", "strategy_type", "status", "is_champion",
                    "samples", "tickers", "predictions_per_day",
                    "weighted_score", "alpha_score", "sharpe_like",
                    "avg_hit_rate", "avg_efficiency", "avg_sync_rate",
                    "avg_actual_return", "avg_pred_return",
                    "backtest_score", "forward_score", "stability_score",
                    "last_scored_at",
                ]
                show_cols   = [c for c in display_order if c in view.columns]
                view_display = view[show_cols].copy()
                view_display = _arrow_safe_display_df(
                    view_display,
                    numeric_cols=[
                        "is_champion",
                        "samples",
                        "predictions_per_day",
                        "weighted_score",
                        "alpha_score",
                        "sharpe_like",
                        "avg_hit_rate",
                        "avg_efficiency",
                        "avg_sync_rate",
                        "avg_actual_return",
                        "avg_pred_return",
                        "backtest_score",
                        "forward_score",
                        "stability_score",
                    ],
                )

                def _style_hit(val):
                    try:
                        return _hit_color(float(val))
                    except Exception:
                        return ""

                style_targets = [c for c in ["avg_hit_rate", "weighted_score"] if c in view_display.columns]
                if style_targets:
                    styled = view_display.style.applymap(_style_hit, subset=style_targets)
                    st.dataframe(styled, use_container_width=True, hide_index=True)
                else:
                    st.dataframe(view_display, use_container_width=True, hide_index=True)


    # ═════════════════════════════════════════════════════════════════════════════
    # TAB 4 — Prediction Log
    # ═════════════════════════════════════════════════════════════════════════════

    with tab_predictions:
        st.subheader("Prediction Log")
        st.caption("Every scored prediction. signal_strength = |pred_return| × efficiency. Green = correct direction.")

        if not _table_exists("prediction_scores"):
            st.info("No `prediction_scores` table found. Run the full pipeline first.")
        else:
            total_n = _scalar("SELECT COUNT(*) FROM prediction_scores")

            pf1, pf2, pf3, pf4 = st.columns(4)

            tickers_ps = _query("SELECT DISTINCT ticker FROM prediction_scores ORDER BY ticker")
            p_tickers  = ["(all)"] + (tickers_ps["ticker"].tolist() if not tickers_ps.empty else [])
            sel_p_ticker = pf1.selectbox("Ticker", p_tickers, key="pl_ticker")

            strats_ps  = _query("SELECT DISTINCT strategy_id FROM prediction_scores ORDER BY strategy_id")
            p_strats   = ["(all)"] + (strats_ps["strategy_id"].tolist() if not strats_ps.empty else [])
            sel_p_strat = pf2.selectbox("Strategy", p_strats, key="pl_strat")

            tfs_ps = _query("SELECT DISTINCT timeframe FROM prediction_scores ORDER BY timeframe")
            p_tfs  = ["(all)"] + (tfs_ps["timeframe"].tolist() if not tfs_ps.empty else [])
            sel_p_tf = pf3.selectbox("Timeframe", p_tfs, key="pl_tf")

            p_limit = pf4.slider("Max rows", 50, 5000, 500, step=50, key="pl_limit")

            st.caption(f"Total scored predictions: **{total_n:,}**")

            where_parts: list[str] = []
            qparams: list = []
            if sel_p_ticker != "(all)":
                where_parts.append("ticker = ?")
                qparams.append(sel_p_ticker)
            if sel_p_strat != "(all)":
                where_parts.append("strategy_id = ?")
                qparams.append(sel_p_strat)
            if sel_p_tf != "(all)":
                where_parts.append("timeframe = ?")
                qparams.append(sel_p_tf)
            where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

            pred_df = _query(
                f"""
                SELECT
                    created_at                                      AS scored_at,
                    ticker,
                    strategy_id,
                    timeframe,
                    forecast_days,
                    ROUND(direction_hit_rate, 3)                    AS hit_rate,
                    ROUND(efficiency_rating, 3)                     AS efficiency,
                    ROUND(total_return_pred  * 100, 2)              AS pred_return_pct,
                    ROUND(total_return_actual * 100, 2)             AS actual_return_pct,
                    ROUND(total_return_error  * 100, 2)             AS return_error_pct,
                    ROUND(ABS(total_return_pred) * efficiency_rating, 4) AS signal_strength,
                    ROUND(sync_rate, 3)                             AS sync_rate,
                    regime,
                    run_id
                FROM prediction_scores
                {where_sql}
                ORDER BY signal_strength DESC, created_at DESC
                LIMIT ?
                """,
                tuple(qparams) + (p_limit,),
            )

            if pred_df.empty:
                st.info("No predictions match the current filters.")
            else:
                pred_df["correct"] = pred_df["hit_rate"].apply(
                    lambda x: "✅" if (x is not None and str(x) != "—" and float(x) >= 0.5)
                    else ("❌" if (x is not None and str(x) != "—") else "—")
                )

                def _row_style(row):
                    try:
                        bg = "#E8F5E9" if float(row["hit_rate"]) >= 0.5 else "#FFEBEE"
                        return [f"background-color: {bg}"] * len(row)
                    except Exception:
                        return [""] * len(row)

                st.dataframe(
                    pred_df.style.apply(_row_style, axis=1),
                    use_container_width=True,
                    hide_index=True,
                )

            # ── per-ticker × strategy summary ─────────────────────────────────────
            st.divider()
            st.markdown("**Hit rate by ticker × strategy**")
            summary = _query("""
                SELECT
                    ticker, strategy_id,
                    COUNT(*)                                        AS samples,
                    ROUND(AVG(direction_hit_rate), 3)               AS avg_hit_rate,
                    ROUND(AVG(efficiency_rating),  3)               AS avg_efficiency,
                    ROUND(AVG(ABS(total_return_pred) * efficiency_rating), 4) AS avg_signal_strength,
                    ROUND(AVG(total_return_actual) * 100, 2)        AS avg_actual_pct,
                    ROUND(AVG(total_return_pred)   * 100, 2)        AS avg_pred_pct
                FROM prediction_scores
                GROUP BY ticker, strategy_id
                ORDER BY avg_hit_rate DESC
            """)
            if not summary.empty:
                def _style_sum_hit(val):
                    try:
                        return _hit_color(float(val))
                    except Exception:
                        return ""

                st.dataframe(
                    summary.style.map(_style_sum_hit, subset=["avg_hit_rate"]),
                    use_container_width=True,
                    hide_index=True,
                )


    # ═════════════════════════════════════════════════════════════════════════════
    # TAB 5 — Pipeline Health
    # ═════════════════════════════════════════════════════════════════════════════

    with tab_health:
        st.subheader("Pipeline Health")
        st.caption(f"Staleness threshold: **{stale_hours}h** (change in sidebar). Red = action needed.")

        # ── 1. Ingestion coverage ─────────────────────────────────────────────────
        st.markdown("### Ingestion Coverage")

        if _table_exists("ingest_runs"):
            cov_df = _query("""
                SELECT
                    source_id                                           AS adapter,
                    COUNT(*)                                            AS total_windows,
                    SUM(CASE WHEN ok = 1 THEN 1 ELSE 0 END)            AS ok_windows,
                    SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END) AS skipped_windows,
                    SUM(COALESCE(emitted_count, 0))                     AS total_emitted,
                    MAX(CASE WHEN ok = 1 THEN completed_at END)         AS last_ok_at,
                    MIN(start_ts)                                       AS earliest_window,
                    MAX(end_ts)                                         AS latest_window
                FROM ingest_runs
                GROUP BY source_id
            """)

            if not cov_df.empty:
                cov_df["coverage_pct"] = cov_df.apply(
                    lambda r: round((r["ok_windows"] + r["skipped_windows"]) / r["total_windows"], 3)
                    if r["total_windows"] > 0 else None,
                    axis=1,
                )
                cov_df["hours_since_ok"] = cov_df["last_ok_at"].apply(_hours_since)

                # Merge with known adapters
                known_cov = pd.DataFrame({"adapter": ALL_ADAPTERS})
                cov_merged = known_cov.merge(cov_df, on="adapter", how="left")
                cov_merged["hours_since_ok"] = cov_merged["last_ok_at"].apply(_hours_since)

                # Summary
                covered = cov_merged["ok_windows"].fillna(0).sum()
                total_w = cov_merged["total_windows"].fillna(0).sum()
                overall_cov = round(covered / total_w, 3) if total_w > 0 else 0

                hc1, hc2, hc3 = st.columns(3)
                hc1.metric("Overall coverage", f"{overall_cov:.1%}")
                hc2.metric("Ok windows", f"{int(covered):,}")
                hc3.metric("Total windows", f"{int(total_w):,}")

                # Color by coverage
                def _cov_color(val):
                    try:
                        v = float(val)
                        if v >= 0.90:
                            return "color: #2E7D32"
                        if v >= 0.70:
                            return "color: #F57C00"
                        return "color: #C62828"
                    except Exception:
                        return ""

                show_cov = cov_merged[
                    [
                        "adapter",
                        "total_windows",
                        "ok_windows",
                        "skipped_windows",
                        "coverage_pct",
                        "total_emitted",
                        "hours_since_ok",
                        "earliest_window",
                        "latest_window",
                    ]
                ].copy()
                show_cov = _arrow_safe_display_df(
                    show_cov,
                    numeric_cols=[
                        "total_windows",
                        "ok_windows",
                        "skipped_windows",
                        "coverage_pct",
                        "total_emitted",
                        "hours_since_ok",
                    ],
                )
                cov_style_cols = [c for c in ["coverage_pct"] if c in show_cov.columns]
                if cov_style_cols:
                    st.dataframe(
                        show_cov.style.map(_cov_color, subset=cov_style_cols),
                        use_container_width=True, hide_index=True,
                    )
                else:
                    st.dataframe(show_cov, use_container_width=True, hide_index=True)
            else:
                st.info("No ingest run data found.")
        else:
            st.info("`ingest_runs` table not found. Run a backfill first.")

        st.divider()

        # ── 2. Stale adapters ─────────────────────────────────────────────────────
        st.markdown("### Stale Adapters")
        st.caption(f"Adapters with no successful run in the last {stale_hours}h.")

        if _table_exists("ingest_runs"):
            stale_df = _query("""
                SELECT source_id AS adapter,
                       MAX(CASE WHEN ok = 1 THEN completed_at END) AS last_ok_at,
                       SUM(CASE WHEN ok = 0 THEN 1 ELSE 0 END) AS failed_runs
                FROM ingest_runs
                GROUP BY source_id
            """)

            known_stale = pd.DataFrame({"adapter": ALL_ADAPTERS})
            stale_merged = known_stale.merge(stale_df if not stale_df.empty else pd.DataFrame({"adapter": [], "last_ok_at": [], "failed_runs": []}),
                                             on="adapter", how="left")
            stale_merged["hours_since_ok"] = stale_merged["last_ok_at"].apply(_hours_since)

            def _stale_flag(row):
                h = row.get("hours_since_ok")
                if h is None:
                    return "⬜ never run"
                if h > stale_hours:
                    return f"🔴 stale ({h:.0f}h ago)"
                return f"✅ fresh ({h:.0f}h ago)"

            stale_merged["staleness"] = stale_merged.apply(_stale_flag, axis=1)

            stale_only = stale_merged[stale_merged["staleness"].str.startswith(("🔴", "⬜"))]
            if stale_only.empty:
                st.success(f"All adapters have run within the last {stale_hours}h.")
            else:
                st.warning(f"{len(stale_only)} adapter(s) are stale or have never run.")
                stale_show = stale_only[["adapter", "staleness", "last_ok_at", "failed_runs"]].copy()
                stale_show = _arrow_safe_display_df(stale_show, numeric_cols=["failed_runs"])
                st.dataframe(stale_show, use_container_width=True, hide_index=True)

            with st.expander("All adapter staleness", expanded=False):
                stale_all = stale_merged[["adapter", "staleness", "last_ok_at", "failed_runs"]].copy()
                stale_all = _arrow_safe_display_df(stale_all, numeric_cols=["failed_runs"])
                st.dataframe(stale_all, use_container_width=True, hide_index=True)
        else:
            st.info("`ingest_runs` table not found.")

        st.divider()

        # ── 3. Zero-emission warnings ─────────────────────────────────────────────
        st.markdown("### Zero-Emission Warnings")
        st.caption("Adapters that ran successfully but emitted zero events — likely a filter or API issue.")

        if _table_exists("ingest_runs"):
            zero_df = _query("""
                SELECT source_id AS adapter,
                       COUNT(*) AS ok_runs,
                       SUM(COALESCE(emitted_count, 0)) AS total_emitted,
                       SUM(COALESCE(fetched_count, 0)) AS total_fetched
                FROM ingest_runs
                WHERE ok = 1
                GROUP BY source_id
                HAVING total_emitted = 0
                ORDER BY total_fetched DESC
            """)

            if zero_df.empty:
                st.success("No zero-emission adapters found.")
            else:
                st.warning(f"{len(zero_df)} adapter(s) ran successfully but emitted zero events.")
                st.dataframe(zero_df, use_container_width=True, hide_index=True)
        else:
            st.info("`ingest_runs` table not found.")

        st.divider()

        # ── 4. Schema drift warnings ──────────────────────────────────────────────
        st.markdown("### Schema Drift")
        st.caption("Checks that key tables exist and have expected columns.")

        schema_rows = []
        for table, expected_cols in SCHEMA_EXPECTATIONS.items():
            exists = _table_exists(table)
            if not exists:
                schema_rows.append({
                    "table": table,
                    "status": "❌ missing",
                    "missing_columns": ", ".join(expected_cols),
                    "notes": "Table does not exist",
                })
                continue

            actual_cols = _columns_for(table)
            missing = [c for c in expected_cols if c not in actual_cols]
            row_count = _scalar(f"SELECT COUNT(*) FROM {table}")

            schema_rows.append({
                "table": table,
                "status": "✅ ok" if not missing else "⚠️ drift",
                "rows": row_count,
                "missing_columns": ", ".join(missing) if missing else "—",
                "notes": f"{len(actual_cols)} columns present",
            })

        schema_report = pd.DataFrame(schema_rows)

        issues = schema_report[schema_report["status"] != "✅ ok"]
        if issues.empty:
            st.success("All expected tables and columns are present.")
        else:
            st.warning(f"{len(issues)} schema issue(s) detected.")

        st.dataframe(schema_report, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    st.set_page_config(page_title="Signal Audit", layout="wide", page_icon="??")
    audit_main()
