from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import math
import pandas as pd
import streamlit as st
try:
    # Optional dependency; the UI should still import without it (unit tests, CI).
    from streamlit_autorefresh import st_autorefresh  # type: ignore
except Exception:  # pragma: no cover
    def st_autorefresh(*args, **kwargs):  # type: ignore[no-redef]
        return None

from app.ui.middle.dashboard_service import DashboardService
from app.ui.middle.job_runner import python_module_argv, python_script_argv, run_subprocess_job_in_thread
from app.ui.middle.ops_job_store import OpsJobStore
from app.db.repository import AlphaRepository


def _arrow_safe_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_numeric_dtype(out[col]):
            continue
        out[col] = out[col].astype("string")
    return out


@dataclass(frozen=True)
class DirStat:
    path: str
    files: int
    bytes: int
    newest: str | None


def _scan_dir(root: Path, *, glob: str = "**/*", max_files: int = 50_000) -> DirStat:
    files = 0
    total = 0
    newest_ts: float | None = None
    newest_iso: str | None = None
    try:
        for p in root.glob(glob):
            if files >= max_files:
                break
            try:
                if not p.is_file():
                    continue
                files += 1
                st_ = p.stat()
                total += int(st_.st_size)
                m = float(st_.st_mtime)
                if newest_ts is None or m > newest_ts:
                    newest_ts = m
                    newest_iso = datetime.fromtimestamp(m, tz=timezone.utc).isoformat().replace("+00:00", "Z")
            except Exception:
                continue
    except Exception:
        pass
    return DirStat(path=str(root), files=files, bytes=total, newest=newest_iso)


def _human_bytes(n: int) -> str:
    x = float(n or 0)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if x < 1024.0 or unit == "TB":
            return f"{x:.1f} {unit}" if unit != "B" else f"{int(x)} {unit}"
        x /= 1024.0
    return f"{int(n)} B"


def _month_range(*, years: int = 10) -> list[str]:
    # Inclusive list of YYYY-MM months ending at the current month.
    today = date.today()
    end = date(today.year, today.month, 1)
    start = date(today.year - years + 1, today.month, 1)
    months: list[str] = []
    cur = start
    while cur <= end:
        months.append(f"{cur.year:04d}-{cur.month:02d}")
        # add one month
        y, m = cur.year, cur.month + 1
        if m == 13:
            y += 1
            m = 1
        cur = date(y, m, 1)
    return months


def _chunk_date_range(start_d: date, end_d: date, *, chunk_days: int) -> list[tuple[date, date]]:
    """
    Return inclusive date chunks [start, end] as date pairs.
    """
    if end_d < start_d:
        return []
    out: list[tuple[date, date]] = []
    cur = start_d
    while cur <= end_d:
        nxt = min(end_d, cur + timedelta(days=int(chunk_days) - 1))
        out.append((cur, nxt))
        cur = nxt + timedelta(days=1)
    return out


def _coverage_grid(months: list[str], counts: dict[str, int]) -> pd.DataFrame:
    # Return a 10y x 12-ish grid as a DataFrame with rows = years, cols = months.
    rows: dict[str, dict[str, str]] = {}
    for ym in months:
        year, mon = ym.split("-", 1)
        rows.setdefault(year, {})
        n = int(counts.get(ym, 0) or 0)
        rows[year][mon] = "✓" if n > 0 else ""
    # Ensure stable column order
    cols = [f"{m:02d}" for m in range(1, 13)]
    years = sorted(rows.keys())
    return pd.DataFrame([{**{"year": y}, **{c: rows.get(y, {}).get(c, "") for c in cols}} for y in years])


def ops_data_console_main(
    service: DashboardService,
    *,
    tenant_id: str,
    ticker: str | None,
    show_page_header: bool = True,
) -> None:
    if show_page_header:
        st.markdown("# Ops / Data Console")
    else:
        st.markdown("### Ops / Data Console")

    st.caption(
        "Keep the data plane healthy so prediction quality improves, then validate downstream in backtests and paper trading."
    )

    @st.cache_resource
    def _ops_job_store() -> OpsJobStore:
        return OpsJobStore("data/ops_jobs.db")

    ops_store = _ops_job_store()
    repo_root = Path(__file__).resolve().parents[2]

    tab_status, tab_coverage, tab_jobs, tab_factors = st.tabs(["Status", "Coverage", "Run Jobs", "Factor Importance"])

    with tab_status:
        with st.expander("How this supports profitable trades (read this first)", expanded=True):
            st.markdown(
                """
This console exists to keep a clean throughline:
**better data → better features → better predictions → better evaluation → better paper/live execution**.

**1) Dumps (disk) → historical coverage**
Dump adapters (priority 1) read files under `data/raw_dumps/**` to serve historical windows without live API calls.

**2) APIs → recent coverage**  
API adapters (priority 2+) are intended to fill only *recent* windows. Historical windows are guarded to avoid rate-limit pain.

**3) Backfill runner → `ingest_runs` + `events`**  
Backfill writes a window ledger (`ingest_runs`) and emits normalized rows into `events`.

**4) Replay / backtests → `price_bars`, `prediction_runs`, predictions**  
Bars providers (Alpaca/Polygon/YFinance) populate `price_bars`. Prediction runs write `prediction_runs` and related analytics tables.
                """.strip()
            )
            st.markdown(
                """
**What you should be looking for**
- Fewer failed/running ingest windows, fewer repeated retries, and stable counts over time.
- Fresh events for your key sources (news/social/macro) and consistent `price_bars` coverage for the tickers you trade.
- Prediction runs that appear regularly and reflect the data you expect to be present.
- ML readiness moving from "blocked" → "trainable" for the symbols you care about.
                """.strip()
            )

        st.subheader("Prediction Queue (system flow)")
        pq1, pq2, pq3, pq4 = st.columns([2, 2, 2, 2])
        with pq1:
            pq_date = st.date_input("Queue date", value=date.today(), key="predq_status_date")
        with pq2:
            pq_db = st.text_input("DB", value="data/alpha.db", key="predq_status_db")
        with pq3:
            pq_tenant = st.text_input("Tenant", value=str(tenant_id), key="predq_status_tenant")
        with pq4:
            show_processing = st.toggle("Show processing", value=True, key="predq_status_show_processing")

        def _fmt_hhmm(iso: str | None) -> str:
            if not iso:
                return "—"
            try:
                dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
                return dt.astimezone().strftime("%H:%M")
            except Exception:
                return str(iso)

        counts: dict[str, int] = {}
        last_job: dict[str, Any] | None = None
        try:
            repo = AlphaRepository(db_path=str(pq_db))
            try:
                counts = repo.prediction_queue_status_counts(as_of_date=pq_date.isoformat(), tenant_id=str(pq_tenant))
                last_job = repo.latest_prediction_job(tenant_id=str(pq_tenant))
            finally:
                repo.close()
        except Exception as e:
            st.error(f"Prediction queue status unavailable: {type(e).__name__}: {e}")

        pending_n = int(counts.get("pending", 0) or 0)
        processed_n = int(counts.get("processed", 0) or 0)
        failed_n = int(counts.get("failed", 0) or 0)
        processing_n = int(counts.get("processing", 0) or 0)

        m1, m2, m3, m4 = st.columns([1, 1, 1, 2])
        with m1:
            st.metric("pending", pending_n)
        with m2:
            st.metric("processed", processed_n)
        with m3:
            st.metric("failed", failed_n)
        with m4:
            last_run = _fmt_hhmm((last_job or {}).get("completed_at") or (last_job or {}).get("started_at"))
            last_status = str((last_job or {}).get("status") or "—")
            st.metric("last run", f"{last_run} ({last_status})")

        if show_processing:
            st.caption(f"processing: {processing_n}")

        # Backfill / ingestion health (last 30 days default)
        st.subheader("Backfill health (ingestion windows)")
        with st.expander("What this is / what to look for", expanded=False):
            st.markdown(
                """
This section answers: **is backfill progressing and is it trustworthy?**

- `complete` should dominate. Persistent `running` suggests stalled windows (use cleanup).
- `failed` should be rare; repeated failures usually mean missing credentials, provider throttling, schema drift, or data gaps.
- `empty` windows can be legitimate (quiet periods) but spikes can indicate parsing issues or provider changes.
- `fetched` vs `emitted`: large gaps can indicate dedupe, validation drops, or extractor mismatch.
                """.strip()
            )
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=30)
        start_ts = start_dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        end_ts = end_dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")

        c1, c2, c3 = st.columns([2, 2, 2])
        with c1:
            source_filter = st.text_input("Filter source_id (optional)", value="")
            source_id = source_filter.strip() or None
        with c2:
            window_days = int(st.selectbox("Window", options=[7, 30, 90, 365], index=1))
        with c3:
            max_rows = int(st.selectbox("Max rows", options=[50, 100, 200, 500], index=2))

        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=window_days)
        start_ts = start_dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        end_ts = end_dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")

        summary = service.get_ingest_run_summary(start_ts=start_ts, end_ts=end_ts, source_id=source_id)
        if summary:
            st.dataframe(_arrow_safe_df(pd.DataFrame(summary)), use_container_width=True, hide_index=True)
        else:
            st.info("No `ingest_runs` rows found (or table is empty). Run a backfill first.")

        recent = service.get_recent_ingest_runs(
            start_ts=start_ts, end_ts=end_ts, source_id=source_id, limit=max_rows
        )
        if recent:
            df = pd.DataFrame(recent)
            failures = df[df["status"].astype(str).str.startswith("failed")]
            running = df[df["status"].astype(str) == "running"]

            cols = st.columns(3)
            cols[0].metric("Recent windows", len(df))
            cols[1].metric("Running", int(len(running)))
            cols[2].metric("Failed", int(len(failures)))

            with st.expander("Recent windows (most recent first)", expanded=False):
                show_cols = [
                    "updated_at",
                    "source_id",
                    "status",
                    "start_ts",
                    "end_ts",
                    "fetched_count",
                    "emitted_count",
                    "retry_count",
                    "last_error",
                ]
                st.dataframe(_arrow_safe_df(df[[c for c in show_cols if c in df.columns]]), use_container_width=True, hide_index=True)

            if not failures.empty:
                with st.expander("Recent failures (top)", expanded=True):
                    fail_cols = [
                        "updated_at",
                        "source_id",
                        "start_ts",
                        "end_ts",
                        "last_error",
                    ]
                    st.dataframe(
                        _arrow_safe_df(failures[fail_cols]),
                        use_container_width=True,
                        hide_index=True,
                    )

        st.subheader("Backfill horizons")
        with st.expander("Why horizons matter", expanded=False):
            st.markdown(
                """
Horizon markers are a quick "how far did we get?" view for each source/spec hash.

If horizons stop moving while you're expecting data to fill, the pipeline is not expanding its training/evaluation surface area.
                """.strip()
            )
        horizons = service.get_backfill_horizons(limit=200)
        if horizons:
            st.dataframe(_arrow_safe_df(pd.DataFrame(horizons)), use_container_width=True, hide_index=True)
        else:
            st.caption("No horizon markers recorded yet (table empty).")

        st.subheader("Events freshness (what sources are producing data?)")
        with st.expander("Why event freshness matters", expanded=False):
            st.markdown(
                """
Many strategies are only as good as the **freshness and continuity** of their upstream signals.

Look for:
- `max_ts` advancing for your key sources (news/social/macro).
- reasonable row counts (not zero, not exploding unexpectedly).
                """.strip()
            )
        ev = service.get_events_freshness(start_ts=start_ts, end_ts=end_ts, limit=100)
        if ev:
            ev_df = pd.DataFrame(ev)
            st.dataframe(_arrow_safe_df(ev_df), use_container_width=True, hide_index=True)
        else:
            st.caption("No `events` rows in this time window.")

        st.subheader("Pipeline / backtest history (prediction runs)")
        with st.expander("Why prediction runs matter", expanded=False):
            st.markdown(
                """
Prediction runs are your evaluation checkpoints. If runs are missing or stale, you can't trust rankings, champions, or downstream trade decisions.

Look for:
- recent runs for the tenant you care about
- consistent timeframes (e.g., daily)
- run frequency matching your expected schedule
                """.strip()
            )
        runs = service.list_prediction_runs(tenant_id=tenant_id)
        if runs:
            st.dataframe(
                _arrow_safe_df(pd.DataFrame([{"id": r.id, "label": r.label, "timeframe": r.timeframe, "created_at": r.created_at} for r in runs[:25]])),
                use_container_width=True,
                hide_index=True,
            )
            st.caption("Use the sidebar Run/Ticker/Strategy filters, then navigate to analysis views for deeper inspection.")
        else:
            st.caption("No `prediction_runs` found for this tenant.")

        st.subheader("On-disk dumps and artifacts")
        with st.expander("Why dumps matter", expanded=False):
            st.markdown(
                """
Dumps are your historical backbone. Missing/old dumps mean the system cannot widen its training surface area, and backfills will stall or become API-heavy.

Look for:
- non-zero file counts
- recent `newest_utc` timestamps after a refresh job
- expected directories present under `data/raw_dumps/**`
                """.strip()
            )
        dump_root = repo_root / "data" / "raw_dumps"
        outputs_root = repo_root / "outputs"
        profiles_root = repo_root / "data" / "company_profiles"

        stats: list[DirStat] = []
        if dump_root.exists():
            for sub in sorted([p for p in dump_root.iterdir() if p.is_dir()], key=lambda p: p.name):
                stats.append(_scan_dir(sub, glob="**/*"))
        if profiles_root.exists():
            stats.append(_scan_dir(profiles_root, glob="*.json"))
        if outputs_root.exists():
            stats.append(_scan_dir(outputs_root, glob="**/*"))

        if stats:
            st.dataframe(
                _arrow_safe_df(
                    pd.DataFrame(
                        [
                            {
                                "path": s.path.replace(str(repo_root), "").lstrip("\\/"),
                                "files": s.files,
                                "size": _human_bytes(s.bytes),
                                "newest_utc": s.newest or "—",
                            }
                            for s in stats
                        ]
                    )
                ),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.caption("No dump/artifact directories found.")

        st.subheader("ML readiness (per-ticker)")
        with st.expander("How to interpret ML readiness", expanded=False):
            st.markdown(
                """
This panel answers: **do we have enough data to train a per-ticker ML model?**

We require:
- **Labels present**: `future_return` computed (needs `price_bars` for *symbol + SPY* at entry/exit).
- **Feature coverage**: `coverage_ratio` is high enough (min coverage selector below).
- **Enough rows vs feature count**: `min_rows = max(50, n_features * 10)` (matches `app/ml/train.py` guard).

Note: current training code is pooled by horizon, but this readiness view is per-ticker to support per-ticker models.
                """.strip()
            )
            st.markdown(
                """
**What to look for**
- Blockers should be actionable: fill `price_bars` gaps, improve factor coverage, or extend history.
- "Suggested action" should move a symbol from blocked → ready without guessing.
- Use tenant `ml_train` to check data loaded via `scripts/load_training_data.py`.
                """.strip()
            )

        ml_end = date.today()
        ml_start = ml_end - timedelta(days=730)
        mc1, mc2, mc3, mc4, mc5 = st.columns([2, 2, 2, 2, 2])
        with mc1:
            ml_symbol = (ticker or "").strip().upper() or None
            ml_symbol_input = st.text_input("Symbol (optional)", value=(ml_symbol or ""))
            ml_symbol = ml_symbol_input.strip().upper() or None
        with mc2:
            ml_tenant = st.selectbox("Tenant", options=["ml_train", "default", "backfill"], index=0)
        with mc3:
            ml_horizon = st.selectbox("Horizon", options=["7d", "1d", "30d"], index=0)
        with mc4:
            min_cov = float(st.selectbox("Min coverage", options=[0.5, 0.6, 0.7, 0.8, 0.9], index=1))
        with mc5:
            limit_syms = int(st.selectbox("Max symbols", options=[20, 50, 80, 120], index=2))

        show_only_not_ready = st.toggle("Only not-ready", value=False)

        ml_state = service.get_ml_dataset_state(
            tenant_id=ml_tenant,
            horizon=ml_horizon,
            start_date=ml_start.isoformat(),
            end_date=ml_end.isoformat(),
            symbol=ml_symbol,
        )
        has_any_rows = int(ml_state.get("total_rows") or 0) > 0

        readiness = service.get_ml_readiness_per_ticker(
            tenant_id=ml_tenant,
            horizon=ml_horizon,
            start_date=ml_start.isoformat(),
            end_date=ml_end.isoformat(),
            min_feature_coverage=min_cov,
            symbol=ml_symbol,
            limit_symbols=limit_syms,
        )

        if not has_any_rows and not readiness:
            st.warning("No ML dataset exists (no `ml_learning_rows`) for this scope yet.")
            st.caption("Suggested action: build the ML dataset for one or more symbols (this computes features + labels).")

            build_syms = st.text_input("Symbols to build (comma-separated)", value=(ml_symbol or "AAPL"))
            build_syms_list = [s.strip().upper() for s in build_syms.split(",") if s.strip()]
            if build_syms_list:
                repo_root = Path(__file__).resolve().parents[2]
                argv = python_module_argv(
                    "app.ml.dataset_cli",
                    [
                        "build",
                        "--symbols",
                        ",".join(build_syms_list),
                        "--horizon",
                        "7d",
                        "--start",
                        ml_start.isoformat(),
                        "--end",
                        ml_end.isoformat(),
                        "--db",
                        "data/alpha.db",
                        "--tenant-id",
                        "backfill",
                        "--min-coverage",
                        str(min_cov),
                    ],
                )
                st.markdown("**Command preview**")
                st.code(" ".join(argv), language="bash")
                confirm = st.checkbox("I understand and want to build the ML dataset", value=False, key="ml_build_confirm")
                if st.button("Build ML dataset", type="primary", disabled=not confirm, key="ml_build_btn"):
                    job_id = run_subprocess_job_in_thread(store=ops_store, argv=argv, cwd=str(repo_root), title="ml_dataset build (7d)")
                    st.success(f"Started job {job_id[:8]}")
                    st.caption("View logs in Ops / Data → Run Jobs.")
            else:
                st.caption("Enter at least one symbol.")

        elif readiness:
            r_df = pd.DataFrame(readiness)
            if show_only_not_ready and "ready" in r_df.columns:
                r_df = r_df[r_df["ready"] == False]  # noqa: E712

            # Headline metrics
            if "ready" in r_df.columns and not r_df.empty:
                total = int(len(r_df))
                ready_n = int((r_df["ready"] == True).sum())  # noqa: E712
                not_ready_n = total - ready_n
                mcols = st.columns(3)
                mcols[0].metric("Symbols checked", total)
                mcols[1].metric("Ready", ready_n)
                mcols[2].metric("Not ready", not_ready_n)

            show_cols = [
                "symbol",
                "train_rows_labeled",
                "min_rows_required",
                "label_null_rate",
                "coverage_p10",
                "coverage_median",
                "pct_cov_ge_min",
                "n_features_est",
                "ready",
                "top_blocker",
                "suggested_action",
            ]
            st.dataframe(_arrow_safe_df(r_df[[c for c in show_cols if c in r_df.columns]]), use_container_width=True, hide_index=True)

            with st.expander("Fix (run suggested action)", expanded=False):
                not_ready = r_df[r_df.get("ready", True) == False]  # noqa: E712
                if not_ready.empty:
                    st.caption("Nothing to fix — all rows are ready.")
                else:
                    choices = [f"{row['symbol']} — {row.get('top_blocker','')}" for _, row in not_ready.iterrows()]
                    pick = st.selectbox("Pick a symbol", options=choices, index=0)
                    sym = pick.split("—", 1)[0].strip()
                    match = next((x for x in readiness if x.get("symbol") == sym), None)
                    if not match:
                        st.caption("Selection not found.")
                    else:
                        kind = str(match.get("suggested_action_kind") or "")
                        action_label = str(match.get("suggested_action") or "")
                        st.caption(f"Suggested action: {action_label}")

                        # Fix flows:
                        # - build_ml_dataset: run dataset build
                        # - backfill_missing_data: force check-only first, then run chunked backfill-range, then (optional) rebuild dataset
                        # - extend_history: offer chunked backfill over older range + rebuild dataset over older range

                        if kind == "build_ml_dataset":
                            argv = python_module_argv(
                                "app.ml.dataset_cli",
                                [
                                    "build",
                                    "--symbols",
                                    sym,
                                    "--horizon",
                                    "7d",
                                    "--start",
                                    ml_start.isoformat(),
                                    "--end",
                                    ml_end.isoformat(),
                                    "--db",
                                    "data/alpha.db",
                                    "--tenant-id",
                                    "backfill",
                                    "--min-coverage",
                                    str(min_cov),
                                ],
                            )
                            st.markdown("**Command preview**")
                            st.code(" ".join([str(a) for a in argv]), language="bash")
                            confirm = st.checkbox("I understand and want to run this fix", value=False, key="ml_fix_confirm_build")
                            if st.button("Fix (build dataset)", type="primary", disabled=not confirm, key="ml_fix_btn_build"):
                                job_id = run_subprocess_job_in_thread(store=ops_store, argv=argv, cwd=str(repo_root), title=f"ml_dataset build {sym}")
                                st.success(f"Started job {job_id[:8]}")
                                st.caption("View logs in Ops / Data → Run Jobs.")

                        elif kind in {"backfill_missing_data", "extend_history"}:
                            # Choose the proposed window.
                            if kind == "extend_history":
                                start_for_fix = (ml_end - timedelta(days=1460))  # 4y
                                end_for_fix = ml_end
                            else:
                                # Derive a "bad coverage" window from the ML dataset, fallback to training window.
                                w = service.get_ml_low_coverage_window(
                                    tenant_id="backfill",
                                    symbol=sym,
                                    horizon="7d",
                                    start_date=ml_start.isoformat(),
                                    end_date=ml_end.isoformat(),
                                    min_feature_coverage=min_cov,
                                )
                                start_for_fix = ml_start
                                end_for_fix = ml_end
                                if w.get("min_bad_date") and w.get("max_bad_date"):
                                    try:
                                        start_for_fix = date.fromisoformat(str(w["min_bad_date"]))
                                        end_for_fix = date.fromisoformat(str(w["max_bad_date"]))
                                    except Exception:
                                        start_for_fix = ml_start
                                        end_for_fix = ml_end

                            # Cap / chunking
                            span_days = (end_for_fix - start_for_fix).days + 1
                            chunk_days = 90 if span_days > 365 else 30
                            chunks = _chunk_date_range(start_for_fix, end_for_fix, chunk_days=chunk_days)

                            # What will be fixed (explicit summary)
                            st.markdown("**What will be fixed**")
                            num_chunks = int(math.ceil(span_days / float(chunk_days))) if span_days > 0 else 0
                            st.write(
                                f"- Run `backfill-range` for **all enabled sources** from `{start_for_fix.isoformat()}` to `{end_for_fix.isoformat()}` ({span_days} days) to resolve upstream gaps impacting `{sym}`."
                            )
                            st.write(f"- Run backfill in `{num_chunks}` chunk(s) of ~`{chunk_days}` days (safety cap).")
                            st.write(f"- Rebuild the ML dataset for `{sym}` (separate button) to recompute features + labels.")
                            st.write(f"- Target feature coverage: `≥ {min_cov:.0%}`.")

                            # Optional directional delta preview (rough)
                            try:
                                current_pct = match.get("pct_cov_ge_min")
                                if isinstance(current_pct, (int, float)):
                                    cur = float(current_pct)
                                    # Heuristic: fixes often improve coverage noticeably but not magically.
                                    lo = max(cur, min_cov - 0.10)
                                    hi = min(0.95, max(min_cov, cur + 0.20))
                                    st.caption(f"Coverage (directional): current ~{cur:.0%} → after fix ~{lo:.0%}–{hi:.0%}")
                            except Exception:
                                pass

                            # Enforce check-only first
                            check_key = f"ml_backfill_check::{sym}::{start_for_fix.isoformat()}::{end_for_fix.isoformat()}::{chunk_days}"
                            last_check = st.session_state.get("ml_last_backfill_check")
                            check_ok = False
                            if isinstance(last_check, dict) and last_check.get("key") == check_key:
                                job = ops_store.get_job(job_id=str(last_check.get("job_id") or ""))
                                check_ok = bool(job and job.status == "succeeded")

                            check_argv = python_module_argv(
                                "app.ingest.backfill_cli",
                                [
                                    "backfill-range",
                                    "--start",
                                    start_for_fix.isoformat(),
                                    "--end",
                                    (end_for_fix + timedelta(days=1)).isoformat(),  # backfill-range uses half-open semantics
                                    "--db",
                                    "data/alpha.db",
                                    "--batch-size-days",
                                    "1",
                                    "--check-only",
                                ],
                            )
                            st.markdown("**Step 1: Check coverage (required)**")
                            st.code(" ".join(check_argv), language="bash")
                            confirm_check = st.checkbox("I want to run check-only first", value=False, key="ml_fix_confirm_check")
                            if st.button("Check Coverage", type="secondary", disabled=not confirm_check, key="ml_fix_btn_check"):
                                job_id = run_subprocess_job_in_thread(store=ops_store, argv=check_argv, cwd=str(repo_root), title=f"backfill check {sym}")
                                st.session_state.ml_last_backfill_check = {"key": check_key, "job_id": job_id}
                                st.success(f"Started check job {job_id[:8]}")
                                st.caption("Wait for it to succeed, then run backfill.")
                                st.rerun()

                            st.markdown("**Step 2: Run targeted backfill-range**")
                            run_enabled = check_ok
                            if not run_enabled:
                                st.info("Run is disabled until the check-only job succeeds for this exact window.")

                            run_confirm = st.checkbox("I understand and want to run the backfill chunks", value=False, key="ml_fix_confirm_backfill")
                            if st.button("Run Backfill (chunked)", type="primary", disabled=(not run_enabled or not run_confirm), key="ml_fix_btn_backfill"):
                                for (cs, ce) in chunks:
                                    argv = python_module_argv(
                                        "app.ingest.backfill_cli",
                                        [
                                            "backfill-range",
                                            "--start",
                                            cs.isoformat(),
                                            "--end",
                                            (ce + timedelta(days=1)).isoformat(),
                                            "--db",
                                            "data/alpha.db",
                                            "--batch-size-days",
                                            "1",
                                            "--replay",
                                        ],
                                    )
                                    run_subprocess_job_in_thread(
                                        store=ops_store,
                                        argv=argv,
                                        cwd=str(repo_root),
                                        title=f"backfill-range {sym} {cs.isoformat()}",
                                    )
                                st.success(f"Queued {len(chunks)} backfill job(s).")
                                st.caption("After backfill completes, rebuild the ML dataset for this symbol.")

                            st.markdown("**Step 3: Rebuild ML dataset (separate, explicit)**")
                            build_start = start_for_fix
                            build_end = end_for_fix
                            build_argv = python_module_argv(
                                "app.ml.dataset_cli",
                                [
                                    "build",
                                    "--symbols",
                                    sym,
                                    "--horizon",
                                    "7d",
                                    "--start",
                                    build_start.isoformat(),
                                    "--end",
                                    build_end.isoformat(),
                                    "--db",
                                    "data/alpha.db",
                                    "--tenant-id",
                                    "backfill",
                                    "--min-coverage",
                                    str(min_cov),
                                ],
                            )
                            st.code(" ".join(build_argv), language="bash")
                            confirm_build = st.checkbox("I understand and want to rebuild ML dataset", value=False, key="ml_fix_confirm_rebuild")
                            if st.button("Rebuild ML dataset", type="secondary", disabled=not confirm_build, key="ml_fix_btn_rebuild"):
                                job_id = run_subprocess_job_in_thread(store=ops_store, argv=build_argv, cwd=str(repo_root), title=f"ml_dataset rebuild {sym}")
                                st.success(f"Started dataset rebuild job {job_id[:8]}")
                                st.caption("View logs in Ops / Data → Run Jobs.")

                        else:
                            st.caption("No runnable fix is defined for this blocker yet.")
        else:
            st.caption("ML dataset exists but no readiness rows were computed for this scope.")

    with tab_coverage:
        with st.expander("What the coverage grid means", expanded=True):
            st.markdown(
                """
This grid is a **10-year month map**.

- A **✓** means "we have *some* rows for that month" (starting simple).
- Missing months indicate gaps (not yet backfilled, missing dumps, or bars provider issues).

We can tighten the definition later (e.g., minimum trading days per month for daily bars).
                """.strip()
            )
            st.markdown(
                """
**How this connects to profitability**
- Gaps in `price_bars` can break labels and reduce ML training rows.
- Gaps in `events` reduce signal continuity and can create misleading "quiet periods".
- Gaps in `ingest_runs` usually mean the system *thought* it ran but didn't complete cleanly.
                """.strip()
            )

        months = _month_range(years=10)
        start_ym = months[0]
        end_ym = months[-1]

        c1, c2, c3, c4 = st.columns([2, 2, 2, 2])
        with c1:
            dataset = st.selectbox("Dataset", options=["price_bars", "events", "ingest_runs", "ml_7d"], index=0)
        with c2:
            tf = st.selectbox("Timeframe (price_bars)", options=["1d", "1h", "1m"], index=0, disabled=(dataset != "price_bars"))
        with c3:
            ticker_label = "Ticker (price_bars)" if dataset == "price_bars" else "Symbol (ml_7d)"
            ticker_in = st.text_input(
                ticker_label,
                value=(ticker or ""),
                disabled=(dataset not in {"price_bars", "ml_7d"}),
            )
            ticker_sel = ticker_in.strip().upper() or None
        with c4:
            source_in = st.text_input(
                "Source (events/ingest_runs)",
                value="",
                disabled=(dataset in {"price_bars", "ml_7d"}),
            )
            source_sel = source_in.strip() or None

        counts = service.get_coverage_month_counts(
            dataset=dataset,
            tenant_id=("backfill" if dataset == "ml_7d" else tenant_id),
            ticker=ticker_sel,
            timeframe=tf,
            source_id=source_sel,
            start_ym=start_ym,
            end_ym=end_ym,
        )
        grid = _coverage_grid(months, counts)
        st.dataframe(_arrow_safe_df(grid), use_container_width=True, hide_index=True)

        with st.expander("Counts by month (debug)", expanded=False):
            st.dataframe(
                _arrow_safe_df(pd.DataFrame([{"ym": k, "rows": int(v)} for k, v in sorted(counts.items())])),
                use_container_width=True,
                hide_index=True,
            )

    with tab_jobs:
        with st.expander("What this does (safe job runner)", expanded=True):
            st.markdown(
                """
This tab runs **existing CLI commands/scripts** (subprocess) and captures logs in `data/ops_jobs.db`.

Safety rules:
- The UI does **not** write `alpha.db` directly.
- Jobs default to **small windows** and **check-only** where available.
- API keys must be in environment variables; this tab does not accept secrets as inputs.
                """.strip()
            )
            st.markdown(
                """
**What you should be looking for**
- You can reproduce and fix issues from the Status/Coverage tabs without leaving the UI.
- Every job is visible (command + logs + exit code) so debugging is deterministic.
                """.strip()
            )

        with st.expander("Job presets (safe defaults)", expanded=False):
            st.caption("These buttons prefill the form below. You still confirm before anything runs.")

            def _set_preset(*, action_name: str, start: date, end: date, batch: int = 1, replay_flag: bool = True) -> None:
                st.session_state["ops_job_action"] = action_name
                st.session_state["ops_job_db_path"] = "data/alpha.db"
                st.session_state["ops_job_start"] = start
                st.session_state["ops_job_end"] = end
                st.session_state["ops_job_batch"] = batch
                st.session_state["ops_job_replay"] = replay_flag
                if "ml_job_symbols" not in st.session_state:
                    st.session_state["ml_job_symbols"] = (ticker or "AAPL")
                st.rerun()

            today = date.today()
            p1, p2, p3, p4, p5 = st.columns([1, 1, 1, 1, 1])
            with p1:
                if st.button("Check 30d", use_container_width=True):
                    _set_preset(
                        action_name="Backfill coverage check (no network)",
                        start=today - timedelta(days=30),
                        end=today,
                        batch=1,
                        replay_flag=False,
                    )
            with p2:
                if st.button("Backfill 7d", use_container_width=True):
                    _set_preset(
                        action_name="Backfill a date range",
                        start=today - timedelta(days=7),
                        end=today,
                        batch=1,
                        replay_flag=True,
                    )
            with p3:
                if st.button("Health 30d", use_container_width=True):
                    _set_preset(
                        action_name="Ingest health report",
                        start=today - timedelta(days=30),
                        end=today,
                        batch=1,
                        replay_flag=False,
                    )
            with p4:
                if st.button("Cleanup", use_container_width=True):
                    _set_preset(
                        action_name="Cleanup stalled ingest windows (dry-run)",
                        start=today - timedelta(days=30),
                        end=today,
                        batch=1,
                        replay_flag=False,
                    )
            with p5:
                if st.button("Build ML 2y", use_container_width=True):
                    _set_preset(
                        action_name="Build ML dataset",
                        start=today - timedelta(days=730),
                        end=today,
                        batch=1,
                        replay_flag=False,
                    )

        # Auto-refresh while any jobs are running.
        running_jobs = [j for j in ops_store.list_jobs(limit=25) if j.status == "running"]
        if running_jobs:
            st_autorefresh(interval=1500, key="ops_jobs_autorefresh")

        st.subheader("Discovery controls")
        with st.expander("Discovery (selection layer)", expanded=False):
            st.caption("Runs discovery CLIs as background jobs; logs appear below in Recent jobs.")

            dc1, dc2 = st.columns([2, 3])
            with dc1:
                disc_db = st.text_input("DB path", value=st.session_state.get("ops_job_db_path", "data/alpha.db"), key="disc_db_path")
            with dc2:
                disc_symbols = st.text_input("Symbols (comma-separated, optional)", value="", key="disc_symbols").strip()

            st.markdown("**Sync fundamentals (FMP → fundamentals_snapshot)**")
            st.caption("Requires `FMP_API_KEY` in environment. If Symbols is blank, nothing runs.")
            if st.button("Sync fundamentals", disabled=not bool(disc_symbols), use_container_width=True):
                argv = python_module_argv(
                    "app.discovery.discovery_cli",
                    [
                        "sync-fundamentals",
                        "--symbols",
                        disc_symbols,
                        "--db",
                        disc_db,
                        "--tenant-id",
                        tenant_id,
                    ],
                )
                job_id = run_subprocess_job_in_thread(
                    store=ops_store, argv=argv, cwd=str(repo_root), title="discovery sync-fundamentals (FMP)"
                )
                st.session_state.ops_selected_job_id = job_id
                st.success(f"Started job {job_id[:8]}")
                st.rerun()

            st.divider()
            st.markdown("**Run discovery (writes discovery_candidates)**")
            r1, r2, r3, r4 = st.columns([2, 2, 2, 2])
            with r1:
                disc_date = st.date_input("As-of date", value=date.today(), key="disc_asof_date")
            with r2:
                disc_top = st.number_input("Top N", min_value=1, max_value=500, value=50, step=5, key="disc_top_n")
            with r3:
                disc_min_adv = st.number_input("Min ADV (20d)", min_value=0.0, value=2_000_000.0, step=500_000.0, key="disc_min_adv")
            with r4:
                disc_use_target = st.checkbox("Use Target Stocks universe", value=True, key="disc_use_target")

            disc_run_symbols = st.text_input(
                "Override symbols for run (optional, comma-separated; disables target universe)",
                value="",
                key="disc_run_symbols",
            ).strip()

            run_args = [
                "run",
                "--date",
                disc_date.isoformat(),
                "--db",
                disc_db,
                "--tenant-id",
                tenant_id,
                "--top",
                str(int(disc_top)),
                "--min-adv",
                str(float(disc_min_adv)),
            ]
            if disc_run_symbols:
                run_args.extend(["--no-use-target-universe", "--symbols", disc_run_symbols])
            elif not disc_use_target:
                run_args.append("--no-use-target-universe")

            if st.button("Run discovery job", type="primary", use_container_width=True):
                argv = python_module_argv("app.discovery.discovery_cli", run_args)
                job_id = run_subprocess_job_in_thread(store=ops_store, argv=argv, cwd=str(repo_root), title="discovery run")
                st.session_state.ops_selected_job_id = job_id
                st.success(f"Started job {job_id[:8]}")
                st.rerun()

            st.divider()
            st.markdown("**Nightly pipeline (one job)**")
            st.caption("Runs: discovery → promote → outcomes → stats. Use this for scheduling.")

            n1, n2, n3, n4 = st.columns([2, 2, 2, 2])
            with n1:
                nightly_date = st.date_input("Nightly date (optional)", value=date.today(), key="nightly_date")
                nightly_use_date = st.checkbox("Use explicit date", value=False, key="nightly_use_date")
            with n2:
                nightly_out_scope = st.selectbox("Outcomes scope", options=["both", "watchlist", "candidates"], index=0, key="nightly_scope")
            with n3:
                nightly_stats_window = int(st.number_input("Stats window (days)", min_value=7, max_value=180, value=30, step=7, key="nightly_stats_window"))
            with n4:
                nightly_min_adv = float(st.number_input("Min ADV (20d)", min_value=0.0, value=2_000_000.0, step=500_000.0, key="nightly_min_adv"))

            nightly_args = [
                "nightly",
                "--db",
                disc_db,
                "--tenant-id",
                tenant_id,
                "--min-adv",
                str(float(nightly_min_adv)),
                "--outcomes-scope",
                str(nightly_out_scope),
                "--stats-window",
                str(int(nightly_stats_window)),
            ]
            if nightly_use_date:
                nightly_args.extend(["--date", nightly_date.isoformat()])

            st.markdown("**Command preview**")
            st.code(" ".join([str(x) for x in python_module_argv("app.discovery.discovery_cli", nightly_args)]), language="bash")

            if st.button("Run nightly pipeline now", type="primary", use_container_width=True):
                argv = python_module_argv("app.discovery.discovery_cli", nightly_args)
                job_id = run_subprocess_job_in_thread(store=ops_store, argv=argv, cwd=str(repo_root), title="discovery nightly")
                st.session_state.ops_selected_job_id = job_id
                st.success(f"Started job {job_id[:8]}")
                st.rerun()

            st.markdown("**Quick controls**")
            run_date = nightly_date.isoformat() if nightly_use_date else date.today().isoformat()
            include_paper = st.toggle("Include paper trading", value=False, key="daily_runner_include_paper")

            predq_argv = python_module_argv(
                "app.engine.prediction_cli",
                ["run-queue", "--as-of", run_date, "--db", disc_db, "--tenant-id", tenant_id],
            )
            daily_args = ["--date", run_date, "--db", disc_db, "--tenant-id", tenant_id]
            if include_paper:
                daily_args.append("--paper-trade")
            daily_argv = python_module_argv("app.jobs.daily_runner", daily_args)

            qc1, qc2, qc3 = st.columns([2, 2, 2])
            with qc1:
                if st.button("Run Discovery", use_container_width=True, key="qc_run_discovery"):
                    argv = python_module_argv("app.discovery.discovery_cli", run_args)
                    job_id = run_subprocess_job_in_thread(store=ops_store, argv=argv, cwd=str(repo_root), title="discovery run")
                    st.session_state.ops_selected_job_id = job_id
                    st.success(f"Started job {job_id[:8]}")
                    st.rerun()
            with qc2:
                if st.button("Run Prediction Queue", use_container_width=True, key="qc_run_pred_queue"):
                    job_id = run_subprocess_job_in_thread(store=ops_store, argv=predq_argv, cwd=str(repo_root), title="prediction queue")
                    st.session_state.ops_selected_job_id = job_id
                    st.success(f"Started job {job_id[:8]}")
                    st.rerun()
            with qc3:
                if st.button("Run Full Daily Pipeline", type="primary", use_container_width=True, key="qc_run_full_daily"):
                    job_id = run_subprocess_job_in_thread(store=ops_store, argv=daily_argv, cwd=str(repo_root), title="daily runner")
                    st.session_state.ops_selected_job_id = job_id
                    st.success(f"Started job {job_id[:8]}")
                    st.rerun()

            with st.expander("Command previews", expanded=False):
                st.code(" ".join([str(x) for x in predq_argv]), language="bash")
                st.code(" ".join([str(x) for x in daily_argv]), language="bash")

            st.divider()
            st.markdown("**Scheduling (Windows Task Scheduler)**")
            st.caption("Use this if you want the machine to run the nightly pipeline automatically.")
            task_name = st.text_input("Task name", value="AlphaEngine_DiscoveryNightly", key="nightly_task_name")
            task_time = st.text_input("Start time (HH:MM, 24h)", value="21:30", key="nightly_task_time")
            st.caption("Tip: choose a time after market close so today's bars exist (e.g. 21:30 local).")

            # Schedule should not pass a fixed --date; nightly defaults to today's date.
            sched_args = [
                "nightly",
                "--db",
                disc_db,
                "--tenant-id",
                tenant_id,
                "--min-adv",
                str(float(nightly_min_adv)),
                "--outcomes-scope",
                str(nightly_out_scope),
                "--stats-window",
                str(int(nightly_stats_window)),
            ]
            sched_cmd = " ".join([str(x) for x in python_module_argv("app.discovery.discovery_cli", sched_args)])
            st.text_area("Suggested scheduled command", value=sched_cmd, height=80)

            create_cmd = f'schtasks /Create /F /SC DAILY /ST {task_time} /TN "{task_name}" /TR "{sched_cmd}"'
            st.text_area("schtasks create", value=create_cmd, height=80)

            confirm_sched = st.checkbox("I understand this creates/overwrites a scheduled task on this machine", value=False, key="nightly_sched_confirm")
            if st.button("Create scheduled task", disabled=not confirm_sched, use_container_width=True):
                argv = ["schtasks", "/Create", "/F", "/SC", "DAILY", "/ST", str(task_time), "/TN", str(task_name), "/TR", str(sched_cmd)]
                job_id = run_subprocess_job_in_thread(store=ops_store, argv=argv, cwd=str(repo_root), title="create scheduled task (discovery nightly)")
                st.session_state.ops_selected_job_id = job_id
                st.success(f"Started job {job_id[:8]}")
                st.rerun()

            st.divider()
            st.markdown("**Promote → Watchlist + Prediction Queue**")
            p1, p2, p3, p4, p5 = st.columns([2, 2, 2, 2, 2])
            with p1:
                prom_date = st.date_input("Promote date", value=date.today(), key="promote_date")
            with p2:
                prom_top = int(st.number_input("Top K", min_value=1, max_value=200, value=20, step=5, key="promote_top"))
            with p3:
                prom_window = int(st.number_input("Window days", min_value=2, max_value=30, value=5, step=1, key="promote_window"))
            with p4:
                prom_min_overlap = int(st.number_input("Min overlap", min_value=1, max_value=5, value=2, step=1, key="promote_min_overlap"))
            with p5:
                prom_min_days = int(st.number_input("Min days seen", min_value=1, max_value=30, value=3, step=1, key="promote_min_days"))
            prom_min_score = float(st.slider("Min avg score", min_value=0.0, max_value=1.0, value=0.85, step=0.01, key="promote_min_score"))

            if st.button("Promote watchlist", use_container_width=True):
                argv = python_module_argv(
                    "app.discovery.discovery_cli",
                    [
                        "promote",
                        "--date",
                        prom_date.isoformat(),
                        "--db",
                        disc_db,
                        "--tenant-id",
                        tenant_id,
                        "--top",
                        str(prom_top),
                        "--window-days",
                        str(prom_window),
                        "--min-overlap",
                        str(prom_min_overlap),
                        "--min-days-seen",
                        str(prom_min_days),
                        "--min-score",
                        str(prom_min_score),
                    ],
                )
                job_id = run_subprocess_job_in_thread(store=ops_store, argv=argv, cwd=str(repo_root), title="discovery promote")
                st.session_state.ops_selected_job_id = job_id
                st.success(f"Started job {job_id[:8]}")
                st.rerun()

            st.divider()
            st.markdown("**Outcomes + Stats (feedback loop)**")
            oc1, oc2, oc3 = st.columns([2, 2, 2])
            with oc1:
                out_date = st.date_input("Outcomes date", value=date.today(), key="outcomes_date")
            with oc2:
                out_h = st.multiselect("Horizons", options=[1, 5, 20], default=[1, 5, 20], key="outcomes_horizons")
            with oc3:
                stats_window = int(st.number_input("Stats window (days)", min_value=7, max_value=180, value=30, step=7, key="stats_window_days"))

            if st.button("Compute outcomes", use_container_width=True):
                hs = ",".join([str(int(x)) for x in (out_h or [1, 5, 20])])
                argv = python_module_argv(
                    "app.discovery.discovery_cli",
                    ["outcomes", "--date", out_date.isoformat(), "--db", disc_db, "--tenant-id", tenant_id, "--horizons", hs],
                )
                job_id = run_subprocess_job_in_thread(store=ops_store, argv=argv, cwd=str(repo_root), title="discovery outcomes")
                st.session_state.ops_selected_job_id = job_id
                st.success(f"Started job {job_id[:8]}")
                st.rerun()

            if st.button("Compute stats (5d)", use_container_width=True):
                argv = python_module_argv(
                    "app.discovery.discovery_cli",
                    [
                        "stats",
                        "--end-date",
                        out_date.isoformat(),
                        "--db",
                        disc_db,
                        "--tenant-id",
                        tenant_id,
                        "--window",
                        str(stats_window),
                        "--horizons",
                        "5,20",
                    ],
                )
                job_id = run_subprocess_job_in_thread(store=ops_store, argv=argv, cwd=str(repo_root), title="discovery stats")
                st.session_state.ops_selected_job_id = job_id
                st.success(f"Started job {job_id[:8]}")
                st.rerun()

        st.subheader("Launch a job")
        action = st.selectbox(
            "Action",
            options=[
                "Backfill coverage check (no network)",
                "Backfill a date range",
                "Ingest health report",
                "Cleanup stalled ingest windows (dry-run)",
                "Build ML dataset",
                "Extend ML dataset history",
                "Load training data (full_history + yfinance)",
                "Download FRED macro dumps",
                "Download dump: Tiingo",
                "Download dump: Alpha Vantage",
                "Download dump: FMP",
            ],
            index=0,
            key="ops_job_action",
        )

        db_path = st.text_input("DB path", value="data/alpha.db", key="ops_job_db_path")
        c1, c2, c3, c4 = st.columns([2, 2, 2, 2])
        with c1:
            start_d = st.date_input("Start", value=(date.today() - timedelta(days=30)), key="ops_job_start")
        with c2:
            end_d = st.date_input("End (inclusive)", value=(date.today()), key="ops_job_end")
        with c3:
            batch_size_days = int(
                st.selectbox("Batch size (days)", options=[1, 2, 7, 14, 30], index=0, key="ops_job_batch")
            )
        with c4:
            replay = st.toggle("Replay after fetch", value=True, key="ops_job_replay")

        # `backfill-range` CLI uses half-open [start, end). Users think in inclusive dates,
        # so we translate End(inclusive) -> end_exclusive = end + 1 day.
        end_excl = end_d + timedelta(days=1)

        # Build argv for the selected action
        title = ""
        argv: list[str] = []
        notes: list[str] = []

        if action == "Backfill coverage check (no network)":
            title = "backfill_cli backfill-range --check-only"
            argv = python_module_argv(
                "app.ingest.backfill_cli",
                [
                    "backfill-range",
                    "--start",
                    start_d.isoformat(),
                    "--end",
                    end_excl.isoformat(),
                    "--db",
                    db_path,
                    "--batch-size-days",
                    str(batch_size_days),
                    "--check-only",
                ],
            )
            notes.append("Does not fetch; prints missing/partial/complete windows using existing markers.")
            notes.append(f"Range interpreted as {start_d.isoformat()} → {end_d.isoformat()} (inclusive).")

        elif action == "Backfill a date range":
            title = "backfill_cli backfill-range"
            argv = python_module_argv(
                "app.ingest.backfill_cli",
                [
                    "backfill-range",
                    "--start",
                    start_d.isoformat(),
                    "--end",
                    end_excl.isoformat(),
                    "--db",
                    db_path,
                    "--batch-size-days",
                    str(batch_size_days),
                    ("--replay" if replay else "--no-replay"),
                ],
            )
            notes.append("Runs the full backfill runner for the selected window. May make API calls for recent windows.")
            notes.append(f"Range interpreted as {start_d.isoformat()} → {end_d.isoformat()} (inclusive).")

        elif action == "Ingest health report":
            title = "backfill_cli ingest-health"
            argv = python_module_argv(
                "app.ingest.backfill_cli",
                [
                    "ingest-health",
                    "--db",
                    db_path,
                    "--start",
                    start_d.isoformat(),
                    "--end",
                    end_excl.isoformat(),
                    "--batch-size-days",
                    str(batch_size_days),
                ],
            )
            notes.append("Summarizes coverage %, freshness, drift warnings, and latency per source over the date range.")
            notes.append(f"Range interpreted as {start_d.isoformat()} → {end_d.isoformat()} (inclusive).")

        elif action == "Cleanup stalled ingest windows (dry-run)":
            title = "backfill_cli ingest-runs-cleanup --dry-run"
            argv = python_module_argv(
                "app.ingest.backfill_cli",
                [
                    "ingest-runs-cleanup",
                    "--db",
                    db_path,
                    "--dry-run",
                ],
            )
            notes.append("Marks stale `ingest_runs` rows (status=running) as failed. Dry-run prints changes only.")

        elif action in {"Build ML dataset", "Extend ML dataset history"}:
            title = "ml.dataset_cli build"
            syms = st.text_input("Symbols (comma-separated)", value=((ticker or "NVDA")), key="ml_job_symbols")
            sym_list = [s.strip().upper() for s in syms.split(",") if s.strip()]
            j1, j2, j3 = st.columns(3)
            job_tenant = j1.selectbox("Tenant", options=["ml_train", "default", "backfill"], index=0, key="ml_job_tenant")
            job_horizon = j2.selectbox("Horizon", options=["7d", "1d", "30d"], index=0, key="ml_job_horizon")
            job_min_cov = float(j3.selectbox("Min coverage", options=["0.5", "0.6", "0.7", "0.8"], index=1, key="ml_job_mincov"))
            if action == "Extend ML dataset history":
                start_for_fix = (date.today() - timedelta(days=2190)).isoformat()  # ~6 years
                notes.append(f"Extends ML history back to {start_for_fix}.")
                start_arg = start_for_fix
            else:
                start_arg = start_d.isoformat()

            argv = python_module_argv(
                "app.ml.dataset_cli",
                [
                    "build",
                    "--symbols",
                    ",".join(sym_list) if sym_list else "NVDA",
                    "--horizon",
                    job_horizon,
                    "--start",
                    start_arg,
                    "--end",
                    end_d.isoformat(),
                    "--db",
                    db_path,
                    "--tenant-id",
                    job_tenant,
                    "--min-coverage",
                    str(job_min_cov),
                ],
            )
            notes.append("Builds `ml_learning_rows` (features + labels) for training. Requires `price_bars` for symbol + SPY.")

        elif action == "Load training data (full_history + yfinance)":
            title = "scripts/load_training_data.py"
            argv = python_script_argv(repo_root / "scripts" / "load_training_data.py", ["--no-train"])
            notes.append("Loads full_history/ CSVs and fetches ^VIX, BTC-USD, CL=F, DX-Y.NYB via yfinance into price_bars.")
            notes.append("Runs against both 'default' and 'ml_train' tenants. Pass --no-train to skip re-training (done here by default).")

        elif action == "Download FRED macro dumps":
            title = "scripts/download_fred_dump.py"
            argv = python_script_argv(repo_root / "scripts" / "download_fred_dump.py")
            notes.append("Downloads FEDFUNDS, T10Y2Y, DFII10, UNRATE parquets into data/raw_dumps/fred/.")
            notes.append("Requires FRED_API_KEY in .env. Free key: https://fred.stlouisfed.org/docs/api/api_key.html")

        elif action == "Download dump: Tiingo":
            title = "scripts/download_tiingo.py"
            argv = python_script_argv(repo_root / "scripts" / "download_tiingo.py")
            notes.append("Requires `TIINGO_API_KEY`. Writes CSVs into `data/raw_dumps/tiingo/`.")

        elif action == "Download dump: Alpha Vantage":
            title = "scripts/download_alpha_vantage.py"
            argv = python_script_argv(repo_root / "scripts" / "download_alpha_vantage.py")
            notes.append("Requires `ALPHA_VANTAGE_API_KEY` (script currently has a default). Writes CSVs into `data/raw_dumps/alpha_vantage/`.")

        elif action == "Download dump: FMP":
            title = "scripts/download_fmp.py"
            argv = python_script_argv(repo_root / "scripts" / "download_fmp.py")
            notes.append("Requires `FMP_API_KEY`. Writes CSVs into `data/raw_dumps/fmp/`.")

        if notes:
            st.caption(" ".join(notes))

        st.markdown("**Command preview**")
        st.code(" ".join([str(a) for a in argv]), language="bash")

        confirm = st.checkbox("I understand and want to run this job", value=False)
        if st.button("Run job", type="primary", disabled=not confirm):
            job_id = run_subprocess_job_in_thread(store=ops_store, argv=argv, cwd=str(repo_root), title=title)
            st.session_state.ops_selected_job_id = job_id
            st.success(f"Started job {job_id[:8]}")
            st.rerun()

        st.subheader("Recent jobs")
        jobs = ops_store.list_jobs(limit=25)
        if jobs:
            job_df = pd.DataFrame(
                [
                    {
                        "id": j.id,
                        "status": j.status,
                        "started_at": j.started_at,
                        "finished_at": j.finished_at,
                        "exit_code": j.exit_code,
                        "command": j.command,
                    }
                    for j in jobs
                ]
            )
            st.dataframe(_arrow_safe_df(job_df), use_container_width=True, hide_index=True)

            job_ids = [j.id for j in jobs]
            default_job_id = st.session_state.get("ops_selected_job_id")
            if default_job_id not in job_ids:
                default_job_id = job_ids[0]
            selected_job_id = st.selectbox(
                "View job logs",
                options=job_ids,
                index=job_ids.index(default_job_id),
            )
            st.session_state.ops_selected_job_id = selected_job_id

            job = ops_store.get_job(job_id=selected_job_id)
            if job:
                st.caption(f"Job: `{job.id}` status={job.status} exit_code={job.exit_code}")

            events = ops_store.list_job_events(job_id=selected_job_id, limit=2000)
            if events:
                text = "\n".join([f"[{e.ts} {e.stream}] {e.line}" for e in events[-500:]])
                st.text_area("Log (tail)", value=text, height=320)
            else:
                st.caption("No logs recorded yet.")
        else:
            st.caption("No jobs recorded yet.")

    # ── Tab 4: Factor Importance ──────────────────────────────────────────────
    with tab_factors:
        import json as _json
        import pandas as _pd

        st.subheader("ML Factor Importance")

        # Controls row
        fc1, fc2, fc3 = st.columns([2, 2, 2])
        with fc1:
            fi_horizon = st.selectbox("Horizon", options=["7d", "1d", "30d"], index=0, key="fi_horizon")
        with fc2:
            fi_top_n = st.slider("Top N factors", min_value=5, max_value=30, value=15, key="fi_top_n")
        with fc3:
            fi_view = st.selectbox("View", options=["Aggregated (mean |coef|)", "Time series", "Per-model detail"], index=0, key="fi_view")

        # Load models
        try:
            fi_rows = conn.execute(
                """
                SELECT model_id, horizon, train_start, train_end,
                       model_accuracy, ic_score, win_rate, train_rows,
                       feature_importance_json, passed_gate
                FROM ml_models
                WHERE passed_gate = 1 AND horizon = ?
                ORDER BY train_start ASC
                """,
                (fi_horizon,),
            ).fetchall()
        except Exception as _e:
            fi_rows = []
            st.error(f"DB error: {_e}")

        if not fi_rows:
            st.warning(f"No passed-gate models found for horizon={fi_horizon}. Run training first.")
        else:
            # Parse factor importance into a DataFrame
            fi_records = []
            for r in fi_rows:
                try:
                    fi_dict = _json.loads(r["feature_importance_json"])
                except Exception:
                    continue
                fi_records.append({
                    "model_id": r["model_id"][:8],
                    "train_start": r["train_start"],
                    "train_end": r["train_end"],
                    "accuracy": r["model_accuracy"],
                    "ic_score": r["ic_score"],
                    "win_rate": r["win_rate"],
                    "train_rows": r["train_rows"],
                    **{f: abs(float(v)) for f, v in fi_dict.items()},
                })

            fi_df = _pd.DataFrame(fi_records)
            meta_cols = ["model_id", "train_start", "train_end", "accuracy", "ic_score", "win_rate", "train_rows"]
            factor_cols = [c for c in fi_df.columns if c not in meta_cols]

            # ── View: Aggregated ─────────────────────────────────────────────
            if fi_view == "Aggregated (mean |coef|)":
                agg = fi_df[factor_cols].mean().sort_values(ascending=False)
                top_factors = agg.head(fi_top_n)

                st.caption(f"{len(fi_rows)} models  |  {len(factor_cols)} factors  |  sorted by mean |coef| across all windows")

                # Bar chart
                chart_df = top_factors.reset_index()
                chart_df.columns = ["Factor", "Mean |coef|"]
                st.bar_chart(chart_df.set_index("Factor"), height=400)

                # Table with presence rate
                presence = (fi_df[factor_cols] > 0.001).mean()
                summary = _pd.DataFrame({
                    "Mean |coef|": agg,
                    "Max |coef|": fi_df[factor_cols].max(),
                    "Presence %": (presence * 100).round(1),
                }).head(fi_top_n).reset_index()
                summary.columns = ["Factor", "Mean |coef|", "Max |coef|", "Present in % of models"]
                summary["Mean |coef|"] = summary["Mean |coef|"].map("{:.4f}".format)
                summary["Max |coef|"] = summary["Max |coef|"].map("{:.4f}".format)
                summary["Present in % of models"] = summary["Present in % of models"].map("{:.0f}%".format)
                st.dataframe(summary, use_container_width=True, hide_index=True)

            # ── View: Time series ────────────────────────────────────────────
            elif fi_view == "Time series":
                agg = fi_df[factor_cols].mean().sort_values(ascending=False)
                top_factors = list(agg.head(fi_top_n).index)

                st.caption(f"How top {fi_top_n} factor weights evolve across {len(fi_rows)} training windows")

                ts_df = fi_df[["train_start"] + top_factors].copy()
                ts_df["train_start"] = _pd.to_datetime(ts_df["train_start"])
                ts_df = ts_df.set_index("train_start").sort_index()

                st.line_chart(ts_df[top_factors], height=450)

                with st.expander("Raw time-series table", expanded=False):
                    display = ts_df[top_factors].copy()
                    for c in display.columns:
                        display[c] = display[c].map("{:.4f}".format)
                    st.dataframe(display, use_container_width=True)

            # ── View: Per-model detail ────────────────────────────────────────
            else:
                model_options = [
                    f"{r['train_start'][:10]} -> {r['train_end'][:10]}  acc={r['model_accuracy']:.3f}  ic={r['ic_score']:.3f}"
                    for r in fi_rows
                ]
                selected_idx = st.selectbox("Select model window", options=range(len(model_options)),
                                            format_func=lambda i: model_options[i],
                                            index=len(model_options) - 1, key="fi_model_sel")
                r = fi_rows[selected_idx]
                try:
                    fi_dict = _json.loads(r["feature_importance_json"])
                except Exception:
                    fi_dict = {}

                mc1, mc2, mc3, mc4 = st.columns(4)
                mc1.metric("Accuracy", f"{r['model_accuracy']:.1%}")
                mc2.metric("IC Score", f"{r['ic_score']:.3f}")
                mc3.metric("Win Rate", f"{r['win_rate']:.1%}")
                mc4.metric("Train Rows", f"{r['train_rows']:,}")

                sorted_fi = sorted(fi_dict.items(), key=lambda x: abs(x[1]), reverse=True)[:fi_top_n]
                detail_df = _pd.DataFrame(sorted_fi, columns=["Factor", "|Coef|"])
                detail_df["|Coef|"] = detail_df["|Coef|"].map("{:.4f}".format)
                st.dataframe(detail_df, use_container_width=True, hide_index=True)

                bar_df = _pd.DataFrame(sorted_fi, columns=["Factor", "Coef"])
                bar_df["Coef"] = bar_df["Coef"].abs()
                st.bar_chart(bar_df.set_index("Factor"), height=350)

            # ── Model quality table ───────────────────────────────────────────
            with st.expander("Model quality across windows", expanded=False):
                qual_df = fi_df[["train_start", "train_end", "accuracy", "ic_score", "win_rate", "train_rows"]].copy()
                qual_df.columns = ["Train Start", "Train End", "Accuracy", "IC Score", "Win Rate", "Rows"]
                for c in ["Accuracy", "Win Rate"]:
                    qual_df[c] = qual_df[c].map("{:.1%}".format)
                qual_df["IC Score"] = qual_df["IC Score"].map("{:.3f}".format)
                qual_df["Rows"] = qual_df["Rows"].map("{:,}".format)
                st.dataframe(qual_df, use_container_width=True, hide_index=True)
