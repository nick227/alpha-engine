#!/usr/bin/env python
"""
Full-universe signal validation.
Three-condition filter: vix_term > 0  AND  candle_body <= p33  AND  volume_zscore < 0
Stratified by: decade (2000s / 2010s / 2020s) and size quintile (avg dollar-volume proxy).
"""
from __future__ import annotations

import math
import sqlite3
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

DB = str(_ROOT / "data" / "alpha.db")


def ic_test(x, y, label=""):
    x, y = np.array(x, float), np.array(y, float)
    m = np.isfinite(x) & np.isfinite(y)
    x, y = x[m], y[m]
    r, p = stats.spearmanr(x, y)
    t = r * math.sqrt(len(x)) / math.sqrt(max(1 - r**2, 1e-9))
    sig = "***" if p < 0.001 else ("**" if p < 0.01 else ("*" if p < 0.05 else "ns"))
    da = np.mean(np.sign(x) == np.sign(y))
    if label:
        print(f"  {label:<44} IC={r:+.4f} t={t:+.1f} {sig}  dir={da:.1%}  n={len(x):,}")
    return r, p


def bucket_stats(sub, label):
    n = len(sub)
    r1 = sub["r1"].values
    r5 = sub["r5"].values
    m1 = np.mean(r1) * 10000
    m5 = np.mean(r5) * 10000
    dn = np.mean(r1 < 0)
    big_up = np.mean(r1 > 0.02)
    big_dn = np.mean(r1 < -0.02)
    print(
        f"  {label:<44} n={n:>7,}  1d={m1:>+5.0f}bp  5d={m5:>+5.0f}bp"
        f"  dn={dn:.1%}  >+2%={big_up:.1%}  <-2%={big_dn:.1%}"
    )


def main() -> None:
    t_start = time.time()
    conn = sqlite3.connect(DB)

    # ── Qualifying tickers ───────────────────────────────────────────
    print("Loading qualifying tickers...")
    qual = conn.execute("""
        SELECT ticker, COUNT(*) as n, MAX(DATE(timestamp)) as last
        FROM price_bars WHERE tenant_id='ml_train' AND timeframe='1d'
        GROUP BY ticker
        HAVING MAX(DATE(timestamp)) >= '2024-01-01' AND COUNT(*) >= 1250
        ORDER BY ticker
    """).fetchall()
    tickers = [r[0] for r in qual]
    print(f"  {len(tickers):,} qualifying tickers")

    # ── VIX term structure ───────────────────────────────────────────
    vix_rows = conn.execute(
        "SELECT DATE(timestamp), close FROM price_bars "
        "WHERE tenant_id='default' AND ticker='^VIX' AND timeframe='1d' ORDER BY timestamp"
    ).fetchall()
    vix3m_rows = conn.execute(
        "SELECT DATE(timestamp), close FROM price_bars "
        "WHERE tenant_id='default' AND ticker='^VIX3M' AND timeframe='1d' ORDER BY timestamp"
    ).fetchall()
    vix      = pd.Series({r[0]: float(r[1]) for r in vix_rows})
    vix3m    = pd.Series({r[0]: float(r[1]) for r in vix3m_rows})
    vix_term = (vix - vix3m).to_dict()
    print(f"  VIX term dates: {len(vix_term)}")

    # ── Size proxy: avg dollar-volume per ticker (last 252 bars) ────
    print("Computing size proxy (avg dollar-volume)...")
    ph = ",".join("?" * len(tickers))
    size_rows = conn.execute(f"""
        SELECT ticker, AVG(close * volume) as avg_dv
        FROM (
            SELECT ticker, close, volume,
                   ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY timestamp DESC) as rn
            FROM price_bars
            WHERE tenant_id='ml_train' AND timeframe='1d' AND ticker IN ({ph})
        ) WHERE rn <= 252
        GROUP BY ticker
    """, tickers).fetchall()
    size_map = {r[0]: float(r[1]) for r in size_rows if r[1] and r[1] > 0}
    dv_vals  = np.array([size_map.get(t, 0.0) for t in tickers])
    nonzero  = dv_vals[dv_vals > 0]
    dv_pcts  = np.percentile(nonzero, [20, 40, 60, 80])

    def size_quintile(ticker: str) -> str | None:
        dv = size_map.get(ticker, 0.0)
        if dv <= 0:
            return None
        if dv < dv_pcts[0]: return "Q1 micro"
        if dv < dv_pcts[1]: return "Q2 small"
        if dv < dv_pcts[2]: return "Q3 mid"
        if dv < dv_pcts[3]: return "Q4 large"
        return "Q5 mega"

    # ── Main loop: chunks of 300 tickers ────────────────────────────
    print("Computing signals (chunks of 300)...")
    CHUNK = 300
    records = []
    processed = 0

    for chunk_start in range(0, len(tickers), CHUNK):
        batch = tickers[chunk_start : chunk_start + CHUNK]
        ph_b  = ",".join("?" * len(batch))
        rows  = conn.execute(
            f"SELECT ticker, DATE(timestamp) as dt, open, high, low, close, volume "
            f"FROM price_bars "
            f"WHERE tenant_id='ml_train' AND timeframe='1d' AND ticker IN ({ph_b}) "
            f"ORDER BY ticker, dt",
            batch,
        ).fetchall()

        df = pd.DataFrame(rows, columns=["ticker","dt","open","high","low","close","volume"])
        for col in ("open","high","low","close","volume"):
            df[col] = pd.to_numeric(df[col], errors="coerce")

        for ticker, grp in df.groupby("ticker"):
            g  = grp.sort_values("dt").reset_index(drop=True)
            if len(g) < 30:
                continue
            cl  = g["close"].values.astype(float)
            op  = g["open"].values.astype(float)
            hi  = g["high"].values.astype(float)
            lo  = g["low"].values.astype(float)
            vl  = g["volume"].values.astype(float)
            dts = g["dt"].values
            sq  = size_quintile(ticker)

            for i in range(21, len(g) - 5):
                rng  = hi[i] - lo[i]
                body = float(np.clip((cl[i] - op[i]) / rng, -1.0, 1.0)) if rng > 0 else 0.0
                vw   = vl[i-20:i]
                vstd = float(np.std(vw))
                vz   = (vl[i] - float(np.mean(vw))) / vstd if vstd > 0 else 0.0
                r1   = (cl[i+1] / cl[i] - 1) if cl[i] > 0 else np.nan
                r5   = (cl[i+5] / cl[i] - 1) if cl[i] > 0 else np.nan
                dt_s = str(dts[i])[:10]
                vt   = vix_term.get(dt_s, np.nan)
                yr   = int(dt_s[:4])
                dec  = "2000s" if yr < 2010 else ("2010s" if yr < 2020 else "2020s")
                records.append((ticker, dt_s, body, vz, float(vt), r1, r5, sq, dec))

        processed += len(batch)
        if processed % 600 == 0 or processed >= len(tickers):
            elapsed = time.time() - t_start
            print(f"  {processed:>5}/{len(tickers)}  records={len(records):,}  t={elapsed:.0f}s")

    conn.close()

    # ── Build panel ──────────────────────────────────────────────────
    print(f"Building DataFrame from {len(records):,} records...")
    cols  = ["ticker","dt","candle_body","vzscore","vix_term","r1","r5","sq","decade"]
    panel = pd.DataFrame(records, columns=cols).dropna(
        subset=["r1","r5","vix_term","candle_body","vzscore"]
    )
    print(f"Clean panel: {len(panel):,} obs, {panel['ticker'].nunique():,} tickers")

    # ── Three-condition filter ───────────────────────────────────────
    fear      = panel["vix_term"] > 0
    t33_cb    = panel.loc[fear, "candle_body"].quantile(0.33)
    bear_body = panel["candle_body"] <= t33_cb
    low_vol   = panel["vzscore"] < 0.0

    all_fear  = panel[fear]
    setup     = panel[fear & bear_body & low_vol]
    setup_hv  = panel[fear & bear_body & ~low_vol]
    bull_fear = panel[fear & ~bear_body]

    # ── Results ──────────────────────────────────────────────────────
    print()
    print("=" * 65)
    print("FULL UNIVERSE VALIDATION")
    print(f"  Universe : {panel['ticker'].nunique():,} stocks  |  {len(panel):,} obs")
    print(f"  Fear days: {len(all_fear):,} obs  ({all_fear['ticker'].nunique():,} tickers)")
    print(f"  Setup    : {len(setup):,} obs  ({setup['ticker'].nunique():,} tickers)")
    print(f"  Cutoff   : candle_body <= {t33_cb:.3f}")
    print("=" * 65)

    print()
    print("--- IC: candle_body vs 1d return ---")
    ic_test(all_fear["candle_body"], all_fear["r1"],  "Full universe, fear regime -> 1d")
    ic_test(panel["candle_body"],    panel["r1"],     "Full universe, all regimes -> 1d")
    ic_test(all_fear["candle_body"], all_fear["r5"],  "Full universe, fear regime -> 5d")

    print()
    print("--- THREE-CONDITION SETUP vs CONTROLS ---")
    bucket_stats(setup,     "SETUP: fear + bear candle + low vol")
    bucket_stats(setup_hv,  "Fear + bear candle + HIGH vol")
    bucket_stats(bull_fear, "Fear + bull candle (opposite)")
    bucket_stats(all_fear,  "All fear days (no filter)")

    print()
    print("--- DECADE BREAKDOWN (setup only) ---")
    for dec in ["2000s", "2010s", "2020s"]:
        sub = setup[setup["decade"] == dec]
        if len(sub) >= 50:
            bucket_stats(sub, dec)

    print()
    print("--- DECADE: FULL FEAR REGIME (sanity check) ---")
    for dec in ["2000s", "2010s", "2020s"]:
        sub = all_fear[all_fear["decade"] == dec]
        if len(sub) >= 50:
            ic_test(sub["candle_body"], sub["r1"], f"IC in fear, {dec}")

    print()
    print("--- MARKET CAP QUINTILE (setup only) ---")
    for q in ["Q1 micro", "Q2 small", "Q3 mid", "Q4 large", "Q5 mega"]:
        sub = setup[setup["sq"] == q]
        if len(sub) >= 50:
            bucket_stats(sub, q)

    print()
    print("--- MARKET CAP: IC within each quintile (fear regime) ---")
    for q in ["Q1 micro", "Q2 small", "Q3 mid", "Q4 large", "Q5 mega"]:
        sub = all_fear[all_fear["sq"] == q]
        if len(sub) >= 50:
            ic_test(sub["candle_body"], sub["r1"], f"candle_body IC, {q}")

    print()
    print("--- CONCENTRATION: top 10 tickers by setup observation count ---")
    top10 = setup.groupby("ticker").size().nlargest(10)
    top10_ret = setup[setup["ticker"].isin(top10.index)]["r1"].mean() * 10000
    all_ret   = setup["r1"].mean() * 10000
    print(f"  Top 10 share: {top10.sum():,} / {len(setup):,} obs = {top10.sum()/len(setup):.1%}")
    print(f"  Mean 1d: top 10 = {top10_ret:+.0f}bp  |  full setup = {all_ret:+.0f}bp")
    for t, n in top10.items():
        tr = setup[setup["ticker"] == t]["r1"].mean() * 10000
        print(f"    {t:<10} {n:>6} obs  {tr:>+5.0f}bp")

    print()
    print(f"Total elapsed: {(time.time()-t_start)/60:.1f} min")


if __name__ == "__main__":
    main()
