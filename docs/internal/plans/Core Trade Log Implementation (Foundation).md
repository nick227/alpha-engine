🧱 PHASE 1 — Core Trade Log (Foundation)
Create trade_log table
Define status enum (proposed, entered, missed, expired, completed)
Add indexes (symbol, created_at, status)
Build TradeLogEntry model/class
Create insert function create_trade_log_entry()
Create update function update_trade_status()
Add basic validation (entry < exit, timestamps valid)

🧠 PHASE 2 — Profile Precompute (Offline Stats)
Create signal_profile table
Define grouping keys (strategy, confidence_bucket, vol_bucket, regime)
Write query to extract historical trades
Compute time_to_success
Compute time_to_failure
Compute time_to_peak
Compute t50_win
Compute t80_win
Compute t50_fail
Compute return distribution
Store target_return
Store stop_return
Store entry_delay_profile
Build build_signal_profiles() job
Schedule profile rebuild (daily or weekly)

⚙️ PHASE 3 — Trade Proposal Generator (Broker Layer)
Create generate_trade_log_from_predictions()
Map prediction → profile key
Lookup signal_profile
Derive entry price range (ATR or distribution)
Derive entry time window (now → now + t_entry)
Derive exit price (target_return)
Derive exit window (t50 → t80)
Assign priority score
Insert TradeLog entry
Prevent duplicate active entries per symbol
Add expiry logic (auto-expire old trades)

⏱ PHASE 4 — Execution Hook
Build check_entry_conditions()
Check price ∈ entry range
Check time ∈ entry window
Build execute_trade_from_log()
Integrate with vectorbt from_orders()
Mark status → entered
Store entry timestamp + price

🎯 PHASE 5 — Exit Logic
Build check_exit_conditions()
Exit if price hits target
Exit if stop triggered
Exit if time window exceeded
Mark status → completed
Store exit timestamp + price

👻 PHASE 6 — Shadow Alpha (Missed Trades)
Detect expired entries not entered
Mark status → missed
Track price path after expiry
Compute “missed profit”
Compute “avoided loss”
Store missed trade metrics
Build shadow_alpha_metrics()

🧪 PHASE 7 — Robustness (Sensitivity Sweep)
Build simulate_price_shift(entry ± k*ATR)
Build simulate_exit_shift(exit ± k*ATR)
Run mini vectorbt sim per trade
Compute return sensitivity
Compute robustness score
Store robustness in TradeLog
Use robustness in ranking

⚖️ PHASE 8 — Allocation (Competition Layer)
Collect all active TradeLog entries
Rank by score (edge + confidence + robustness)
Compute volatility-adjusted score
Normalize scores → weights
Apply soft max allocation cap
Apply max_positions constraint
Apply sector/correlation filter (optional)
Compute final allocation %
Store allocation in TradeLog

🧮 PHASE 9 — Portfolio Feedback Loop
Aggregate results by horizon
Aggregate results by strategy
Compute win rate per bucket
Compute avg return per bucket
Compute time-to-success per bucket
Compare tactical vs strategic performance
Adjust allocation weights dynamically
Store allocation adjustments
Feed into next allocation cycle

🔄 PHASE 10 — Integration
Hook TradeLog generation into prediction pipeline
Hook execution loop into scheduler (hourly/daily)
Hook exit loop into same scheduler
Hook profile rebuild into batch job
Hook feedback loop into nightly job

📊 PHASE 11 — Observability (important)
Log all TradeLog state transitions
Build simple dashboard query
Track:
active trades
missed trades
win rate
avg duration
Track “missed vs taken performance”
Track allocation distribution

🧹 PHASE 12 — Safety / Cleanup
Add TTL for expired trades
Prevent duplicate symbol spam
Add sanity checks on price ranges
Add sanity checks on time windows
Add fallback if profile missing
Add default profile behavior

🚀 Optional (later)
Add pair-trade support (relative value)
Add ML-based sizing refinement
Add regime-aware profile switching
Add dynamic entry zone adjustment
Add capital bucket system
Add live broker integration

🧠 What you actually built
Predictions
→ TradeLog (4D contracts)
→ Execution (rules)
→ Outcomes
→ Feedback (trade + portfolio)
→ Profiles updated
🔥 Key takeaway

👉 This is NOT a rewrite
👉 This is a layered upgrade