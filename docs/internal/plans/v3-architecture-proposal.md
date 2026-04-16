# Alpha Engine v3 Architecture Proposal
## From Signal Generator to Adaptive Trading System

### Executive Summary

Alpha Engine v2 has successfully built a sophisticated signal generation pipeline with strategy competition, regime-aware consensus, and automated scoring. However, the system remains a **signal generator** rather than a **trading system**. 

v3 transforms Alpha Engine into a true adaptive trading system by adding the missing decision engine that allocates capital based on signals, manages risk, and learns from outcomes.

**CRITICAL REALIZATION:** We must prove we have an edge before building complex systems on top of noise.

### Current State Assessment (v2)

**What We Have Built:**
- Strategy framework with genetic optimization and promotion
- Regime-aware consensus (sentiment vs quant)
- Automated prediction scoring and replay
- Solid data pipeline and feature engineering
- Comprehensive logging and monitoring

**What We Actually Do:** "We generate signals and log results"

**What We Need to Do:** "We select, weight, and simulate capital allocation"

**Critical Gaps Identified:**
1. **No Single Decision Surface** - missing unified signal scoring function
2. **No Portfolio Simulation** - everything is theoretical without daily portfolio testing
3. **Signal Clustering Problem** - "top N" ignores concentration risk
4. **Time Alignment Issues** - mixing discovery timestamps with price availability creates bias
5. **Missing No-Trade Baseline** - need cash comparison for high volatility periods
6. **No Selection Logging** - don't track why signals were selected
7. **Kill Mechanism Too Late** - should exclude bad strategies even in simulation
8. **Under-leveraged Regime System** - no hard gating based on regime performance
9. **Benchmarking Too Late** - must prove edge before building complexity
10. **Premature Feature Attribution** - need signal attribution first, not feature attribution

### v3 Architecture Vision

**Transform from:** Signal Generator
**Transform to:** Adaptive Trading System with Decision Engine

#### Core Philosophy
- **Signal Generation** (v2 strength) + **Decision Engine** (v3 addition) = **Trading System**
- Every signal must answer: "Should we allocate capital? yes/no + how much"
- Every outcome must answer: "Did this make money and should we continue?"

#### The Missing Core Primitive
```python
def score_signal(signal):
    return (
        w1 * confidence +
        w2 * strategy_win_rate +
        w3 * regime_performance +
        w4 * risk_adjustment
    )
```

**Everything flows from this single scalar decision surface.**

### REVISED Phase Structure (Corrected Approach)

### Phase 1: Portfolio Simulation + Benchmark (MUST DO FIRST)

**Objective:** Prove we have an edge before building anything else.

#### 1.1 Daily Portfolio Simulation (with Critical Constraints)
```python
def simulate_daily_portfolio(date):
    # Each day:
    # 1. Get all signals for that day
    # 2. Filter out bad strategies (kill mechanism)
    # 3. Rank by simple score (confidence + basic win rate)
    # 4. Apply diversification constraints
    # 5. Take top N signals (10-20)
    # 6. Equal weight positions
    # 7. Calculate daily PnL with strict time alignment
    # 8. Track cumulative return
    pass
```

**Critical Constraints:**
- **Signal Clustering Limits:** Max 2 signals per ticker, max 3 per strategy
- **Time Alignment Rules:** Entry = next market open AFTER signal, Exit = exact horizon close
- **Kill Mechanism:** Exclude strategies with win_rate < threshold (e.g., 45%)
- **Selection Logging:** Log selected_rank, selection_score, selection_reason for each signal

**Simulation Rules:**
- **Top N Selection:** 10-20 signals per day (with clustering constraints)
- **Equal Weighting:** Simple, transparent allocation
- **No Overfitting:** Basic ranking only
- **Full History:** Simulate on entire available dataset
- **Strict Time Rules:** Prevent look-ahead bias

#### 1.2 Benchmark Comparison (Expanded)
```python
def compare_vs_benchmarks(portfolio_returns):
    # Compare against:
    # - SPY (market)
    # - Random selection (significance)
    # - Equal weight universe (baseline)
    # - Cash (no-trade baseline)
    pass
```

**Critical Questions to Answer:**
- Does our selection beat SPY consistently?
- Does our selection beat cash (important in high volatility)?
- Is the outperformance statistically significant?
- What's the Sharpe ratio and maximum drawdown?
- Does the edge persist across different time periods?

#### 1.3 Signal Attribution + Selection Analysis
```python
def analyze_signal_performance():
    # Which signals actually make money?
    # Strategy-level attribution, not feature-level
    # Selection decision analysis
    pass
```

**Signal Analysis:**
- **Strategy Win Rates:** Which strategies generate profitable signals?
- **Regime Performance:** Which signals work in which regimes?
- **Timeframe Analysis:** Which horizons perform best?
- **Ticker Performance:** Are certain tickers more predictable?
- **Selection Effectiveness:** Do selected signals outperform rejected ones?

**Success Criteria for Phase 1 (Softer Gates):**
- **Primary:** Positive return vs SPY (any amount, consistency matters)
- **Secondary:** Win rate > 55% (directional consistency)
- **Tertiary:** Maximum drawdown < 25% (risk control)
- **Optional:** Sharpe ratio > 0.8 (risk-adjusted performance)

**Note:** Tighten criteria in later phases after proving basic edge

### Phase 2: Signal Ranking (ONLY if Phase 1 succeeds)

#### 2.1 Unified Signal Scoring
```python
def score_signal(signal):
    return (
        w1 * confidence +
        w2 * strategy_win_rate +
        w3 * regime_performance +
        w4 * risk_adjustment
    )
```

**Scoring Components:**
- **Confidence:** Prediction probability from consensus
- **Strategy Win Rate:** Historical success rate of this strategy
- **Regime Performance:** How this signal type performs in current regime
- **Risk Adjustment:** Volatility-adjusted expected return

#### 2.2 Signal Ranking Engine
```python
class SignalRanker:
    def rank_signals(self, signals: List[Signal]) -> List[RankedSignal]:
        # Apply unified scoring function
        # Return ranked list with scalar scores
        pass
```

#### 2.3 Hard Regime Gating
```python
def regime_gate(signal, current_regime):
    # If strategy X fails in regime Y -> stop using it
    # Hard gating, not just weighting
    pass
```

**Gating Rules:**
- **Strategy-Regime Kill Switch:** Disable strategies in regimes where they consistently fail
- **Minimum Sample Size:** Require sufficient data before gating decisions
- **Recovery Mechanism:** Re-enable strategies after sufficient evidence of recovery

### Phase 3: Capital Allocation (ONLY if Phase 2 improves performance)

#### 3.1 Position Sizing
```python
def calculate_position_size(signal_score, portfolio_risk):
    # Size positions based on:
    # - Signal strength
    # - Portfolio risk budget
    # - Correlation constraints
    pass
```

#### 3.2 Risk Management
```python
class PortfolioRiskManager:
    def monitor_risk(self, portfolio):
        # Volatility targeting
        # Drawdown control
        # Concentration limits
        pass
```

#### 3.3 Kill Mechanism
```python
def strategy_kill_switch(strategy_id):
    # Aggressive pruning of failing strategies
    # Not "maybe demote" but "kill immediately"
    pass
```

**Kill Criteria:**
- **Performance Threshold:** Kill if underperforms for X consecutive periods
- **Drawdown Limit:** Kill if maximum drawdown exceeds threshold
- **Signal Quality:** Kill if signal quality degrades below minimum

### Phase 4+: Advanced Analysis (ONLY if Phase 3 works)

#### 4.1 Feature Attribution (NOW it's time)
```python
class FeatureAttribution:
    def track_prediction_drivers(self, prediction):
        # SHAP values, feature importance
        # Only after we know signals work
        pass
```

#### 4.2 Causal Learning
```python
class CausalLearner:
    def learn_relationships(self, successful_signals):
        # Move beyond correlation to causation
        # Only for signals that actually make money
        pass
```

#### 4.3 Temporal Pattern Mining
```python
class TemporalPatternMiner:
    def mine_patterns(self, successful_outcomes):
        # Pattern mining on profitable signals only
        pass
```

### Implementation Roadmap (REVISED)

#### Phase 1: Portfolio Simulation + Benchmark (Weeks 1-3)
- **Week 1:** Daily portfolio simulation engine
- **Week 2:** Benchmark comparison framework (SPY, random, equal weight)
- **Week 3:** Signal attribution analysis and success criteria validation

**GO/NO-GO Decision:** Only proceed if Phase 1 meets success criteria

#### Phase 2: Signal Ranking (Weeks 4-6) - ONLY if Phase 1 succeeds
- **Week 4:** Unified signal scoring function implementation
- **Week 5:** Signal ranking engine with hard regime gating
- **Week 6:** Kill mechanism for failing strategies

**GO/NO-GO Decision:** Only proceed if Phase 2 improves performance vs Phase 1

#### Phase 3: Capital Allocation (Weeks 7-9) - ONLY if Phase 2 succeeds
- **Week 7:** Position sizing and risk budget implementation
- **Week 8:** Portfolio risk manager with drawdown control
- **Week 9:** Integration testing and performance validation

**GO/NO-GO Decision:** Only proceed if Phase 3 improves performance vs Phase 2

#### Phase 4+: Advanced Analysis (Weeks 10+) - ONLY if Phase 3 works
- **Week 10+:** Feature attribution, causal learning, pattern mining
- **Only after proving we have profitable signals**

### Reality Check

**Right Now:** "We generate signals and log results"

**After Phase 1:** "We select top N signals and simulate portfolio performance"

**After Phase 2:** "We rank signals with unified scoring and hard regime gating"

**After Phase 3:** "We allocate capital with risk management and kill mechanisms"

**If You Do ONLY ONE THING:**

**Daily portfolio simulation:**
```python
for day in history:
    signals = get_signals(day)
    
    ranked = sort(signals, key=confidence)
    
    selected = take_top_N(ranked, constraints)
    
    returns = compute_returns(selected)
    
    log(day, selected, returns)
```

**Then:**
```python
compare_to_spy()
compare_to_random()
compare_to_cash()
```

### Reality Checkpoint

After Phase 1 you will land in ONE of these:

**Case A: No Edge**
- Performance ~ random / SPY / cash
- Win rate ~ 50%
- No consistent alpha

**Actions:**
- Fix strategies
- Improve features
- Tighten filtering
- **Do NOT proceed to Phase 2**

**Case B: Weak Edge**
- Small positive alpha
- Unstable performance
- Win rate 55-60%

**Actions:**
- Improve ranking
- Add regime filtering
- Consider Phase 2 with caution

**Case C: Clear Edge** (rare but possible)
- Consistent outperformance
- Win rate > 60%
- Stable across periods

**Actions:**
- Proceed to Phase 2
- Full v3 architecture justified
- Scale up complexity

**Bottom Line:** This version is now correct in philosophy, properly staged, and low-risk to execute. The only remaining adjustments are the critical constraints that prevent biased backtests.

### Technical Architecture (REVISED)

#### Phase 1 Components (Minimal)
```
app/
  simulation/
    portfolio_simulator.py
    benchmark_engine.py
    signal_analyzer.py
```

#### Phase 2 Components (If Phase 1 succeeds)
```
app/
  decision/
    signal_scorer.py
    signal_ranker.py
    regime_gating.py
    kill_mechanism.py
```

#### Phase 3 Components (If Phase 2 succeeds)
```
app/
  allocation/
    position_sizer.py
    risk_manager.py
    portfolio_optimizer.py
```

#### Phase 4+ Components (If Phase 3 succeeds)
```
app/
  learning/
    feature_attribution.py
    causal_learner.py
    pattern_miner.py
```

#### Database Schema Changes (Phase 1)
```sql
-- Portfolio simulation results
CREATE TABLE portfolio_simulations (
    id TEXT PRIMARY KEY,
    simulation_date DATE,
    signals_selected INTEGER,
    portfolio_return REAL,
    spy_return REAL,
    cash_return REAL,
    sharpe_ratio REAL,
    max_drawdown REAL,
    win_rate REAL,
    created_at TIMESTAMP
);

-- Signal selection logging (NEW)
CREATE TABLE signal_selections (
    id TEXT PRIMARY KEY,
    simulation_date DATE,
    signal_id TEXT,
    selected_rank INTEGER,
    selection_score REAL,
    selection_reason TEXT,
    selected BOOLEAN,
    created_at TIMESTAMP
);

-- Strategy kill tracking (NEW)
CREATE TABLE strategy_kills (
    id TEXT PRIMARY KEY,
    strategy_id TEXT,
    kill_date DATE,
    kill_reason TEXT,
    win_rate_at_kill REAL,
    sample_size_at_kill INTEGER,
    reactivated_date DATE,
    created_at TIMESTAMP
);

-- Signal performance tracking
CREATE TABLE signal_performance (
    id TEXT PRIMARY KEY,
    strategy_id TEXT,
    regime TEXT,
    signal_count INTEGER,
    win_rate REAL,
    avg_return REAL,
    sample_size INTEGER,
    last_updated TIMESTAMP
);
```

#### Database Schema Changes (Phase 2+)
```sql
-- Signal rankings (Phase 2)
CREATE TABLE signal_rankings (
    id TEXT PRIMARY KEY,
    signal_id TEXT,
    unified_score REAL,
    confidence_score REAL,
    strategy_win_rate REAL,
    regime_performance_score REAL,
    risk_adjustment REAL,
    created_at TIMESTAMP
);

-- Portfolio allocations (Phase 3)
CREATE TABLE portfolio_allocations (
    id TEXT PRIMARY KEY,
    signal_id TEXT,
    ticker TEXT,
    position_size REAL,
    risk_budget REAL,
    allocation_date TIMESTAMP
);
```

### Success Metrics (REVISED)

#### Phase 1 Success Criteria (Must Pass)
1. **Positive Alpha** - Minimum 2% annual outperformance vs SPY
2. **Risk-Adjusted Returns** - Sharpe ratio > 1.0
3. **Drawdown Control** - Maximum drawdown < 20%
4. **Statistical Significance** - p < 0.05 for outperformance
5. **Consistency** - Positive returns in > 60% of simulated periods

#### Phase 2 Success Criteria (If Phase 1 passes)
1. **Improved Selection** - Signal ranking improves returns by > 10% vs Phase 1
2. **Regime Gating** - Hard regime gating reduces drawdowns by > 20%
3. **Kill Effectiveness** - Removing failing strategies improves Sharpe by > 15%

#### Phase 3 Success Criteria (If Phase 2 passes)
1. **Risk Management** - Portfolio volatility stays within 15% annual target
2. **Position Sizing** - Risk-adjusted allocation improves returns by > 10%
3. **Capital Efficiency** - Better risk-adjusted returns vs equal weighting

#### Technical Metrics
- **Simulation Speed** - Daily portfolio simulation in < 1 second
- **Data Efficiency** - Handle 1+ years of signal history
- **Reproducibility** - Deterministic results across runs
- **Monitoring** - Real-time performance dashboards

### Risk Mitigation

#### Implementation Risks
1. **Overfitting** - extensive cross-validation and out-of-sample testing
2. **Model Risk** - ensemble methods and model diversity
3. **Data Quality** - enhanced validation and anomaly detection
4. **Operational Risk** - comprehensive testing and rollback procedures

#### Business Risks
1. **Market Impact** - position sizing limits and execution algorithms
2. **Liquidity Risk** - real-time liquidity monitoring
3. **Regulatory Compliance** - pre-trade compliance checks
4. **Technology Risk** - redundancy and disaster recovery

### Conclusion

Alpha Engine v3 represents a critical transformation from signal generator to adaptive trading system. However, the key insight is that we must **prove we have an edge before building complexity**.

The corrected approach prioritizes validation over architecture:
1. **Phase 1:** Prove our signals can beat SPY through simple portfolio simulation
2. **Phase 2:** Only if Phase 1 succeeds, build signal ranking with hard regime gating
3. **Phase 3:** Only if Phase 2 succeeds, add capital allocation and risk management
4. **Phase 4+:** Only if Phase 3 succeeds, explore advanced learning and attribution

This phase-gated approach prevents us from building sophisticated analysis on top of noise. Each phase must demonstrate measurable improvement before proceeding to the next.

**The Bottom Line:** Right now we generate signals and log results. After Phase 1, we'll know if those signals actually make money. Everything else depends on that answer.

**Immediate Next Steps:**
1. **Review and approve** this corrected architecture proposal
2. **Assign development resources** to Phase 1: Portfolio Simulation + Benchmark
3. **Set up simulation environment** with historical signal data
4. **Begin implementation** of daily portfolio simulation engine
5. **Define success criteria** and GO/NO-GO decision points

**Critical Success Factor:** 
> If Phase 1 fails to demonstrate consistent alpha vs SPY, we stop and fix the signal generation before proceeding further.

**The Question Phase 1 Must Answer:**
> Do our signals, when selected and weighted simply, generate consistent alpha?

If the answer is yes, we proceed. If no, we fix the foundation before building the house.
