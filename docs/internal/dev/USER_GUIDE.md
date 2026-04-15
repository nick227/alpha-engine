# Alpha Engine User Guide

## 🎯 Quick Start

### **For Daily Operations**
Most users will only need to check the automated results. The system runs automatically via Windows Task Scheduler.

### **For Development/Testing**
Use the manual commands below for development and testing.

---

## 📁 Key Scripts Overview

### **🔄 Automated Scripts (Task Scheduler)**
| Script | Purpose | Schedule | Log File |
|---------|----------|----------|------------|
| `run_download_prices.bat` | Download market data | Daily 6:00 AM | `logs\prices.log` |
| `run_discovery_nightly.bat` | Generate predictions | Daily 6:30 AM | `logs\discovery_nightly.log` |
| `run_replay_score.bat` | Score predictions | Daily 7:30 AM | `logs\replay_score.log` |

### **🛠️ Manual Scripts (Development)**
| Script | Purpose | When to Use |
|---------|----------|-------------|
| `run_paper_trading.py` | Paper trading simulation | Testing strategies |
| `auto_backfill_outcomes.py` | Fill real outcomes | Manual outcome filling |
| `test_dimensional_tagger.py` | Test dimensional system | Development/debugging |

---

## 🚀 Daily Operations

### **✅ What Runs Automatically**
1. **6:00 AM**: Market data downloaded
2. **6:30 AM**: Discovery pipeline generates predictions
3. **7:30 AM**: Replay/scoring evaluates performance
4. **Throughout**: Outcome backfill processes matured predictions

### **📊 What to Check Daily**
```bash
# Check today's results
python run_paper_trading.py --report-only --start-date 2026-04-14 --end-date 2026-04-21

# Check discovery vs baseline
python dev_scripts/scripts/compare_discovery_vs_baseline.py
```

### **📋 Log File Monitoring**
```
logs/
├── prices.log              # Market data download status
├── discovery_nightly.log    # Prediction generation status
└── replay_score.log         # Performance scoring status
```

---

## 🛠️ Manual Operations

### **📥 Running Discovery Pipeline**
```bash
# Generate predictions for specific date
python run_paper_trading.py --date 2026-04-14

# Generate with custom parameters
python run_paper_trading.py --date 2026-04-14 --strategies SC,BSS
```

### **📈 Performance Analysis**
```bash
# Full performance report
python run_paper_trading.py --report-only --start-date 2026-04-14 --end-date 2026-04-21

# Compare strategies
python dev_scripts/scripts/compare_discovery_vs_baseline.py
```

### **🔧 Dimensional System Testing**
```bash
# Test dimensional tagger
python test_dimensional_tagger.py

# Test outcome backfill
python app/ml/outcome_backfill.py

# Run automatic backfill
python scripts/auto_backfill_outcomes.py
```

---

## 🎯 Key Commands Reference

### **📊 Paper Trading Commands**
```bash
# Basic run
python run_paper_trading.py

# Specific date
python run_paper_trading.py --date 2026-04-14

# Report only
python run_paper_trading.py --report-only

# Date range report
python run_paper_trading.py --report-only --start-date 2026-04-14 --end-date 2026-04-21

# Specific strategies
python run_paper_trading.py --strategies SC,BSS

# Custom config
python run_paper_trading.py --config custom_config.json
```

### **🔍 Discovery Commands**
```bash
# Run discovery pipeline
python run_paper_trading.py --discovery

# Discovery with specific date
python run_paper_trading.py --discovery --date 2026-04-14

# Compare to baseline
python dev_scripts/scripts/compare_discovery_vs_baseline.py
```

### **📈 Analysis Commands**
```bash
# Recent performance
python run_paper_trading.py --report-only --recent 30

# Strategy comparison
python dev_scripts/scripts/compare_discovery_vs_baseline.py

# Full historical analysis
python run_paper_trading.py --report-only --start-date 2026-01-01 --end-date 2026-04-14
```

---

## 🔧 Dimensional ML System

### **🏗️ System Components**
| Component | Purpose | Status |
|-----------|---------|--------|
| `dimensional_tagger.py` | Axis-based prediction tagging | ✅ Complete |
| `outcome_backfill.py` | Automatic outcome filling | ✅ Complete |
| `auto_backfill_outcomes.py` | Daily backfill automation | ✅ Complete |

### **📊 System Status Commands**
```bash
# Check dimensional system status
python scripts/auto_backfill_outcomes.py

# Check outcome statistics
python -c "
from app.ml.outcome_backfill import OutcomeBackfill
backfill = OutcomeBackfill()
stats = backfill.get_outcome_statistics()
for key, value in stats.items():
    print(f'{key}: {value}')
"

# Check self-correction readiness
python -c "
from app.ml.dimensional_tagger import get_dimensional_tagger
tagger = get_dimensional_tagger()
status = tagger.get_real_outcome_status()
print(f'Can self-correct: {status[\"can_self_correct\"]}')
print(f'Outcomes needed: {status[\"min_outcomes_per_axis\"]} per axis')
"
```

---

## 📋 Troubleshooting

### **🔍 Common Issues**

#### **Issue: No predictions generated**
```bash
# Check discovery log
type logs\discovery_nightly.log

# Check data availability
python -c "
import sqlite3
conn = sqlite3.connect('data/alpha.db')
cursor = conn.execute('SELECT COUNT(*) FROM features')
print(f'Features available: {cursor.fetchone()[0]}')
conn.close()
"
```

#### **Issue: No outcomes filled**
```bash
# Check backfill status
python scripts/auto_backfill_outcomes.py

# Manually run backfill
python app/ml/outcome_backfill.py

# Check maturity status
sqlite3 data/alpha.db "SELECT COUNT(*) FROM dimensional_predictions WHERE matured = TRUE AND actual_return IS NULL;"
```

#### **Issue: Poor performance**
```bash
# Check recent performance
python run_paper_trading.py --report-only --recent 30

# Compare strategies
python dev_scripts/scripts/compare_discovery_vs_baseline.py

# Check dimensional weights
python -c "
from app.ml.dimensional_tagger import get_dimensional_tagger
tagger = get_dimensional_tagger()
weights = tagger.calculate_axis_weights()
for axis, weight in weights.items():
    print(f'{axis}: {weight:.3f}')
"
```

---

## 🎯 Performance Monitoring

### **📊 Key Metrics to Watch**
- **Win Rate**: Target >58% for SC, >54% for BSS
- **Outcome Coverage**: Should increase daily
- **Self-Correction Status**: Activates when 90+ outcomes
- **Axis Performance**: Watch for strong/weak performers

### **📈 Daily Checklist**
- [ ] Check `logs\prices.log` for data download
- [ ] Check `logs\discovery_nightly.log` for predictions
- [ ] Check `logs\replay_score.log` for scoring
- [ ] Run weekly performance report
- [ ] Monitor outcome coverage progress
- [ ] Watch for self-correction activation

---

## 🚀 Advanced Usage

### **🔧 Custom Configuration**
```bash
# Create custom config
cp config/default.json config/my_config.json

# Edit and use
python run_paper_trading.py --config config/my_config.json
```

### **📊 Custom Analysis**
```bash
# Strategy-specific analysis
python run_paper_trading.py --strategies SC --report-only

# Date-specific analysis
python run_paper_trading.py --date 2026-04-14 --report-only

# Custom date range
python run_paper_trading.py --report-only --start-date 2026-04-01 --end-date 2026-04-14
```

### **🔍 Development Testing**
```bash
# Test individual components
python test_dimensional_tagger.py
python test_lightweight_dimensional_ml.py
python test_axis_diversity.py

# Test backfill system
python app/ml/outcome_backfill.py

# Test self-correction (when ready)
python -c "
from app.ml.dimensional_tagger import get_dimensional_tagger
tagger = get_dimensional_tagger()
tagger._self_correcting_enabled = True
weights = tagger.calculate_axis_weights()
print(weights)
"
```

---

## 📞 Getting Help

### **🔍 Log Locations**
```
logs/
├── prices.log              # Data download issues
├── discovery_nightly.log    # Discovery pipeline issues
└── replay_score.log         # Scoring issues
```

### **🛠️ Debug Commands**
```bash
# Verbose discovery
python run_paper_trading.py --discovery --verbose

# Debug dimensional system
python test_dimensional_tagger.py --debug

# Check database status
sqlite3 data/alpha.db ".schema"
sqlite3 data/alpha.db "SELECT COUNT(*) FROM dimensional_predictions;"
```

---

## 🎯 Summary

### **✅ Daily Automation**
- **System runs automatically** via Task Scheduler
- **Check logs** for daily status
- **Monitor performance** weekly

### **✅ Manual Operations**
- **Use specific commands** for testing/development
- **Check log files** for troubleshooting
- **Monitor key metrics** for system health

### **✅ System Health**
- **Outcome coverage** should increase daily
- **Self-correction** activates at 90+ outcomes
- **Performance** should meet target thresholds

**The system is designed for "set it and forget it" operation with comprehensive monitoring and manual override capabilities.** 🎯
