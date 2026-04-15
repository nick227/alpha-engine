# Alpha Engine Directory Structure

## 🎯 Clean Root Directory

### **✅ Core Production Files**
```
run_paper_trading.py           # Main trading engine
run_daily_pipeline.bat         # Full automation pipeline
run_download_prices.bat         # Task Scheduler: 6:00 AM
run_discovery_nightly.bat      # Task Scheduler: 6:30 AM
run_replay_score.bat           # Task Scheduler: 7:30 AM
run_trading_morning.bat        # Manual morning trading
run_trading_report.bat         # Manual reporting
```

### **✅ Core Directories**
```
app/                          # Main application code
config/                        # Configuration files
data/                          # Database and data files
logs/                          # Log files
scripts/                       # Production scripts
dev_scripts/                   # Development scripts
tests/                          # Unit tests
docs/                           # Documentation
experiments/                    # Experimental code
```

---

## 📁 Organized Development Structure

### **✅ dev_scripts/ Organization**
```
dev_scripts/
├── scripts/                     # Production-ready scripts (55 items)
├── test_scripts/                 # Test scripts (10 items)
├── ab_test_scripts/              # A/B test scripts (6 items)
├── utility_scripts/              # Utility scripts (7 items)
└── [various development files]  # Core development tools
```

### **✅ Test Scripts (test_scripts/)**
```
test_adaptive_validation.py        # Adaptive system validation
test_axis_diversity.py           # Dimensional axis testing
test_dimensional_fixes.py        # Dimensional system fixes
test_enhanced_regime_system.py  # Enhanced regime testing
test_env_v2.py                  # Environment v2 testing
test_industry_adaptive_mock.py   # Industry adaptive testing
test_integrated_adaptive.py      # Integrated adaptive testing
test_lightweight_dimensional_ml.py # Lightweight dimensional ML testing
test_multidimensional_adaptive.py  # Multidimensional testing
test_regime_aware_ml_system.py  # Regime-aware ML testing
```

### **✅ A/B Test Scripts (ab_test_scripts/)**
```
adaptive_ab_test.py               # Adaptive A/B testing
final_adaptive_ab_test.py         # Final adaptive A/B test
final_multidimensional_ab_test.py # Multidimensional A/B test
large_ab_test.py                 # Large-scale A/B test
mass_adaptive_test.py            # Mass adaptive testing
simple_adaptive_test.py          # Simple adaptive A/B test
```

### **✅ Utility Scripts (utility_scripts/)**
```
collect_sync_adaptive_data.py     # Data collection utilities
debug_config_test.py             # Configuration debugging
run_mock_seeder.py              # Mock data seeding
seed_backtest_data.py            # Backtest data seeding
seed_mock_data.py               # Mock data generation
start.py                        # Development startup
verify_ranking.py               # Ranking verification
```

---

## 🎯 Key Benefits of This Structure

### **✅ Clean Root Directory**
- **Only essential files** in root
- **No loose test files** cluttering main directory
- **Clear separation** of concerns

### **✅ Logical Organization**
- **Production**: Core files and directories
- **Development**: Organized by purpose
- **Testing**: Separate from production code
- **Utilities**: Reusable development tools

### **✅ Easy Navigation**
- **Predictable structure**: Know where to find files
- **Purpose-based grouping**: Related files together
- **Scalable organization**: Room for growth

---

## 🚀 File Categories Explained

### **✅ Core Production Files**
- **`run_paper_trading.py`**: Main trading engine
- **`run_*.bat`**: Scheduled automation tasks
- **Configuration**: `config/` directory
- **Data**: `data/` and `logs/` directories

### **✅ Application Code**
- **`app/`**: Main application modules
- **Dimensional ML**: `app/ml/dimensional_tagger.py`
- **Outcome backfill**: `app/ml/outcome_backfill.py`
- **Core functionality**: Well-organized structure

### **✅ Development Tools**
- **Test scripts**: Individual component testing
- **A/B tests**: Comparative testing framework
- **Utilities**: Reusable development tools
- **Experiments**: Cutting-edge features

---

## 🎯 Maintenance Guidelines

### **✅ Where to Add New Files**
- **New test**: `dev_scripts/test_scripts/`
- **New A/B test**: `dev_scripts/ab_test_scripts/`
- **New utility**: `dev_scripts/utility_scripts/`
- **New experiment**: `experiments/`

### **✅ File Naming Conventions**
- **Tests**: `test_[feature].py`
- **A/B tests**: `[type]_ab_test.py`
- **Utilities**: `[action]_[tool].py`
- **Production**: Clear, descriptive names

---

## 🏆 Final Status

### **✅ Root Directory: CLEAN**
- **No loose files**: Everything organized
- **Clear structure**: Easy to navigate
- **Production ready**: Core files accessible

### **✅ Development: ORGANIZED**
- **Test scripts**: 10 files organized
- **A/B tests**: 6 files organized
- **Utilities**: 7 files organized
- **Logical grouping**: By purpose and type

### **✅ Scalability: MAINTAINED**
- **Room for growth**: Clear structure for expansion
- **Easy maintenance**: Predictable organization
- **Team friendly**: Clear file locations

**The directory structure is now clean, organized, and ready for both production deployment and development work.** 🎯
