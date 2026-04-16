# SEC Regulatory Data Integration

## Overview

Integration of SEC EDGAR data via sec-api to inject verified, low-noise regulatory signals into alpha engine.

## Architecture

### 1. Data Collection (`sec_ingest.py`)

**Core Forms Collected**:
- **Form 4**: Insider trading activity (buys/sells)
- **8-K**: Corporate events (mergers, exec changes, bankruptcy)
- **10-Q/10-K**: Financial fundamentals and earnings

**API Integration**:
```python
from sec_api import QueryApi, ExtractorApi
```

**Database Schema**:
```sql
CREATE TABLE regulatory_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    company_name TEXT NOT NULL,
    event_type TEXT NOT NULL,        -- "insider_buy", "merger", etc.
    source_type TEXT DEFAULT 'regulatory_event',
    filing_type TEXT NOT NULL,        -- "Form 4", "8-K", etc.
    filing_date TEXT NOT NULL,
    event_date TEXT NOT NULL,
    description TEXT,
    details TEXT,                    -- JSON details
    confidence REAL DEFAULT 1.0,       -- SEC data = high confidence
    processed_at TEXT
);
```

### 2. Signal Generation (`regulatory_signals.py`)

**Signal Types**:
- **Insider Activity**: Bullish bias on buys, bearish on sells
- **Corporate Events**: High impact (mergers, exec changes, bankruptcy)
- **Fundamental Updates**: Earnings and financial health

**Signal Structure**:
```python
{
    'symbol': 'AAPL',
    'signal_type': 'insider_activity',
    'event_type': 'insider_buy',
    'direction': 'bullish',
    'strength': 0.8,           # Weighted by magnitude
    'confidence': 0.9,           # High for SEC data
    'source': 'regulatory',
    'expires_at': '2026-04-23T10:00:00',
    'details': {
        'net_shares': 50000,
        'magnitude': 1.0
    }
}
```

### 3. ML Feature Integration (`regulatory_ml_features.py`)

**Feature Categories**:
```python
# Insider activity features
'regulatory_insider_buy_recent': 1.0,
'regulatory_insider_sell_recent': 0.0,
'regulatory_insider_net_activity': 5.0,
'regulatory_insider_magnitude': 1.0,

# Corporate event features
'regulatory_merger_recent': 0.0,
'regulatory_exec_change_recent': 0.0,
'regulatory_bankruptcy_recent': 0.0,
'regulatory_major_event_recent': 0.0,

# Fundamental features
'regulatory_earnings_recent': 1.0,
'regulatory_fundamental_health': 0.8,

# Composite features
'regulatory_signal_strength': 0.7,
'regulatory_confidence': 0.85,
'regulatory_bullish_bias': 0.3,
'regulatory_bearish_bias': -0.1,
'regulatory_event_count': 2.0
```

## Usage

### Installation

```bash
# Install SEC API
pip install sec-api

# Set API key
set SEC_API_KEY=your_api_key
# or export SEC_API_KEY=your_api_key
```

### Data Collection

```bash
# Collect last 7 days (default)
python scripts/collect_regulatory_data.py

# Collect last 30 days
python scripts/collect_regulatory_data.py 30

# Or use batch file
scripts\run_regulatory_collection.bat
```

### Signal Generation

```python
from app.regulatory.regulatory_signals import get_regulatory_signals

# Get all active signals
signals = get_regulatory_signals()

# Get signals for specific symbols
signals = get_regulatory_signals(['AAPL', 'MSFT', 'GOOGL'])
```

### ML Feature Extraction

```python
from app.regulatory.regulatory_ml_features import extract_regulatory_features

# Extract features for ML
features = extract_regulatory_features('AAPL')

# Features include 20+ regulatory signals
print(f"Insider buy signal: {features['regulatory_insider_buy_recent']}")
print(f"Merger activity: {features['regulatory_merger_recent']}")
print(f"Bullish bias: {features['regulatory_bullish_bias']}")
```

## Integration Points

### 1. Standalone Event Strategies

```python
# Merger arbitrage strategy
if regulatory_merger_recent > 0:
    # Implement merger-specific logic
    pass

# Insider trading strategy
if regulatory_insider_buy_recent > 0 and regulatory_insider_magnitude > 0.5:
    # Implement insider-following logic
    pass
```

### 2. Confirmation Layer

```python
# Use regulatory signals to confirm other strategies
base_signal = generate_technical_signal()
regulatory_confirmation = get_regulatory_signals([symbol])

if base_signal.direction == 'bullish' and regulatory_confirmation:
    # Increase confidence
    base_signal.confidence *= 1.2
```

### 3. ML Feature Input

```python
# Add regulatory features to existing ML feature set
base_features = extract_technical_features(symbol)
regulatory_features = extract_regulatory_features(symbol)

# Combine for high-quality ML input
combined_features = {**base_features, **regulatory_features}
```

## Performance Tracking

### Feature Performance Analysis

```python
from app.regulatory.regulatory_feature_tracker import get_regulatory_feature_tracker

tracker = get_regulatory_feature_tracker()
analysis = tracker.analyze_feature_performance(days_back=30)

print(f"Best performing features: {analysis['best_performers']}")
print(f"Average feature performance: {analysis['summary']['avg_performance']:.3%}")
```

## Benefits

### 1. Improved Model Reliability
- **Verified Data**: SEC filings are legally mandated and accurate
- **Low Noise**: Regulatory events are factual, not speculative
- **High Confidence**: Source confidence = 1.0 for SEC data

### 2. Reduced Noise Dependency
- **Ground Truth**: Anchors predictions in real-world events
- **Event-Driven**: Less reliance on noisy price patterns
- **Causal**: Clear cause-effect relationships

### 3. Enhanced Signal Quality
- **Insider Activity**: Direct insight into company insiders
- **Corporate Events**: High-impact merger/bankruptcy signals
- **Fundamental Updates**: Verified financial data

## Testing

```bash
# Test complete integration
python scripts/test_regulatory_integration.py

# Tests:
# ✅ SEC data collection
# ✅ Signal generation
# ✅ ML feature extraction
# ✅ Performance tracking
```

## Configuration

### Collection Settings
```python
# In sec_ingest.py
COLLECTION_INTERVAL_HOURS = 6  # Check for new filings every 6 hours
DEFAULT_DAYS_BACK = 7        # Default collection window
MAX_EVENTS_PER_RUN = 100      # Limit per collection cycle
```

### Signal Weights
```python
# In regulatory_signals.py
SIGNAL_WEIGHTS = {
    'insider_buy': 0.8,      # Strong bullish signal
    'insider_sell': -0.6,     # Bearish signal
    'merger': 0.9,           # Very strong event
    'exec_change': 0.3,        # Moderate signal
    'earnings': 0.5,           # Moderate signal
    'bankruptcy': -1.0,        # Very strong bearish
    'corp_event': 0.2           # Weak signal
}
```

## Monitoring

### Daily Collection
```bash
# Add to daily pipeline
scripts\run_regulatory_collection.bat
```

### Performance Dashboard
Track regulatory feature performance over time:
- Best performing event types
- Feature decay rates
- Signal accuracy by event type
- Correlation with price movements

## Safety

### API Limits
- **Rate Limits**: 10 requests/minute for free tier
- **Data Quality**: All data verified by SEC
- **Error Handling**: Graceful degradation if API unavailable

### Data Validation
- **Symbol Validation**: Cross-check ticker symbols
- **Date Validation**: Ensure filing dates are reasonable
- **Duplicate Prevention**: Database constraints prevent duplicates

## Future Enhancements

### 1. Real-time Filings
- WebSocket integration for immediate filing alerts
- Push notifications for major events
- Automated strategy triggering

### 2. Advanced Parsing
- NLP on 8-K content for sentiment
- Financial statement trend analysis
- Insider trading pattern detection

### 3. Cross-Reference
- Combine with news sentiment
- Correlate with price movements
- Multi-source signal confirmation

---

This regulatory integration provides a clean, factual signal layer that improves model reliability, reduces noise dependency, and anchors predictions in real-world events.
