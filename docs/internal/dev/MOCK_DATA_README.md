# Mock Data Seeder

This seeder populates the Alpha Engine database with realistic mock data for UI testing purposes.

## What it seeds

### Core Entities
- **Strategies**: 5 different strategies (sentiment, technical, momentum, mean reversion, AI/ML)
- **Prediction Runs**: Multiple runs covering different time periods
- **Price Data**: 90 days of historical OHLC data for 8 major tickers

### Events & Analysis
- **Raw Events**: News and events from various sources (Bloomberg, Reuters, etc.)
- **Scored Events**: AI-scored events with sentiment, direction, and confidence
- **Predictions**: Strategy predictions linked to scored events
- **Outcomes**: Realized prediction outcomes with performance metrics

### Performance Data
- **Strategy Performance**: Accuracy, returns, and alpha by horizon
- **Consensus Signals**: Multi-strategy consensus for each ticker
- **Champion Data**: Strategy promotion events and champion tracking
- **System Health**: Loop heartbeat data for system monitoring

## Data Coverage

The mock data provides coverage for all major UI components:

### Dashboard
- Top ten signals with rankings and performance metrics
- Strategy consensus by horizon (1d, 7d, 30d)
- Champion indicators with win rates and alpha
- Recent signals table

### Intelligence Hub
- Strategy performance matrix with efficiency ratings
- Individual strategy cards with prediction charts
- Performance rankings and stability metrics
- Market consensus and regime analysis

### Audit Page
- Adapter activity and event streams
- Strategy leaderboards and prediction logs
- Pipeline health and system diagnostics

## Usage

### Quick Start
```bash
python run_mock_seeder.py
```

### Manual Execution
```bash
python seed_mock_data.py
```

### Requirements
- Database must exist at `data/alpha.db`
- Run `prisma db push` or `alembic upgrade head` first if needed

## Data Characteristics

### Realistic Values
- Stock prices: $100-$500 range with daily volatility
- Prediction accuracy: 45-85% (realistic range)
- Alpha values: -0.05 to 0.15 (5-15% returns)
- Confidence scores: 0.3-0.95
- Sample sizes: 50-500 predictions per strategy

### Time Distribution
- Events: Past 30 days, 5-15 events per day
- Predictions: Past 30 days with various horizons
- Price data: 90 days of daily OHLC
- System heartbeats: Every 4 hours for past 24 hours

### Strategy Diversity
- **Sentiment**: News-driven, fundamental analysis
- **Technical**: Chart patterns, technical indicators  
- **Momentum**: Trend-following strategies
- **Mean Reversion**: Statistical arbitrage
- **AI/ML**: Machine learning hybrid approaches

## Customization

### Adding New Tickers
Edit the `TICKERS` list in `seed_mock_data.py`:
```python
TICKERS = ["AAPL", "NVDA", "TSLA", "YOUR_TICKER"]
```

### Adding New Strategies
Add to the `STRATEGIES` list:
```python
{"id": "your_strategy_v1", "name": "Your Strategy", "track": "sentiment", "type": "custom"}
```

### Adjusting Data Volume
Modify the random ranges in each seeding method to increase/decrease data volume.

## Database Schema Coverage

The seeder populates these key tables:
- `strategies`
- `prediction_runs` 
- `price_bars`
- `raw_events`
- `scored_events`
- `predictions`
- `prediction_outcomes`
- `strategy_performance`
- `consensus_signals`
- `system_loop_heartbeats`
- `promotion_events`
- `regime_performance`
- `strategy_stability`

## Verification

After seeding, you can verify the data:

```sql
-- Check strategy count
SELECT COUNT(*) FROM strategies WHERE tenant_id = 'default';

-- Check recent predictions  
SELECT COUNT(*) FROM predictions WHERE tenant_id = 'default' AND timestamp > datetime('now', '-7 days');

-- Check consensus signals
SELECT ticker, COUNT(*) FROM consensus_signals GROUP BY ticker;
```

## Troubleshooting

### Database Locked
Ensure no other processes are using the database when running the seeder.

### Missing Tables
Run database migrations first:
```bash
prisma db push
```

### Duplicate Data
The seeder uses `INSERT OR REPLACE` to avoid duplicates, but you can clear data first:
```sql
DELETE FROM predictions WHERE tenant_id = 'default';
```

## Performance

The seeder is optimized for development/testing:
- Uses batch operations where possible
- Generates realistic but not excessive data
- Completes in under 10 seconds on typical hardware

For larger datasets, consider adjusting the random ranges in each seeding method.
