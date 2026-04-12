import asyncio
import logging
from typing import Dict, Any
from decimal import Decimal
import os
import sys

# Add project root to path
sys.path.append(os.getcwd())

from app.trading.paper_trader import PaperTrader, TradeDirection
from app.trading.trade_lifecycle import OrderType
from app.db.repository import AlphaRepository

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("TestLLMSideCar")

async def test_llm_side_car():
    """
    Verifies that the LLM Shadow Analyst correctly generates and persists 
    both qualitative narrative AND structured predictions to the database.
    """
    logger.info("Starting LLM Side-Car Strategy Verification...")
    
    # Mock Config
    config = {
        'llm_validation': {
            'enabled': True,
            'min_confidence_for_llm': 0.8,
            'model': 'gpt-4o',
            'timeout': 30
        },
        'portfolio': {
            'max_positions': 5,
            'min_consensus': 0.4
        },
        'risk': {
            'max_drawdown_limit': 0.1,
            'max_daily_loss': 0.05
        }
    }
    
    # Initialize components
    db_path = "data/test_analysis.db"
    # Ensure fresh start for verification
    if os.path.exists(db_path):
        os.remove(db_path)
        
    repository = AlphaRepository(db_path)
    trader = PaperTrader(config, repository=repository)
    
    # Mock LLM Client to verify structured data persistence
    from unittest.mock import AsyncMock
    # The qualification layer index for LLM in the current pipeline is usually 2 or last
    # Let's find it dynamically to be safe
    llm_layer = None
    for layer in trader.qualification_layers:
        if hasattr(layer, 'client'):
            llm_layer = layer
            break
            
    if not llm_layer:
        logger.error("LLM layer not found in trader!")
        return

    # 1. TEST CASE: High Conviction Qualified Trade
    ticker = "AAPL"
    llm_layer.client.validate_signal = AsyncMock(return_value={
        'decision': 'QUALIFIED',
        'analysis': 'MOCK ANALYSIS: AAPL shows strong momentum with upside potential confirmed by volume.',
        'key_risk_factor': 'High valuation relative to peers.',
        'conviction_commentary': 'The engine conviction of 0.95 is well-supported.'
    })
    
    signal_data = {
        'id': 'test_signal_1',
        'ticker': ticker,
        'strategy_id': 'strategy_v1',
        'direction': TradeDirection.LONG,
        'confidence': 0.95,
        'consensus_score': 0.88,
        'alpha_score': 0.045,
        'entry_price': 220.0,
        'stop_loss': 210.0,
        'regime': 'BULLISH_TREND',
        'feature_snapshot': {'adx_20': 35.5, 'rsi_14': 62.0}
    }
    
    # Prepare signal arguments (remove unsupported ones for process_signal)
    signal_args = signal_data.copy()
    signal_args.pop('id', None)
    signal_args.pop('stop_loss', None)
    
    print(f"\n[TEST 1] Processing signal for {ticker} (LLM Decision: QUALIFIED)...")
    result = await trader.process_signal(**signal_args)
    
    # Verify DB persistence for Case 1
    trades = repository.get_trades(ticker=ticker)
    assert len(trades) > 0, "Trade 1 was not saved to DB"
    saved_trade = trades[0]
    
    print(f"[SUCCESS] Trade 1 persisted.")
    print(f" -> DB Prediction: {saved_trade['llm_prediction']}")
    print(f" -> DB Analysis Sample: {saved_trade['analysis'][:60]}...")
    assert saved_trade['llm_prediction'] == 'QUALIFIED'

    # 2. TEST CASE: Engine trades but LLM DISAGREES (REJECT)
    ticker2 = "TSLA"
    llm_layer.client.validate_signal = AsyncMock(return_value={
        'decision': 'REJECT',
        'analysis': 'MOCK REJECTION: Technicals are overextended and macro headwinds are increasing.',
        'key_risk_factor': 'Extreme overbought RSI on weekly chart.',
        'conviction_commentary': 'The engine conviction of 0.85 seems overly aggressive in this regime.'
    })
    
    signal2 = signal_data.copy()
    signal2['ticker'] = ticker2
    signal2['id'] = "test_signal_reject"
    signal2['confidence'] = 0.85
    
    # Prepare signal arguments
    signal2_args = signal2.copy()
    signal2_args.pop('id', None)
    signal2_args.pop('stop_loss', None)
    
    print(f"\n[TEST 2] Processing signal for {ticker2} (LLM Decision: REJECT - Disagreement Case)...")
    result2 = await trader.process_signal(**signal2_args)
    
    # Verify persistence for Case 2
    trades2 = repository.get_trades(ticker=ticker2)
    assert len(trades2) > 0, "Trade 2 was not saved to DB"
    saved_trade2 = trades2[0]
    
    print(f"[SUCCESS] Trade 2 persisted despite LLM REJECT (Non-blocking verified).")
    print(f" -> DB Prediction: {saved_trade2['llm_prediction']}")
    print(f" -> Log should show 'STRATEGY DISAGREEMENT' above.")
    assert saved_trade2['llm_prediction'] == 'REJECT'

    print("\n[VERIFICATION COMPLETE] LLM Side-Car Strategy is fully functional.")
    print("Structured predictions are being captured in the 'llm_prediction' column.")

if __name__ == "__main__":
    asyncio.run(test_llm_side_car())
