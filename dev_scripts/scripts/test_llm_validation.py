import asyncio
import os
import sys
import logging
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

# Ensure project root is on sys.path when running as a script
sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.trading.paper_trader import PaperTrader, TradeDirection
from app.db.repository import AlphaRepository

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("LLMTest")

async def test_llm_validation():
    load_dotenv()
    
    # Check for API Key
    if not os.getenv("OPENAI_API_KEY"):
        logger.error("OPENAI_API_KEY not found in .env")
        return

    # Mock config with LLM enabled
    config = {
        'initial_cash': 100000.0,
        'llm_validation': {
            'enabled': True,
            'min_confidence_for_llm': 0.7,
            'model': 'gpt-4o-mini',
            'timeout': 10.0
        },
        'signal_quality': {
            'min_confidence': 0.5,
            'min_consensus': 0.4
        }
    }

    # Initialize repository (using a temp one or existing)
    repo = AlphaRepository("data/alpha.db")
    
    # Initialize PaperTrader (provider=None to stay in simulation Mode)
    trader = PaperTrader(config, provider=None, repository=repo)
    
    logger.info("Processing test signal with LLM validation enabled...")
    
    # Create a high-conviction signal to trigger LLM
    signal_result = await trader.process_signal(
        ticker="AAPL",
        strategy_id="test_strat",
        direction=TradeDirection.LONG,
        confidence=0.85, # Triggers LLM
        consensus_score=0.9,
        alpha_score=0.1,
        feature_snapshot={
            "adx_14": 45,
            "rsi_14": 55,
            "volume_ratio_20": 2.5,
            "realized_vol_20": 0.02,
            "cross_asset_regime": "RISK_ON"
        },
        entry_price=220.0,
        regime="BULL_TREND"
    )
    
    logger.info("--- Signal Result ---")
    if signal_result:
        logger.info(f"Status: {signal_result['status']}")
        decision = signal_result.get('decision_path', {}).get('llm_validation', {})
        logger.info(f"LLM Qualified: {decision.get('qualified')}")
        logger.info(f"LLM Reason: {decision.get('reason')}")
        if 'metadata' in decision and 'llm_result' in decision['metadata']:
            logger.info(f"LLM Reasoning: {decision['metadata']['llm_result'].get('reasoning')}")
    
    repo.close()

if __name__ == "__main__":
    asyncio.run(test_llm_validation())
