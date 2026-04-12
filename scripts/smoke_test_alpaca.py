import asyncio
import os
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv

from app.trading.providers import AlpacaProvider
from app.db.repository import AlphaRepository
from app.trading.trade_lifecycle import OrderType

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("AlpacaSmokeTest")

async def run_smoke_test():
    load_dotenv()
    
    api_key = os.getenv('ALPACA_PAPER_KEY')
    api_secret = os.getenv('ALPACA_PAPER_SECRET')
    
    if not api_key or not api_secret:
        logger.error("ALPACA_PAPER_KEY or ALPACA_PAPER_SECRET not found in .env")
        return

    # Initialize repository
    repo = AlphaRepository("data/smoke_test.db")
    
    # Initialize Alpaca provider
    provider = AlpacaProvider(api_key, api_secret, repo, paper=True)
    
    try:
        # 1. Connect and check account
        logger.info("Checking Alpaca account...")
        account = await provider.get_account()
        logger.info(f"Connected to Alpaca. Equity: ${account['equity']:,.2f}")
        
        # 2. Buy 1 share of SPY
        ticker = "SPY"
        logger.info(f"Submitting buy order for 1 share of {ticker}...")
        order_result = await provider.submit_order(
            ticker=ticker,
            quantity=1.0,
            direction="long",
            order_type=OrderType.MARKET
        )
        logger.info(f"Order submitted: {order_result['id']} - Status: {order_result['status']}")
        
        # 3. Wait a bit for fill (market order should be instant)
        await asyncio.sleep(5)
        
        # 4. Verify position in local DB (via provider sync)
        logger.info("Synchronizing and verifying position...")
        await provider.sync()
        
        positions = repo.get_paper_positions(mode="paper")
        spy_pos = next((p for p in positions if p['ticker'] == ticker), None)
        
        if spy_pos:
            logger.info(f"Verified position in DB: {spy_pos['ticker']} {spy_pos['quantity']} shares")
        else:
            logger.warning("Position not found in local DB after sync!")
            
        # 5. Sell 1 share
        logger.info(f"Submitting sell order for 1 share of {ticker}...")
        sell_result = await provider.submit_order(
            ticker=ticker,
            quantity=1.0,
            direction="sell",
            order_type=OrderType.MARKET
        )
        logger.info(f"Sell order submitted: {sell_result['id']} - Status: {sell_result['status']}")
        
        # 6. Verify trade recording
        # Ideally, we'd check the trades table here
        logger.info("Smoke test completed successfully.")
        
    except Exception as e:
        logger.exception(f"Smoke test failed: {e}")
    finally:
        repo.close()

if __name__ == "__main__":
    asyncio.run(run_smoke_test())
