import asyncio
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

from app.trading.providers import AlpacaProvider
from app.db.repository import AlphaRepository
from app.trading.trade_lifecycle import OrderType

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("DollarTrade")

async def run_dollar_trade():
    load_dotenv()
    
    # Use keys from .env (matching the names found in the file)
    api_key = os.getenv('ALPACA_API_KEY')
    api_secret = os.getenv('ALPACA_API_SECRET')
    paper = os.getenv('ALPACA_PAPER', 'true').lower() == 'true'
    
    if not api_key or not api_secret:
        logger.error("ALPACA_API_KEY or ALPACA_API_SECRET not found in .env")
        return

    # Initialize repository (temporary for this script)
    repo = AlphaRepository("data/alpha.db")
    
    # Initialize Alpaca provider
    provider = AlpacaProvider(api_key, api_secret, repo, paper=paper)
    
    try:
        # 1. Connect and check account
        logger.info(f"Connecting to Alpaca (Paper={paper})...")
        account = await provider.get_account()
        logger.info(f"Account Balance: ${account['equity']:,.2f}")
        
        ticker = "SPY"
        amount = 1.0  # $1.00
        
        # 2. Buy $1 of SPY
        logger.info(f"Buying ${amount} of {ticker}...")
        buy_order = await provider.submit_order(
            ticker=ticker,
            notional=amount,
            direction="buy",
            order_type=OrderType.MARKET
        )
        logger.info(f"Buy order submitted: {buy_order['id']} - Status: {buy_order['status']}")
        
        # 3. Wait for execution (Market orders are usually fast but notional might take a second)
        logger.info("Waiting for fill (up to 30s)...")
        filled = False
        for _ in range(30):
            # We don't have a direct 'get_order' in provider yet, but we can check positions
            await provider.sync()
            positions = repo.get_paper_positions(mode="paper")
            if any(p['ticker'] == ticker and p['quantity'] > 0 for p in positions):
                logger.info(f"Verified {ticker} position filled!")
                filled = True
                break
            await asyncio.sleep(1)
        
        if not filled:
            logger.warning("Order not filled yet (Market may be closed). Continuing with sell attempt...")

        # 4. Sell $1 of SPY
        logger.info(f"Selling ${amount} of {ticker}...")
        try:
            sell_order = await provider.submit_order(
                ticker=ticker,
                notional=amount,
                direction="sell",
                order_type=OrderType.MARKET
            )
            logger.info(f"Sell order submitted: {sell_order['id']} - Status: {sell_order['status']}")
        except Exception as e:
            if "potential wash trade" in str(e):
                logger.warning("Alpaca blocked the sell as a potential wash trade (Buy is likely still pending).")
                logger.info("This is expected behavior when fractional orders are queued outside market hours.")
            else:
                raise
        
        # 5. Final sync
        await provider.sync()
        logger.info("Dollar buy/sell cycle completed.")
        
    except Exception as e:
        logger.error(f"Trade failed: {e}")
    finally:
        repo.close()

if __name__ == "__main__":
    asyncio.run(run_dollar_trade())
