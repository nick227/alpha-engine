import asyncio
import json
from pathlib import Path
from prisma import Prisma

BASE_DIR = Path(__file__).parent.parent
EXPORTS_DIR = BASE_DIR / "data" / "exports"
TOP_TEN_FILE = EXPORTS_DIR / "top_signals.json"

async def compile_top_ten():
    db = Prisma()
    await db.connect()
    try:
        top_predictions = await db.prediction.find_many(
            take=10,
            order={'confidence': 'desc'},
            include={'strategy': True}
        )
        
        EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
        
        signals = []
        for p in top_predictions:
            strat_name = p.strategy.name if p.strategy else "Unknown"
            signals.append({
                "ticker": p.ticker,
                "side": p.prediction,
                "conf": round(p.confidence, 2),
                "reason": f"{strat_name} strategy ({p.mode})"
            })
            
        TOP_TEN_FILE.write_text(json.dumps(signals, indent=2), encoding="utf-8")
        print(f"[SUCCESS] Wrote MVP json signals to {TOP_TEN_FILE}")
        
    except Exception as e:
        print(f"[ERROR] {e}")
    finally:
        await db.disconnect()

if __name__ == "__main__":
    asyncio.run(compile_top_ten())
