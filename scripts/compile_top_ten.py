import asyncio
from pathlib import Path
from prisma import Prisma

# Define absolute paths based on standard structure
BASE_DIR = Path(__file__).parent.parent
EXPORTS_DIR = BASE_DIR / "data" / "exports"
TOP_TEN_FILE = EXPORTS_DIR / "top_ten.md"

async def compile_top_ten():
    """
    Queries the Predictions (or ConsensusSignal) table,
    sorts by signal strength, isolates the top 10,
    and formats them into the top_ten.md dynamic context file.
    """
    db = Prisma()
    await db.connect()

    try:
        # Fetch top 10 predictions based on confidence (signal strength)
        # In a real scenario, you probably filter by a specific recent un-resolved timeframe.
        top_predictions = await db.prediction.find_many(
            take=10,
            order={
                'confidence': 'desc'
            },
            include={
                'strategy': True
            }
        )

        # Ensure directory exists
        EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

        lines = ["# Dynamic Top-Ten Signals\n"]
        lines.append("*The following represents the current highest-confidence predictions available.*")
        lines.append("")

        if not top_predictions:
            lines.append("- No active high-confidence signals available at this time.")
        else:
            for i, p in enumerate(top_predictions, 1):
                strat_name = p.strategy.name if p.strategy else "Unknown"
                mode = p.mode
                lines.append(f"{i}. **{p.ticker}** | Predicted: `{p.prediction}` | Confidence: `{p.confidence:.2f}` | Strategy: {strat_name} ({mode})")

        # Compile and atomic write
        output_content = "\n".join(lines)
        TOP_TEN_FILE.write_text(output_content, encoding="utf-8")
        
        print(f"[SUCCESS] Wrote top-10 signals to {TOP_TEN_FILE}")

    except Exception as e:
        print(f"[ERROR] Failed to compile top ten: {e}")
    finally:
        await db.disconnect()

if __name__ == "__main__":
    asyncio.run(compile_top_ten())
