import asyncio
import os
from app.ingest.backfill_runner import BackfillRunner

async def main():
    # Use a test DB
    db_path = "data/backfill_test.db"
    if os.path.exists(db_path):
        os.remove(db_path)
        
    runner = BackfillRunner(db_path=db_path)
    # Run a small 3-day backfill for testing
    print("--- FIRST RUN (Should execute) ---")
    await runner.run_backfill(days=3)
    
    print("\n--- SECOND RUN (Should skip) ---")
    await runner.run_backfill(days=3)
    
    print("\nBackfill test run successful.")

if __name__ == "__main__":
    asyncio.run(main())
