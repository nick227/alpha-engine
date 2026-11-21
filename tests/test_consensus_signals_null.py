import unittest
import sqlite3
from app.internal_read_v1.intelligence_read import get_consensus_signals

class TestConsensusSignalsNull(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("""
            CREATE TABLE consensus_signals (
                id TEXT PRIMARY KEY,
                tenant_id TEXT,
                ticker TEXT,
                regime TEXT,
                sentiment_strategy_id TEXT,
                quant_strategy_id TEXT,
                sentiment_score REAL,
                quant_score REAL,
                ws REAL,
                wq REAL,
                agreement_bonus REAL,
                p_final REAL,
                stability_score REAL,
                created_at TEXT
            )
        """)

    def tearDown(self):
        self.conn.close()

    def test_handles_null_sentiment_score(self):
        # Insert row with NULL sentiment_score, quant_strategy_id, etc.
        self.conn.execute("""
            INSERT INTO consensus_signals (
                id, tenant_id, ticker, regime, sentiment_strategy_id, quant_strategy_id,
                sentiment_score, quant_score, ws, wq, agreement_bonus, p_final, stability_score, created_at
            ) VALUES (
                'cs_1', 'default', 'AAPL', 'NORMAL', 'sent_1', NULL,
                NULL, 0.85, 0.5, 0.5, 0.1, 0.75, 0.9, '2026-11-20 12:00:00'
            )
        """)
        self.conn.commit()

        result = get_consensus_signals(
            self.conn,
            tenant_id="default",
            limit=10,
            min_p_final=None,
            ticker="AAPL"
        )

        self.assertEqual(result["count"], 1)
        signal = result["signals"][0]
        self.assertEqual(signal["ticker"], "AAPL")
        self.assertIsNone(signal["sentimentScore"])
        self.assertIsNone(signal["quantStrategyId"])
        self.assertEqual(signal["quantScore"], 0.85)
        self.assertEqual(signal["pFinal"], 0.75)

    def test_handles_empty_signals(self):
        result = get_consensus_signals(
            self.conn,
            tenant_id="default",
            limit=10,
            min_p_final=None,
            ticker="NONEXISTENT"
        )
        self.assertEqual(result["count"], 0)
        self.assertEqual(result["signals"], [])

if __name__ == "__main__":
    unittest.main()
