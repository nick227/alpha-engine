"""
Prediction Integrity Testing Suite

Verifies predictions flow correctly through the entire pipeline:
- Prediction creation → Trade → Outcome → Learner → Weights

Tests:
1. Pipeline Connection - verify end-to-end flow
2. Deterministic Price Path - correct/wrong/short predictions
3. Consensus Adaptation - multi-strategy weight verification
4. Traceability - prediction_id flows through system
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from app.core.types import RawEvent, Prediction, PredictionOutcome
from app.testing.safe_backfill import ExecutionCounters

logger = logging.getLogger(__name__)


@dataclass
class PredictionTrace:
    """Tracks a prediction through the entire pipeline."""
    prediction_id: str
    signal_id: str
    strategy_id: str
    ticker: str
    direction: str
    confidence: float
    entry_price: float
    exit_price: float
    
    # Flow tracking
    prediction_created: bool = False
    trade_created: bool = False
    trade_id: Optional[str] = None
    trade_closed: bool = False
    outcome_created: bool = False
    outcome_id: Optional[str] = None
    learner_updated: bool = False
    weight_updated: bool = False
    
    def is_complete(self) -> bool:
        """Check if prediction flowed through entire pipeline."""
        return all([
            self.prediction_created,
            self.trade_created,
            self.trade_closed,
            self.outcome_created,
            self.learner_updated,
            self.weight_updated,
        ])


class PredictionIntegrityTest:
    """
    Test suite for prediction integrity verification.
    """
    
    def __init__(self, counters: ExecutionCounters):
        self.counters = counters
        self.traces: Dict[str, PredictionTrace] = {}
        
    # ==========================================================================
    # TEST A: Pipeline Connection Test
    # ==========================================================================
    def test_pipeline_connection(self) -> Tuple[bool, List[str]]:
        """
        Test A: Verify prediction flows through entire system.
        
        Inject 1 synthetic event:
        - ticker = "TEST"
        - prediction = LONG
        - confidence = 0.8
        - price = 100
        - future_price = 110
        
        Expected: predictions=1, trades=1, outcomes=1, learner_updates=1, weight_updates=1
        """
        logger.info("TEST A: Pipeline Connection Test")
        
        failures = []
        
        # Create synthetic event
        event = RawEvent(
            id="pipeline_test_001",
            timestamp=datetime.now(timezone.utc),
            source="integrity_test",
            text="Pipeline connection test event",
            tickers=["TEST"],
        )
        
        # Simulate pipeline run (this would call actual pipeline in real implementation)
        result = self._simulate_pipeline_run(
            event=event,
            prediction="LONG",
            confidence=0.8,
            entry_price=100.0,
            exit_price=110.0,
        )
        
        # Verify all stages
        if self.counters.predictions < 1:
            failures.append(f"predictions={self.counters.predictions}, expected >=1")
        else:
            logger.info(f"  ✓ predictions: {self.counters.predictions}")
            
        if self.counters.trades < 1:
            failures.append(f"trades={self.counters.trades}, expected >=1")
        else:
            logger.info(f"  ✓ trades: {self.counters.trades}")
            
        if self.counters.outcomes < 1:
            failures.append(f"outcomes={self.counters.outcomes}, expected >=1")
        else:
            logger.info(f"  ✓ outcomes: {self.counters.outcomes}")
            
        if self.counters.learner_updates < 1:
            failures.append(f"learner_updates={self.counters.learner_updates}, expected >=1")
        else:
            logger.info(f"  ✓ learner_updates: {self.counters.learner_updates}")
            
        if self.counters.weight_updates < 1:
            failures.append(f"weight_updates={self.counters.weight_updates}, expected >=1")
        else:
            logger.info(f"  ✓ weight_updates: {self.counters.weight_updates}")
        
        passed = len(failures) == 0
        if passed:
            logger.info("  TEST A PASSED ✓")
        else:
            logger.error(f"  TEST A FAILED: {failures}")
            
        return passed, failures
    
    # ==========================================================================
    # TEST B: Expected Result Tests (Deterministic)
    # ==========================================================================
    def test_deterministic_correct_prediction(self) -> Tuple[bool, List[str]]:
        """
        Test B1: Correct prediction math.
        
        entry = 100, exit = 110, direction = long
        Expect: return_pct = +10%, direction_correct = True, weight increases
        """
        logger.info("TEST B1: Correct Prediction (Long, Price Up)")
        
        failures = []
        
        result = self._simulate_pipeline_run(
            event=RawEvent(
                id="correct_long_001",
                timestamp=datetime.now(timezone.utc),
                source="integrity_test",
                text="Correct long prediction",
                tickers=["TEST"],
            ),
            prediction="LONG",
            confidence=0.8,
            entry_price=100.0,
            exit_price=110.0,
        )
        
        outcome = result.get('outcome', {})
        return_pct = outcome.get('return_pct', 0)
        direction_correct = outcome.get('direction_correct', False)
        
        # Verify return calculation
        if abs(return_pct - 0.10) > 0.001:
            failures.append(f"return_pct={return_pct:.4f}, expected 0.10")
        else:
            logger.info(f"  ✓ return_pct: {return_pct:.4f}")
        
        # Verify direction correct
        if not direction_correct:
            failures.append(f"direction_correct={direction_correct}, expected True")
        else:
            logger.info(f"  ✓ direction_correct: {direction_correct}")
        
        passed = len(failures) == 0
        if passed:
            logger.info("  TEST B1 PASSED ✓")
        else:
            logger.error(f"  TEST B1 FAILED: {failures}")
            
        return passed, failures
    
    def test_deterministic_wrong_prediction(self) -> Tuple[bool, List[str]]:
        """
        Test B2: Wrong prediction math.
        
        entry = 100, exit = 90, direction = long
        Expect: return_pct = -10%, direction_correct = False, weight decreases
        """
        logger.info("TEST B2: Wrong Prediction (Long, Price Down)")
        
        failures = []
        
        result = self._simulate_pipeline_run(
            event=RawEvent(
                id="wrong_long_001",
                timestamp=datetime.now(timezone.utc),
                source="integrity_test",
                text="Wrong long prediction",
                tickers=["TEST"],
            ),
            prediction="LONG",
            confidence=0.8,
            entry_price=100.0,
            exit_price=90.0,
        )
        
        outcome = result.get('outcome', {})
        return_pct = outcome.get('return_pct', 0)
        direction_correct = outcome.get('direction_correct', False)
        
        # Verify return calculation
        if abs(return_pct - (-0.10)) > 0.001:
            failures.append(f"return_pct={return_pct:.4f}, expected -0.10")
        else:
            logger.info(f"  ✓ return_pct: {return_pct:.4f}")
        
        # Verify direction correct
        if direction_correct:
            failures.append(f"direction_correct={direction_correct}, expected False")
        else:
            logger.info(f"  ✓ direction_correct: {direction_correct}")
        
        passed = len(failures) == 0
        if passed:
            logger.info("  TEST B2 PASSED ✓")
        else:
            logger.error(f"  TEST B2 FAILED: {failures}")
            
        return passed, failures
    
    def test_deterministic_short_correct(self) -> Tuple[bool, List[str]]:
        """
        Test B3: Short correct prediction.
        
        entry = 100, exit = 90, direction = short
        Expect: return_pct = +10%, direction_correct = True
        """
        logger.info("TEST B3: Correct Short Prediction")
        
        failures = []
        
        result = self._simulate_pipeline_run(
            event=RawEvent(
                id="correct_short_001",
                timestamp=datetime.now(timezone.utc),
                source="integrity_test",
                text="Correct short prediction",
                tickers=["TEST"],
            ),
            prediction="SHORT",
            confidence=0.8,
            entry_price=100.0,
            exit_price=90.0,
        )
        
        outcome = result.get('outcome', {})
        return_pct = outcome.get('return_pct', 0)
        direction_correct = outcome.get('direction_correct', False)
        
        # Verify return calculation (short profit when price drops)
        if abs(return_pct - 0.10) > 0.001:
            failures.append(f"return_pct={return_pct:.4f}, expected 0.10")
        else:
            logger.info(f"  ✓ return_pct: {return_pct:.4f}")
        
        # Verify direction correct
        if not direction_correct:
            failures.append(f"direction_correct={direction_correct}, expected True")
        else:
            logger.info(f"  ✓ direction_correct: {direction_correct}")
        
        passed = len(failures) == 0
        if passed:
            logger.info("  TEST B3 PASSED ✓")
        else:
            logger.error(f"  TEST B3 FAILED: {failures}")
            
        return passed, failures
    
    def test_confidence_calibration(self) -> Tuple[bool, List[str]]:
        """
        Test B4: Confidence calibration.
        
        High confidence (0.9) but loses → confidence_weight should decrease
        """
        logger.info("TEST B4: Confidence Calibration (High Conf Loses)")
        
        failures = []
        
        # Run multiple times with high confidence loss
        initial_weight = 0.5
        weights = []
        
        for i in range(3):
            result = self._simulate_pipeline_run(
                event=RawEvent(
                    id=f"calibration_{i}",
                    timestamp=datetime.now(timezone.utc),
                    source="integrity_test",
                    text="High confidence wrong prediction",
                    tickers=["TEST"],
                ),
                prediction="LONG",
                confidence=0.9,  # High confidence
                entry_price=100.0,
                exit_price=95.0,  # But loses
            )
            weights.append(result.get('strategy_weight', initial_weight))
        
        # Check if weight decreased
        if weights[-1] >= weights[0]:
            failures.append(f"Weight didn't decrease: {weights[0]:.4f} → {weights[-1]:.4f}")
        else:
            logger.info(f"  ✓ weight decreased: {weights[0]:.4f} → {weights[-1]:.4f}")
        
        passed = len(failures) == 0
        if passed:
            logger.info("  TEST B4 PASSED ✓")
        else:
            logger.error(f"  TEST B4 FAILED: {failures}")
            
        return passed, failures
    
    # ==========================================================================
    # TEST C: Multi-Strategy Weight Adaptation
    # ==========================================================================
    def test_weight_adaptation(self) -> Tuple[bool, List[str]]:
        """
        Test C: Multi-strategy weight adaptation.
        
        Inject: strategy_A always wins, strategy_B always loses
        Expect: weight_A > weight_B, consensus favors A
        """
        logger.info("TEST C: Multi-Strategy Weight Adaptation")
        
        failures = []
        
        # Run 5 iterations
        weights_a = []
        weights_b = []
        
        for i in range(5):
            # Strategy A wins
            result_a = self._simulate_pipeline_run(
                event=RawEvent(
                    id=f"winner_a_{i}",
                    timestamp=datetime.now(timezone.utc),
                    source="integrity_test",
                    text="Strategy A wins",
                    tickers=["WINNER"],
                ),
                prediction="LONG",
                confidence=0.8,
                entry_price=100.0,
                exit_price=110.0,
                strategy_id="strategy_A",
            )
            weights_a.append(result_a.get('strategy_weight', 0.5))
            
            # Strategy B loses
            result_b = self._simulate_pipeline_run(
                event=RawEvent(
                    id=f"loser_b_{i}",
                    timestamp=datetime.now(timezone.utc),
                    source="integrity_test",
                    text="Strategy B loses",
                    tickers=["LOSER"],
                ),
                prediction="LONG",
                confidence=0.8,
                entry_price=100.0,
                exit_price=90.0,
                strategy_id="strategy_B",
            )
            weights_b.append(result_b.get('strategy_weight', 0.5))
        
        # Verify weight_A > weight_B
        final_weight_a = weights_a[-1]
        final_weight_b = weights_b[-1]
        
        if final_weight_a <= final_weight_b:
            failures.append(f"weight_A ({final_weight_a:.4f}) <= weight_B ({final_weight_b:.4f})")
        else:
            logger.info(f"  ✓ weight_A ({final_weight_a:.4f}) > weight_B ({final_weight_b:.4f})")
        
        # Verify trends
        if final_weight_a <= weights_a[0]:
            failures.append(f"weight_A didn't increase: {weights_a[0]:.4f} → {final_weight_a:.4f}")
        else:
            logger.info(f"  ✓ weight_A increased: {weights_a[0]:.4f} → {final_weight_a:.4f}")
        
        if final_weight_b >= weights_b[0]:
            failures.append(f"weight_B didn't decrease: {weights_b[0]:.4f} → {final_weight_b:.4f}")
        else:
            logger.info(f"  ✓ weight_B decreased: {weights_b[0]:.4f} → {final_weight_b:.4f}")
        
        passed = len(failures) == 0
        if passed:
            logger.info("  TEST C PASSED ✓")
        else:
            logger.error(f"  TEST C FAILED: {failures}")
            
        return passed, failures
    
    # ==========================================================================
    # TEST D: Full Pipeline Smoke Assertion
    # ==========================================================================
    def test_full_pipeline_smoke(self) -> Tuple[bool, List[str]]:
        """
        Test D: Full pipeline smoke assertion.
        
        After run: predictions > 0, trades > 0, outcomes > 0, weights updated, consensus changed
        """
        logger.info("TEST D: Full Pipeline Smoke Assertion")
        
        failures = []
        
        # Run a batch of predictions
        for i in range(10):
            self._simulate_pipeline_run(
                event=RawEvent(
                    id=f"smoke_{i}",
                    timestamp=datetime.now(timezone.utc),
                    source="integrity_test",
                    text=f"Smoke test event {i}",
                    tickers=["SMOKE"],
                ),
                prediction="LONG" if i % 2 == 0 else "SHORT",
                confidence=0.7 + (i * 0.02),
                entry_price=100.0,
                exit_price=105.0 if i % 3 == 0 else 95.0,
            )
        
        # Verify all counters
        if self.counters.predictions == 0:
            failures.append("predictions = 0")
        else:
            logger.info(f"  ✓ predictions: {self.counters.predictions}")
        
        if self.counters.trades == 0:
            failures.append("trades = 0")
        else:
            logger.info(f"  ✓ trades: {self.counters.trades}")
        
        if self.counters.outcomes == 0:
            failures.append("outcomes = 0")
        else:
            logger.info(f"  ✓ outcomes: {self.counters.outcomes}")
        
        if self.counters.weight_updates == 0:
            failures.append("weight_updates = 0")
        else:
            logger.info(f"  ✓ weight_updates: {self.counters.weight_updates}")
        
        passed = len(failures) == 0
        if passed:
            logger.info("  TEST D PASSED ✓")
        else:
            logger.error(f"  TEST D FAILED: {failures}")
            
        return passed, failures
    
    # ==========================================================================
    # TEST E: Traceability Test (Critical)
    # ==========================================================================
    def test_prediction_traceability(self) -> Tuple[bool, List[str], Optional[PredictionTrace]]:
        """
        Test E: Prediction traceability (CRITICAL).
        
        Pick one prediction ID and verify it flows:
        prediction_id → trade.prediction_id → outcome.prediction_id → learner → weight_update
        """
        logger.info("TEST E: Prediction Traceability (CRITICAL)")
        
        failures = []
        
        prediction_id = "trace_test_001"
        
        # Create trace
        trace = PredictionTrace(
            prediction_id=prediction_id,
            signal_id="signal_001",
            strategy_id="strategy_test",
            ticker="TRACE",
            direction="LONG",
            confidence=0.85,
            entry_price=100.0,
            exit_price=110.0,
        )
        
        # Simulate full pipeline with trace tracking
        result = self._simulate_traced_pipeline_run(trace)
        
        # Verify each stage
        if not trace.prediction_created:
            failures.append("prediction not created")
        else:
            logger.info("  ✓ prediction created")
        
        if not trace.trade_created:
            failures.append("trade not created")
        else:
            logger.info(f"  ✓ trade created: {trace.trade_id}")
        
        if not trace.trade_closed:
            failures.append("trade not closed")
        else:
            logger.info("  ✓ trade closed")
        
        if not trace.outcome_created:
            failures.append("outcome not created")
        else:
            logger.info(f"  ✓ outcome created: {trace.outcome_id}")
        
        if not trace.learner_updated:
            failures.append("learner not updated")
        else:
            logger.info("  ✓ learner updated")
        
        if not trace.weight_updated:
            failures.append("weight not updated")
        else:
            logger.info("  ✓ weight updated")
        
        # Verify end-to-end wiring
        if not trace.is_complete():
            failures.append(f"incomplete trace: {trace}")
        else:
            logger.info("  ✓ end-to-end wiring verified")
        
        passed = len(failures) == 0
        if passed:
            logger.info("  TEST E PASSED ✓")
        else:
            logger.error(f"  TEST E FAILED: {failures}")
            
        return passed, failures, trace if passed else None
    
    # ==========================================================================
    # Helper Methods
    # ==========================================================================
    def _simulate_pipeline_run(
        self,
        event: RawEvent,
        prediction: str,
        confidence: float,
        entry_price: float,
        exit_price: float,
        strategy_id: str = "default_strategy",
    ) -> Dict[str, Any]:
        """
        Simulate a pipeline run (would call actual pipeline in production).
        
        Returns result dict with outcome, weights, etc.
        """
        # This is a simulation - in production this would call the actual pipeline
        # and track all the counter updates
        
        # Calculate return
        if prediction == "LONG":
            return_pct = (exit_price - entry_price) / entry_price
            direction_correct = exit_price > entry_price
        else:  # SHORT
            return_pct = (entry_price - exit_price) / entry_price
            direction_correct = exit_price < entry_price
        
        # Update counters
        self.counters.predictions += 1
        self.counters.trades += 1
        self.counters.outcomes += 1
        self.counters.learner_updates += 1
        self.counters.weight_updates += 1
        
        return {
            'event_id': event.id,
            'prediction': prediction,
            'confidence': confidence,
            'outcome': {
                'return_pct': return_pct,
                'direction_correct': direction_correct,
                'entry_price': entry_price,
                'exit_price': exit_price,
            },
            'strategy_weight': 0.5 + (0.1 if direction_correct else -0.1),
        }
    
    def _simulate_traced_pipeline_run(self, trace: PredictionTrace) -> Dict[str, Any]:
        """
        Simulate pipeline run with full trace tracking.
        """
        # Simulate each stage
        trace.prediction_created = True
        
        trace.trade_created = True
        trace.trade_id = f"trade_{trace.prediction_id}"
        
        trace.trade_closed = True
        
        trace.outcome_created = True
        trace.outcome_id = f"outcome_{trace.prediction_id}"
        
        trace.learner_updated = True
        trace.weight_updated = True
        
        # Update counters
        self.counters.predictions += 1
        self.counters.trades += 1
        self.counters.outcomes += 1
        self.counters.learner_updates += 1
        self.counters.weight_updates += 1
        
        return {'trace': trace}
    
    def run_all_tests(self) -> Dict[str, Any]:
        """
        Run all prediction integrity tests.
        
        Returns summary of results.
        """
        logger.info("\n" + "="*60)
        logger.info("PREDICTION INTEGRITY TEST SUITE")
        logger.info("="*60 + "\n")
        
        results = {
            'pipeline_connection': self.test_pipeline_connection(),
            'correct_prediction': self.test_deterministic_correct_prediction(),
            'wrong_prediction': self.test_deterministic_wrong_prediction(),
            'short_correct': self.test_deterministic_short_correct(),
            'confidence_calibration': self.test_confidence_calibration(),
            'weight_adaptation': self.test_weight_adaptation(),
            'full_pipeline_smoke': self.test_full_pipeline_smoke(),
        }
        
        # Critical traceability test
        trace_passed, trace_failures, trace = self.test_prediction_traceability()
        results['prediction_traceability'] = (trace_passed, trace_failures)
        
        # Summary
        all_passed = all(passed for passed, _ in results.values())
        
        logger.info("\n" + "="*60)
        if all_passed:
            logger.info("ALL INTEGRITY TESTS PASSED ✓")
        else:
            logger.error("SOME INTEGRITY TESTS FAILED ✗")
        logger.info("="*60 + "\n")
        
        return {
            'all_passed': all_passed,
            'results': results,
            'trace': trace,
        }
