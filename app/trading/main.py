"""
Paper Trading Main Entry Point

Main entry point for the paper trading system.
Integrates with Alpha Engine pipeline and provides execution interface.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
import argparse
import sys

from app.trading.config import load_config, save_default_config, validate_config, get_development_config
from app.trading.alpha_integration import PaperTradingOrchestrator
from app.engine.runner import run_pipeline
from app.core.feature_integration import FeatureIntegration
from app.core.types import RawEvent

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PaperTradingSystem:
    """Main paper trading system."""
    
    def __init__(self, config_path: Optional[str] = None):
        # Load configuration
        self.config = load_config(config_path)
        
        # Validate configuration
        errors = validate_config(self.config)
        if errors:
            logger.error("Configuration validation failed:")
            for error in errors:
                logger.error(f"  - {error}")
            sys.exit(1)
        
        # Initialize orchestrator
        self.orchestrator = PaperTradingOrchestrator({
            'paper_trader': {
                'initial_cash': self.config.initial_cash,
                'tenant_id': self.config.tenant_id,
                'base_position_pct': self.config.base_position_pct,
                'max_position_pct': self.config.max_position_pct,
                'max_ticker_exposure': self.config.max_ticker_exposure,
                'max_sector_exposure': self.config.max_sector_exposure,
                'max_strategy_exposure': self.config.max_strategy_exposure,
                'max_daily_loss_pct': self.config.max_daily_loss_pct,
                'max_correlation_exposure': self.config.max_correlation_exposure,
                'stop_loss_volatility_multiplier': self.config.stop_loss_volatility_multiplier,
                'reward_risk_ratio': self.config.reward_risk_ratio,
                'trailing_stop_enabled': self.config.trailing_stop_enabled,
                'trailing_stop_pct': self.config.trailing_stop_pct,
                'execution_delay_seconds': self.config.execution_delay_seconds,
                'slippage_bps': self.config.slippage_bps,
                'save_trade_history': self.config.save_trade_history,
                'trade_history_file': self.config.trade_history_file,
                'simulation_mode': self.config.simulation_mode,
                'dry_run': self.config.dry_run,
                'debug_mode': self.config.debug_mode
            }
        })
        
        self.feature_integration = FeatureIntegration()
        
        logger.info(f"Paper trading system initialized with ${self.config.initial_cash:,.2f}")
    
    async def run_with_pipeline_data(
        self,
        raw_events: List[RawEvent],
        price_contexts: Dict[str, Any],
        market_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Run paper trading with Alpha Engine pipeline data.
        
        Args:
            raw_events: Raw events for pipeline processing
            price_contexts: Price contexts for events
            market_data: Current market data
            
        Returns:
            Paper trading session results
        """
        logger.info(f"Processing {len(raw_events)} events through Alpha Engine pipeline")
        
        # Run Alpha Engine pipeline
        pipeline_results = run_pipeline(
            raw_events=raw_events,
            price_contexts=price_contexts,
            persist=False,  # Don't persist in paper trading mode
            evaluate_outcomes=True
        )
        
        # Extract consensus signals and predictions
        consensus_predictions = self._extract_consensus_predictions(pipeline_results)
        strategy_predictions = self._extract_strategy_predictions(pipeline_results)
        
        # Run paper trading session
        session_results = await self.orchestrator.run_paper_trading_session(
            consensus_predictions=consensus_predictions,
            predictions=strategy_predictions,
            market_data=market_data
        )
        
        # Add pipeline statistics
        session_results['pipeline_stats'] = {
            'total_events': len(raw_events),
            'total_predictions': len(strategy_predictions),
            'total_consensus_signals': len(consensus_predictions),
            'pipeline_summary': pipeline_results.get('summary', [])
        }
        
        return session_results
    
    def _extract_consensus_predictions(self, pipeline_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract consensus predictions from pipeline results."""
        consensus_predictions = []
        
        prediction_rows = pipeline_results.get('prediction_rows', [])
        
        for row in prediction_rows:
            if row.get('track') == 'consensus':
                consensus_predictions.append({
                    'ticker': row['ticker'],
                    'direction': row['prediction'],
                    'confidence': row['confidence'],
                    'regime': row.get('regime', 'UNKNOWN'),
                    'trend_strength': row.get('trend_strength', 0.5),
                    'sentiment_confidence': row.get('sentiment_confidence', 0.5),
                    'quant_confidence': row.get('quant_confidence', 0.5),
                    'weighted_consensus': row.get('weighted_consensus', row['confidence']),
                    'metadata': {
                        'strategy_name': row.get('strategy_name'),
                        'strategy_type': row.get('strategy_type'),
                        'horizon': row.get('horizon')
                    }
                })
        
        return consensus_predictions
    
    def _extract_strategy_predictions(self, pipeline_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract strategy predictions from pipeline results."""
        strategy_predictions = []
        
        prediction_rows = pipeline_results.get('prediction_rows', [])
        
        for row in prediction_rows:
            if row.get('track') != 'consensus':  # Non-consensus predictions
                strategy_predictions.append({
                    'ticker': row['ticker'],
                    'strategy_id': row['strategy_id'],
                    'prediction': row['prediction'],
                    'confidence': row['confidence'],
                    'horizon': row.get('horizon', '1d'),
                    'feature_snapshot': {
                        'regime': row.get('regime', 'UNKNOWN'),
                        'trend_strength': row.get('trend_strength', 0.5)
                    }
                })
        
        return strategy_predictions
    
    async def run_demo_session(self) -> Dict[str, Any]:
        """Run a demo paper trading session with simulated data."""
        logger.info("Running demo paper trading session")
        
        # Create sample events
        sample_events = [
            RawEvent(
                id=f"demo_event_{i}",
                timestamp=datetime.now(timezone.utc),
                source="demo",
                text=f"Sample news event {i}",
                tickers=["AAPL", "MSFT", "GOOGL"][i % 3],
                tenant_id="demo"
            )
            for i in range(5)
        ]
        
        # Create sample price contexts
        sample_price_contexts = {}
        for event in sample_events:
            sample_price_contexts[event.id] = {
                'features': {
                    'entry_price': 150.0 + hash(event.tickers[0]) % 50,
                    'volume_ratio_20': 1.2,
                    'realized_vol_20': 0.02,
                    'adx_14': 25.0,
                    'trend_strength': 'MODERATE',
                    'consensus_score': 0.7,
                    'volatility_regime': 'NORMAL'
                },
                'outcomes': {
                    'future_return_1h': 0.01,
                    'future_return_1d': 0.02
                }
            }
        
        # Create sample market data
        market_data = {
            'prices': {
                'AAPL': 175.50,
                'MSFT': 380.25,
                'GOOGL': 140.75
            },
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        # Run session
        return await self.run_with_pipeline_data(
            sample_events,
            sample_price_contexts,
            market_data
        )
    
    def get_system_status(self) -> Dict[str, Any]:
        """Get current system status."""
        portfolio = self.orchestrator.get_current_portfolio()
        
        return {
            'status': 'running',
            'config': {
                'initial_cash': self.config.initial_cash,
                'tenant_id': self.config.tenant_id,
                'min_confidence': self.config.min_confidence,
                'simulation_mode': self.config.simulation_mode
            },
            'portfolio': portfolio,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Alpha Engine Paper Trading System')
    parser.add_argument('--config', type=str, help='Configuration file path')
    parser.add_argument('--demo', action='store_true', help='Run demo session')
    parser.add_argument('--save-config', type=str, help='Save default configuration to file')
    parser.add_argument('--status', action='store_true', help='Show system status')
    
    args = parser.parse_args()
    
    # Save default configuration if requested
    if args.save_config:
        save_default_config(args.save_config)
        return
    
    # Initialize system
    try:
        system = PaperTradingSystem(args.config)
    except Exception as e:
        logger.error(f"Failed to initialize paper trading system: {e}")
        sys.exit(1)
    
    # Show status if requested
    if args.status:
        status = system.get_system_status()
        print("System Status:")
        print(f"  Status: {status['status']}")
        print(f"  Initial Cash: ${status['config']['initial_cash']:,.2f}")
        print(f"  Current Cash: ${status['portfolio']['cash']:,.2f}")
        print(f"  Total Trades: {status['portfolio']['total_trades']}")
        print(f"  Win Rate: {status['portfolio']['win_rate']:.2%}")
        return
    
    # Run demo session if requested
    if args.demo:
        try:
            results = await system.run_demo_session()
            print("\nDemo Session Results:")
            print(f"  Total Trades: {results['total_trades']}")
            print(f"  Consensus Trades: {results['consensus_trades']}")
            print(f"  Prediction Trades: {results['prediction_trades']}")
            print(f"  Portfolio Cash: ${results['portfolio_summary']['cash']:,.2f}")
            print(f"  Win Rate: {results['portfolio_summary']['win_rate']:.2%}")
            
            if results['executed_trades']:
                print("\nExecuted Trades:")
                for trade in results['executed_trades']:
                    print(f"  {trade['ticker']} {trade['direction']} @ {trade['entry_price']:.2f} "
                          f"(size: {trade['position_size']:.2f}, conf: {trade['confidence']:.2f})")
        
        except Exception as e:
            logger.error(f"Demo session failed: {e}")
            sys.exit(1)
    else:
        print("Paper trading system initialized. Use --demo to run a demo session.")
        print("Use --status to check system status.")
        print("Use --save-config <path> to save default configuration.")


if __name__ == "__main__":
    asyncio.run(main())
