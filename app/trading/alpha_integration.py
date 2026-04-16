"""
Alpha Engine Integration

Integrates paper trading system with Alpha Engine pipeline.
Connects consensus signals to qualification pipeline and execution.
"""

from __future__ import annotations
from datetime import datetime, timezone, time
from typing import Dict, List, Any, Optional, Tuple
import asyncio
import logging
from zoneinfo import ZoneInfo

from app.trading.paper_trader import PaperTrader, TradeDirection
from app.core.feature_integration import FeatureIntegration
from app.engine.consensus_engine import ConsensusPrediction
from app.core.types import Prediction
from app.db.repository import AlphaRepository
from app.trading.base import TradingProvider
from app.core.regime_v3 import RegimeClassifierV3, SignalGating, QualityScoreV3, PositionSizerV3
from app.trading.position_sizing_v3 import EnhancedPositionSizer, RegimeAwarePortfolioManager

logger = logging.getLogger(__name__)


class AlphaEngineIntegration:
    """
    Integration layer between Alpha Engine and Paper Trading System.
    
    Responsibilities:
    - Convert Alpha Engine predictions to trade signals
    - Apply qualification pipeline
    - Execute paper trades
    - Track performance
    """
    
    def __init__(self, paper_trader: PaperTrader, feature_integration: FeatureIntegration):
        self.paper_trader = paper_trader
        self.feature_integration = feature_integration
        self.signal_history: List[Dict[str, Any]] = []
        
        # Initialize regime classifier
        self.regime_classifier = RegimeClassifierV3()
        
        # Initialize enhanced position sizer
        self.position_sizer = EnhancedPositionSizer()
        self.portfolio_manager = RegimeAwarePortfolioManager(self.position_sizer)
        
        # Track regime statistics
        self.regime_stats = {
            'total_signals': 0,
            'gated_signals': 0,
            'regime_distribution': {}
        }
        
    async def process_consensus_signals(
        self,
        consensus_predictions: List[ConsensusPrediction],
        current_prices: Dict[str, float]
    ) -> List[Dict[str, Any]]:
        """
        Process consensus signals from Alpha Engine.
        
        Args:
            consensus_predictions: List of consensus predictions
            current_prices: Current market prices for tickers
            
        Returns:
            List of executed paper trades
        """
        executed_trades = []
        
        for consensus in consensus_predictions:
            try:
                # Convert consensus to trade signal
                trade_signal = self._convert_consensus_to_signal(consensus, current_prices)
                
                if not trade_signal:
                    continue
                
                # Apply regime gating
                gated_signal = self._apply_regime_gating(trade_signal, consensus)
                
                if not gated_signal:
                    self.regime_stats['gated_signals'] += 1
                    continue
                
                # Calculate enhanced quality score
                quality_score = self._calculate_quality_score(gated_signal, consensus)
                gated_signal['quality_score'] = quality_score
                
                # Process through paper trader
                trade_event = await self.paper_trader.process_signal(**gated_signal)
                
                if trade_event and trade_event.get('status') in ['EXECUTED', 'executed', 'filled']:
                    executed_trades.append(trade_event)
                    self._record_signal(consensus, trade_event)
                
            except Exception as e:
                logger.error(f"Error processing consensus signal for {consensus.ticker}: {e}")
                continue
        
        return executed_trades
    
    def _convert_consensus_to_signal(
        self,
        consensus: ConsensusPrediction,
        current_prices: Dict[str, float]
    ) -> Optional[Dict[str, Any]]:
        """
        Convert consensus prediction to trade signal.
        """
        ticker = consensus.ticker
        
        # Get current price
        if ticker not in current_prices:
            logger.warning(f"No current price available for {ticker}")
            return None
        
        current_price = current_prices[ticker]
        
        # Convert consensus direction to trade direction
        if consensus.direction.lower() in ['up', 'long']:
            direction = TradeDirection.LONG
        elif consensus.direction.lower() in ['down', 'short']:
            direction = TradeDirection.SHORT
        else:
            logger.info(f"Neutral signal for {ticker}, skipping")
            return None
        
        # Extract features from consensus metadata
        feature_snapshot = self._extract_features_from_consensus(consensus)
        
        # Determine strategy ID from consensus
        strategy_id = self._determine_strategy_id(consensus)
        
        return {
            'ticker': ticker,
            'strategy_id': strategy_id,
            'direction': direction,
            'confidence': consensus.confidence,
            'consensus_score': consensus.weighted_consensus,
            'alpha_score': consensus.confidence,  # Use confidence as alpha score
            'feature_snapshot': feature_snapshot,
            'entry_price': current_price,
            'regime': consensus.regime.get('volatility_regime', 'UNKNOWN')
        }
    
    def _extract_features_from_consensus(self, consensus: ConsensusPrediction) -> Dict[str, Any]:
        """Extract feature snapshot from consensus prediction."""
        features = {}
        
        # Basic features from consensus
        features['consensus_direction'] = consensus.direction
        features['sentiment_confidence'] = consensus.sentiment_confidence
        features['quant_confidence'] = consensus.quant_confidence
        features['weighted_consensus'] = consensus.weighted_consensus
        
        # Regime information
        if consensus.regime:
            features.update({
                'volatility_regime': consensus.regime.get('volatility_regime'),
                'volatility_value': consensus.regime.get('volatility_value'),
                'trend_strength': consensus.regime.get('trend_strength'),
                'adx_value': consensus.regime.get('adx_value')
            })
        
        # Metadata features
        if consensus.metadata:
            features.update({
                'same_direction': consensus.metadata.get('same_direction', False),
                'agreement_bonus': consensus.metadata.get('agreement_bonus', 0.0),
                'sentiment_stability': consensus.metadata.get('sentiment_stability', 0.0),
                'quant_stability': consensus.metadata.get('quant_stability', 0.0)
            })
        
        return features
    
    def _determine_strategy_id(self, consensus: ConsensusPrediction) -> str:
        """Determine strategy ID from consensus prediction."""
        # Use strategy IDs from metadata if available
        metadata = consensus.metadata or {}
        
        sentiment_strategy = metadata.get('sentiment_track', {}).get('strategy_id')
        quant_strategy = metadata.get('quant_track', {}).get('strategy_id')
        
        if sentiment_strategy and quant_strategy:
            return f"consensus_{sentiment_strategy}_{quant_strategy}"
        elif sentiment_strategy:
            return f"consensus_sentiment_{sentiment_strategy}"
        elif quant_strategy:
            return f"consensus_quant_{quant_strategy}"
        else:
            return "consensus_default"
    
    def _apply_regime_gating(
        self, 
        trade_signal: Dict[str, Any], 
        consensus: ConsensusPrediction
    ) -> Optional[Dict[str, Any]]:
        """Apply regime gating to trade signal"""
        
        # Extract regime information from consensus
        regime_data = consensus.regime or {}
        
        # Create regime classification from existing data
        # This is a simplified version - in production, you'd calculate this from market data
        volatility_regime = regime_data.get('volatility_regime', 'NORMAL')
        trend_regime = regime_data.get('trend_strength', 'UNKNOWN')
        
        # Map to new regime classification
        from app.core.regime_v3 import RegimeClassification, TrendRegime, VolatilityRegime
        
        # Convert legacy regime to new classification
        if trend_regime == 'STRONG':
            trend = TrendRegime.BULL  # Assume bull for strong trends
        elif trend_regime == 'WEAK':
            trend = TrendRegime.CHOP
        else:
            trend = TrendRegime.CHOP
        
        if volatility_regime == 'HIGH':
            vol = VolatilityRegime.EXPANSION
        elif volatility_regime == 'LOW':
            vol = VolatilityRegime.COMPRESSION
        else:
            vol = VolatilityRegime.NORMAL
        
        regime = RegimeClassification(
            trend_regime=trend,
            volatility_regime=vol,
            combined_regime=f"({trend.value}, {vol.value})",
            price_vs_ma50=0.0,
            ma50_vs_ma200=0.0,
            atr_percentile=0.5,
            volatility_value=regime_data.get('volatility_value', 0.02),
            adx_value=regime_data.get('adx_value')
        )
        
        # Determine strategy type from signal
        strategy_type = self._extract_strategy_type(trade_signal, consensus)
        
        # Apply gating
        allowed, reason = SignalGating.gate_signal(strategy_type, regime)
        
        # Update statistics
        self.regime_stats['total_signals'] += 1
        regime_key = regime.combined_regime
        self.regime_stats['regime_distribution'][regime_key] = \
            self.regime_stats['regime_distribution'].get(regime_key, 0) + 1
        
        if not allowed:
            logger.info(f"Signal {consensus.ticker} gated: {strategy_type} not allowed in {regime.combined_regime} ({reason})")
            return None
        
        # Add regime information to signal
        trade_signal['regime_classification'] = regime
        trade_signal['strategy_type'] = strategy_type
        trade_signal['gating_reason'] = reason
        
        return trade_signal
    
    def _extract_strategy_type(self, trade_signal: Dict[str, Any], consensus: ConsensusPrediction) -> str:
        """Extract strategy type from signal"""
        
        # Try to get from strategy_id
        strategy_id = trade_signal.get('strategy_id', '').lower()
        
        if 'breakout' in strategy_id or 'volatility' in strategy_id:
            return 'volatility_breakout'
        elif 'momentum' in strategy_id or 'trend' in strategy_id:
            return 'momentum'
        elif 'reversal' in strategy_id or 'mean_reversion' in strategy_id:
            return 'mean_reversion'
        
        # Try to infer from consensus metadata
        metadata = consensus.metadata or {}
        
        # Check for volatility breakout patterns
        if metadata.get('volatility_breakout'):
            return 'volatility_breakout'
        
        # Default to momentum for consensus signals
        return 'momentum'
    
    def _calculate_quality_score(
        self, 
        trade_signal: Dict[str, Any], 
        consensus: ConsensusPrediction
    ) -> float:
        """Calculate enhanced quality score"""
        
        # Extract components
        signal_strength = trade_signal.get('confidence', 0.5)
        regime = trade_signal.get('regime_classification')
        strategy_type = trade_signal.get('strategy_type', 'momentum')
        
        # Calculate agreement score
        metadata = consensus.metadata or {}
        agreement_score = 0.5  # Default
        
        if metadata.get('same_direction'):
            agreement_score = 0.75
        if metadata.get('agreement_bonus', 0) > 0.05:
            agreement_score = 1.0
        
        # Calculate liquidity confidence
        liquidity_confidence = 0.5  # Default - would calculate from market data
        
        # Calculate quality score
        if regime:
            quality = QualityScoreV3.calculate_quality_score(
                signal_strength=signal_strength,
                regime=regime,
                strategy_type=strategy_type,
                agreement_score=agreement_score,
                liquidity_confidence=liquidity_confidence
            )
        else:
            # Fallback to simple confidence-based score
            quality = signal_strength
        
        return quality
    
    async def process_predictions(
        self,
        predictions: List[Prediction],
        current_prices: Dict[str, float]
    ) -> List[Dict[str, Any]]:
        """
        Process individual strategy predictions.
        
        Args:
            predictions: List of strategy predictions
            current_prices: Current market prices
            
        Returns:
            List of executed paper trades
        """
        executed_trades = []
        
        # Group predictions by ticker and horizon
        ticker_predictions = self._group_predictions_by_ticker(predictions)
        
        for ticker, pred_list in ticker_predictions.items():
            if ticker not in current_prices:
                continue
            
            # Select best prediction per ticker
            best_prediction = self._select_best_prediction(pred_list)
            
            if not best_prediction:
                continue
            
            # Convert to trade signal
            trade_signal = self._convert_prediction_to_signal(best_prediction, current_prices[ticker])
            
            if not trade_signal:
                continue
            
            # Process through paper trader
            trade_event = await self.paper_trader.process_signal(**trade_signal)
            
            if trade_event and trade_event.get('status', {}).get('value') in ['EXECUTED', 'executed', 'filled']:
                executed_trades.append(trade_event)
                self._record_prediction(best_prediction, trade_event)
        
        return executed_trades
    
    def _group_predictions_by_ticker(self, predictions: List[Prediction]) -> Dict[str, List[Prediction]]:
        """Group predictions by ticker."""
        ticker_groups = {}
        
        for pred in predictions:
            ticker = pred.ticker
            if ticker not in ticker_groups:
                ticker_groups[ticker] = []
            ticker_groups[ticker].append(pred)
        
        return ticker_groups
    
    def _select_best_prediction(self, predictions: List[Prediction]) -> Optional[Prediction]:
        """Select best prediction from list."""
        if not predictions:
            return None
        
        # Sort by confidence and select highest
        return max(predictions, key=lambda p: p.confidence)
    
    def _record_prediction(
    self,
    prediction: Prediction,
    trade_event: Dict[str, Any]
    ) -> None:
        """Record prediction signal for tracking."""
        self.signal_history.append({
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'type': 'prediction',
            'ticker': prediction.ticker,
            'direction': prediction.prediction,
            'confidence': prediction.confidence,
            'horizon': prediction.horizon,
            'strategy_id': prediction.strategy_id,
            'signal_id': trade_event.get('id'),
            'execution_price': trade_event.get('execution_price'),
            'position_size': trade_event.get('position_size', 0)
        })
    
    def _convert_prediction_to_signal(self, prediction: Prediction, current_price: float) -> Optional[Dict[str, Any]]:
        """Convert prediction to trade signal."""
        # Convert prediction direction
        if prediction.prediction.lower() in ['up', 'long']:
            direction = TradeDirection.LONG
        elif prediction.prediction.lower() in ['down', 'short']:
            direction = TradeDirection.SHORT
        else:
            return None
        
        return {
            'ticker': prediction.ticker,
            'strategy_id': prediction.strategy_id,
            'direction': direction,
            'confidence': prediction.confidence,
            'consensus_score': prediction.confidence,  # Use confidence as consensus
            'alpha_score': prediction.confidence,
            'feature_snapshot': prediction.feature_snapshot or {},
            'entry_price': current_price,
            'regime': prediction.feature_snapshot.get('regime', 'UNKNOWN') if prediction.feature_snapshot else 'UNKNOWN'
        }
    
    def _record_signal(
    self,
    consensus: ConsensusPrediction,
    trade_event: Dict[str, Any]
    ) -> None:
        """Record signal for tracking."""
        self.signal_history.append({
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'type': 'consensus',
            'ticker': consensus.ticker,
            'direction': consensus.direction,
            'confidence': consensus.confidence,
            'horizon': getattr(consensus, "horizon", None),
            'signal_id': trade_event.get('id'),
            'execution_price': trade_event.get('execution_price'),
            'position_size': trade_event.get('position_size', 0)
        })
    
    def get_signal_statistics(self) -> Dict[str, Any]:
        """Get signal processing statistics."""
        if not self.signal_history:
            return {'total_signals': 0}
        
        total_signals = len(self.signal_history)
        consensus_signals = len([s for s in self.signal_history if s['type'] == 'consensus'])
        prediction_signals = len([s for s in self.signal_history if s['type'] == 'prediction'])
        
        # Direction breakdown
        long_signals = len([s for s in self.signal_history if s['direction'].lower() in ['up', 'long']])
        short_signals = len([s for s in self.signal_history if s['direction'].lower() in ['down', 'short']])
        
        # Average confidence
        avg_confidence = sum(s['confidence'] for s in self.signal_history) / total_signals
        
        return {
            'total_signals': total_signals,
            'consensus_signals': consensus_signals,
            'prediction_signals': prediction_signals,
            'long_signals': long_signals,
            'short_signals': short_signals,
            'average_confidence': avg_confidence,
            'signal_success_rate': len([s for s in self.signal_history if 'trade_id' in s]) / total_signals
        }


class PaperTradingOrchestrator:
    """
    Orchestrates the complete paper trading workflow.
    
    Connects Alpha Engine pipeline with paper trading execution.
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        
        # Initialize Repository
        self.repository = AlphaRepository(config.get('db_path', 'data/alpha.db'))
        
        # Initialize Trading Provider
        self.provider = self._initialize_provider()
        
        # Initialize components
        self.feature_integration = FeatureIntegration()
        self.paper_trader = PaperTrader(config.get('paper_trader', {}), self.provider, self.repository)
        self.alpha_integration = AlphaEngineIntegration(self.paper_trader, self.feature_integration)
        
        # Market data simulation
        self.current_prices: Dict[str, float] = {}
        
        logger.info(f"Paper Trading Orchestrator initialized with provider: {self.provider.name}")

    def _initialize_provider(self) -> TradingProvider:
        """Initialize the trading provider based on configuration."""
        import os
        from app.trading.providers import AlpacaProvider, LocalSimProvider
        
        provider_type = self.config.get('trading', {}).get('provider', 'local_sim')
        
        if provider_type == 'alpaca_paper':
            api_key = os.getenv('ALPACA_PAPER_KEY') or self.config.get('alpaca', {}).get('key')
            api_secret = os.getenv('ALPACA_PAPER_SECRET') or self.config.get('alpaca', {}).get('secret')
            
            if not api_key or not api_secret:
                logger.warning("Alpaca credentials missing, falling back to local_sim")
                return LocalSimProvider(self.config.get('paper_trader', {}).get('initial_cash', 100000.0))
                
            return AlpacaProvider(api_key, api_secret, self.repository, paper=True)
        else:
            return LocalSimProvider(self.config.get('paper_trader', {}).get('initial_cash', 100000.0))
    
    async def run_paper_trading_session(
        self,
        consensus_predictions: List[ConsensusPrediction],
        predictions: List[Prediction],
        market_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Run a complete paper trading session.
        
        Args:
            consensus_predictions: Consensus signals from Alpha Engine
            predictions: Individual strategy predictions
            market_data: Current market data
            
        Returns:
            Session results and statistics
        """
        session_start = datetime.now(timezone.utc)

        # Explicit daily P&L reset boundary (paper trading is session-driven today).
        # Reset once per trading day after market open (9:30am America/Chicago).
        try:
            now_utc = session_start
            if market_data and market_data.get("timestamp"):
                now_utc = datetime.fromisoformat(market_data["timestamp"].replace("Z", "+00:00"))

            central = ZoneInfo("America/Chicago")
            now_central = now_utc.astimezone(central)
            market_open = datetime.combine(now_central.date(), time(hour=9, minute=30), tzinfo=central)

            last_reset = self.paper_trader.portfolio.daily_pnl_last_reset
            last_reset_date = last_reset.astimezone(central).date() if last_reset else None

            if now_central >= market_open and last_reset_date != now_central.date():
                self.paper_trader.reset_daily_pnl(reset_timestamp=now_utc)
        except Exception as e:
            logger.warning(f"Failed to evaluate daily P&L reset boundary: {e}")
        
        # Update market data
        if market_data:
            self._update_market_data(market_data)
        
        # Process consensus signals
        consensus_trades = await self.alpha_integration.process_consensus_signals(
            consensus_predictions, self.current_prices
        )
        
        # Process individual predictions
        prediction_trades = await self.alpha_integration.process_predictions(
            predictions, self.current_prices
        )
        
        # Synchronize provider state (reconciliation)
        await self.provider.sync()
        
        # Compile session results
        session_results = {
            'session_id': session_start.strftime('%Y%m%d_%H%M%S'),
            'start_time': session_start.isoformat(),
            'end_time': datetime.now(timezone.utc).isoformat(),
            'consensus_trades': len(consensus_trades),
            'prediction_trades': len(prediction_trades),
            'total_trades': len(consensus_trades) + len(prediction_trades),
            'portfolio_summary': self.paper_trader.get_portfolio_summary(),
            'signal_statistics': self.alpha_integration.get_signal_statistics(),
            'executed_trades': [
                {
                    'id': trade.get('id'),
                    'ticker': trade.get('ticker'),
                    'direction': trade.get('direction'),
                    'entry_price': trade.get('entry_price'),
                    'position_size': trade.get('position_size'),
                    'confidence': trade.get('confidence'),
                    'strategy_id': trade.get('strategy_id')
                }
                for trade in consensus_trades + prediction_trades
            ]
        }
        
        logger.info(f"Paper trading session completed: {session_results['total_trades']} trades executed")
        
        return session_results
    
    def _update_market_data(self, market_data: Dict[str, Any]) -> None:
        """Update current market prices."""
        # Extract prices from market data
        if 'prices' in market_data:
            self.current_prices.update(market_data['prices'])
        
        # Could also update from market data adapters here
        # For now, use simulated prices
    
    def get_current_portfolio(self) -> Dict[str, Any]:
        """Get current portfolio state."""
        return self.paper_trader.get_portfolio_summary()
    
    def get_trade_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent trade history."""
        return self.paper_trader.get_trade_history(limit)
