"""
Test Regulatory Integration

Tests SEC data collection, signal generation, and ML feature integration.
"""

import os
import sys
import logging
from datetime import datetime

# Add app to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from app.regulatory.regulatory_signals import get_regulatory_signals
from app.regulatory.regulatory_ml_features import extract_regulatory_features
from app.regulatory.regulatory_feature_tracker import get_regulatory_feature_tracker

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_sec_collection():
    """Test SEC data collection."""
    
    print("🔍 Testing SEC Data Collection")
    print("=" * 50)
    
    # Check API key
    api_key = os.getenv('SEC_API_KEY')
    if not api_key:
        print("❌ SEC_API_KEY not set")
        return False
    
    print(f"✅ API key found: {api_key[:10]}...")
    
    try:
        from app.regulatory.sec_ingest import get_sec_engine
        
        # Initialize engine
        engine = get_sec_engine(api_key)
        print("✅ SEC engine initialized")
        
        # Test collection (small sample)
        results = engine.run_collection_cycle(days_back=3)
        
        print(f"📊 Collection Results:")
        print(f"  Form 4: {results['form4']}")
        print(f"  8-K: {results['8k']}")
        print(f"  10-Q/10-K: {results['10q_10k']}")
        print(f"  Total: {results['total']}")
        
        return results['total'] > 0
        
    except Exception as e:
        print(f"❌ Collection test failed: {e}")
        return False


def test_signal_generation():
    """Test regulatory signal generation."""
    
    print("\n🎯 Testing Regulatory Signal Generation")
    print("=" * 50)
    
    try:
        # Get signals for some test symbols
        test_symbols = ['AAPL', 'MSFT', 'GOOGL']
        signals = get_regulatory_signals(test_symbols, hours_back=48)
        
        print(f"📊 Generated {len(signals)} signals for {test_symbols}")
        
        if signals:
            print("\nSample signals:")
            for i, signal in enumerate(signals[:5]):
                print(f"  {i+1}. {signal['symbol']} - {signal['event_type']}")
                print(f"     Direction: {signal['direction']}")
                print(f"     Strength: {signal['strength']:.2f}")
                print(f"     Confidence: {signal['confidence']:.2f}")
                print(f"     Description: {signal['description'][:100]}...")
                print()
        
        return len(signals) > 0
        
    except Exception as e:
        print(f"❌ Signal generation test failed: {e}")
        return False


def test_ml_feature_extraction():
    """Test regulatory ML feature extraction."""
    
    print("\n🧠 Testing Regulatory ML Feature Extraction")
    print("=" * 50)
    
    try:
        # Test with a symbol that might have regulatory data
        test_symbol = 'AAPL'
        
        features = extract_regulatory_features(test_symbol)
        
        print(f"📊 Extracted {len(features)} features for {test_symbol}")
        
        # Show key features
        key_features = [
            'regulatory_insider_buy_recent',
            'regulatory_insider_sell_recent',
            'regulatory_merger_recent',
            'regulatory_earnings_recent',
            'regulatory_signal_strength',
            'regulatory_confidence',
            'regulatory_bullish_bias',
            'regulatory_event_count'
        ]
        
        print("\nKey regulatory features:")
        for feature in key_features:
            value = features.get(feature, 0.0)
            status = "🟢" if value > 0 else "⚪"
            print(f"  {status} {feature}: {value:.3f}")
        
        return True
        
    except Exception as e:
        print(f"❌ Feature extraction test failed: {e}")
        return False


def test_feature_performance_tracking():
    """Test regulatory feature performance tracking."""
    
    print("\n📈 Testing Regulatory Feature Performance Tracking")
    print("=" * 50)
    
    try:
        tracker = get_regulatory_feature_tracker()
        
        # Analyze recent performance
        analysis = tracker.analyze_feature_performance(days_back=30)
        
        if not analysis:
            print("⚪ No performance data available")
            return False
        
        print(f"📊 Feature Performance Analysis:")
        print(f"  Total features analyzed: {analysis['summary'].get('total_features', 0)}")
        print(f"  Average performance: {analysis['summary'].get('avg_performance', 0):.3%}")
        print(f"  Best performance: {analysis['summary'].get('best_performance', 0):.3%}")
        print(f"  Worst performance: {analysis['summary'].get('worst_performance', 0):.3%}")
        
        if analysis.get('best_performers'):
            print("\n🏆 Best performing features:")
            for i, feature in enumerate(analysis['best_performers'][:3]):
                print(f"  {i+1}. {feature['feature_name']}")
                print(f"     Event type: {feature['event_type']}")
                print(f"     Performance: {feature['overall_performance']:.3%}")
                print(f"     Samples: {feature['sample_count']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Performance tracking test failed: {e}")
        return False


def test_integration_workflow():
    """Test complete integration workflow."""
    
    print("\n🔄 Testing Complete Integration Workflow")
    print("=" * 50)
    
    try:
        # Step 1: Collect data
        print("Step 1: Testing data collection...")
        collection_ok = test_sec_collection()
        
        if not collection_ok:
            print("❌ Data collection failed - stopping workflow")
            return False
        
        # Step 2: Generate signals
        print("\nStep 2: Testing signal generation...")
        signal_ok = test_signal_generation()
        
        if not signal_ok:
            print("❌ Signal generation failed - stopping workflow")
            return False
        
        # Step 3: Extract features
        print("\nStep 3: Testing feature extraction...")
        feature_ok = test_ml_feature_extraction()
        
        if not feature_ok:
            print("❌ Feature extraction failed - stopping workflow")
            return False
        
        # Step 4: Track performance
        print("\nStep 4: Testing performance tracking...")
        tracking_ok = test_feature_performance_tracking()
        
        if not tracking_ok:
            print("❌ Performance tracking failed - stopping workflow")
            return False
        
        # All steps passed
        print("\n✅ Complete integration workflow successful!")
        print("🎯 Regulatory system is ready for production")
        
        return True
        
    except Exception as e:
        print(f"❌ Integration workflow failed: {e}")
        return False


def main():
    """Main test script."""
    
    print("🧪 Regulatory Integration Test Suite")
    print("=" * 60)
    print("Testing SEC data collection, signal generation, and ML integration")
    print()
    
    # Check prerequisites
    api_key = os.getenv('SEC_API_KEY')
    if not api_key:
        print("❌ SEC_API_KEY not set")
        print("Set it with: set SEC_API_KEY=your_api_key")
        print("Get key from: https://sec-api.io/")
        return 1
    
    # Run integration tests
    success = test_integration_workflow()
    
    if success:
        print("\n🎉 All tests passed!")
        print("🚀 Regulatory system is ready for use")
        return 0
    else:
        print("\n❌ Some tests failed")
        print("🔧 Check logs for details")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
