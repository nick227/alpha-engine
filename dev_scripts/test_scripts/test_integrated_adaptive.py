"""
Test integrated industry-adaptive discovery pipeline.
"""

from app.discovery.runner import run_discovery
from app.db.repository import AlphaRepository

def test_integrated_adaptive():
    """Test the full industry-adaptive pipeline."""
    
    print("=== INTEGRATED INDUSTRY-ADAPTIVE TEST ===")
    
    # Test with industry-aware discovery
    summary = run_discovery(
        db_path="data/alpha.db",
        tenant_id="test",
        as_of="2024-01-01",
        top_n=10,
        min_avg_dollar_volume_20d=1000000,
    )
    
    print("Industry-adaptive discovery completed!")
    print(f"Environment: {summary['environment']['env_bucket']}")
    print(f"Sector regime: {summary['environment']['sector_regime']}")
    
    for strategy, results in summary['strategies'].items():
        print(f"{strategy}:")
        print(f"  Industry filtered stocks: {results['industry_filtered_stocks']}")
        print(f"  Selected config: {results['selected_config']}")
        print(f"  Top candidates: {len(results['top'])}")

if __name__ == "__main__":
    test_integrated_adaptive()
