"""
Large scale A/B test for statistical significance.
"""

from adaptive_ab_test import run_ab_test, analyze_ab_results
from mass_adaptive_test import run_mass_test

def main():
    print('=== LARGE SCALE A/B TEST ===')
    
    # Build more adaptive memory first
    print('Building adaptive memory...')
    run_mass_test(db_path='data/alpha.db', samples_per_env=50, env_types=['LOW_VOL', 'HIGH_VOL'])
    
    # Run larger A/B test
    results = run_ab_test(
        db_path='data/alpha.db',
        test_days=40,
        stocks_per_day=50,
        env_types=['LOW_VOL', 'HIGH_VOL'],
    )
    
    # Analyze results
    adaptive_works = analyze_ab_results(results)
    
    print(f'\n=== FINAL VERDICT ===')
    if adaptive_works is True:
        print('>>> SYNC_ADAPTIVE PROVEN: Adaptive mode outperforms baseline!')
        print('>>> Environment-aware config selection creates real value')
    elif adaptive_works is False:
        print('>>> SYNC_ADAPTIVE FAILS: Adaptive mode worse than baseline')
        print('>>> Environment awareness is hurting performance')
    else:
        print('>>> SYNC_ADAPTIVE UNCLEAR: Results inconclusive')
        print('>>> This suggests environment dimension may need refinement')
        print('>>> Consider: better volatility segmentation, trend classification, or additional dimensions')

if __name__ == "__main__":
    main()
