# Archived Dashboard Files

## Consolidation Completed (2026-04-08)

The following dashboard files were archived to eliminate redundancy and establish a single source of truth:

### Archived Files:
- `dashboard_original.py` - Original basic dashboard
- `dashboard_architecture_settled.py` - Architecture variant with settled schema
- `dashboard_charts.py` - Chart integration variant
- `dashboard_enhanced.py` - Enhanced version with charts
- `dashboard_modern.py` - Modern UI variant
- `dashboard_polished.py` - Polished UI with performance monitoring

### Current Canonical Dashboard:
- `../dashboard.py` - Renamed from `dashboard_optimized.py`

## Reason for Consolidation:

**Problems with Multiple Dashboards:**
- Multiple sources of truth
- Inconsistent behavior between variants
- Duplicated optimization efforts
- Unclear entry point for users
- Bugs fixed in one variant but not others
- Performance fixes applied 7 times
- Scoring logic diverging between implementations
- Card schema drift risk
- Inconsistent caching strategies

**Solution:**
- Single canonical dashboard (`dashboard.py`)
- All performance optimizations preserved
- Consolidated feature set
- Clear entry point
- Easier maintenance and testing

## Migration Notes:

1. **Entry Point**: Use `streamlit run app/ui/dashboard.py`
2. **Features**: All features from optimized version preserved
3. **Performance**: Performance monitoring and optimizations maintained
4. **Dependencies**: Same dependency requirements as before

## Recovery:

If needed, individual archived files can be restored from this directory.
However, the consolidated approach should be maintained going forward.
