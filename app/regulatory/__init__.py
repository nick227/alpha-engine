"""
Regulatory Data Integration Package

Provides SEC EDGAR data integration for alpha engine.
"""

from .sec_ingest import (
    RegulatoryEvent,
    SECIngestionEngine,
    get_sec_engine,
    run_sec_collection
)

from .regulatory_signals import (
    RegulatorySignalGenerator,
    get_regulatory_signals
)

from .regulatory_ml_features import (
    RegulatoryMLFeatures,
    RegulatoryFeatureTracker,
    extract_regulatory_features,
    get_regulatory_ml_features,
    get_regulatory_feature_tracker
)

__all__ = [
    # Data structures
    'RegulatoryEvent',
    
    # Ingestion
    'SECIngestionEngine',
    'get_sec_engine',
    'run_sec_collection',
    
    # Signal generation
    'RegulatorySignalGenerator',
    'get_regulatory_signals',
    
    # ML features
    'RegulatoryMLFeatures',
    'RegulatoryFeatureTracker',
    'extract_regulatory_features',
    'get_regulatory_ml_features',
    'get_regulatory_feature_tracker'
]
