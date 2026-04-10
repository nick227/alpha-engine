# Changelog v2.6

## Added
- recursive architecture scaffolding
- volatility-first regime classification
- trend-strength secondary regime modifier
- regime-aware track weighting
- replay loop for prediction evaluation
- recursive optimizer mutation/tournament scaffold
- new hybrid dual-track strategy config

## Updated
- runner now supports:
  - regime snapshot
  - dynamic track weighting
  - agreement bonus
  - aggregate signal output

## Notes
- volatility is the first primary macro filter
- trend strength is additive, not primary yet
- this release keeps logic modular and overlay-safe
