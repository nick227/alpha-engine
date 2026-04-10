# Alpha Engine POC v2.2 Quant Overlay

This overlay-safe update adds a **Quantitative Analysis Track** that runs in parallel with the existing news/media scoring system.

## New technical strategies
- `technical_rsi_reversion`
- `technical_bollinger_reversion`
- `technical_vwap_reclaim`

## How it fits
The pipeline stays the same:

```text
raw event -> scored event -> MRA -> strategies -> predictions -> outcomes -> ranking
```

News/media and technical strategies now share the same prediction and evaluation pipeline.

## Run it
```bash
pip install -r requirements.txt
python scripts/demo_run.py
streamlit run app/ui/dashboard.py
```

## Strategy configs
See:
- `experiments/strategies/technical_rsi_v1.json`
- `experiments/strategies/technical_bollinger_v1.json`
- `experiments/strategies/technical_vwap_v1.json`

## Notes
- This is still a research scaffold, not a live execution framework.
- Technical strategies use the same event stream and price context as the text-driven strategies so you can compare them directly.
