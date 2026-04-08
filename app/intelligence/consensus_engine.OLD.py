
from app.intelligence.weight_engine import compute_weights

def consensus(sentiment, quant, sentiment_perf, quant_perf, bonus=0):
    ws, wq = compute_weights(sentiment_perf, quant_perf)

    p = ws * sentiment + wq * quant + bonus

    return {
        "p_final": p,
        "ws": ws,
        "wq": wq,
        "sentiment": sentiment,
        "quant": quant
    }
