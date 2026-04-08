
def compute_weights(sentiment_perf, quant_perf):
    total = sentiment_perf + quant_perf
    if total == 0:
        return 0.5, 0.5

    ws = sentiment_perf / total
    wq = quant_perf / total
    return ws, wq
