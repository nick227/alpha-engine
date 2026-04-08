
def compute_stability(live_accuracy, backtest_accuracy):
    if backtest_accuracy == 0:
        return 0
    return live_accuracy / backtest_accuracy
