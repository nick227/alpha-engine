
import numpy as np

class RegimeManager:
    def classify(self, returns):
        vol = np.std(returns[-20:]) if len(returns) >= 20 else 0

        if vol > 0.02:
            return "HIGH_VOL"
        elif vol < 0.008:
            return "LOW_VOL"
        else:
            return "NORMAL"
