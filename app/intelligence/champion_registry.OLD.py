
class ChampionRegistry:
    def __init__(self):
        self.sentiment = None
        self.quant = None

    def update(self, sentiment, quant):
        self.sentiment = sentiment
        self.quant = quant

    def snapshot(self):
        return {
            "sentiment": self.sentiment,
            "quant": self.quant
        }
