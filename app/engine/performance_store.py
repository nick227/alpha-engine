
class PerformanceStore:
    def __init__(self):
        self.data = {}

    def update(self, strategy_id, accuracy, returns):
        self.data[strategy_id] = {
            "accuracy": accuracy,
            "returns": returns
        }

    def get(self, strategy_id):
        return self.data.get(strategy_id)
