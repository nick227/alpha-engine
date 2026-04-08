
import copy
import random

class MutationEngine:
    def mutate(self, strategy):
        child = copy.deepcopy(strategy)

        if "threshold" in child:
            child["threshold"] += random.uniform(-0.05, 0.05)

        if "hold" in child:
            child["hold"] += random.randint(-3, 3)

        child["version"] = child.get("version", 0) + 1
        child["parent_id"] = strategy.get("id")

        return child
