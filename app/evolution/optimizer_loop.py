
from app.evolution.mutation_engine import MutationEngine
from app.evolution.tournament import tournament
from app.evolution.reaper import should_kill

class OptimizerLoop:

    def __init__(self):
        self.mutator = MutationEngine()

    def run(self, active_strategy):
        candidates = []

        for _ in range(5):
            child = self.mutator.mutate(active_strategy)
            candidates.append(child)

        winner = tournament(candidates)

        if should_kill(winner):
            return None

        return winner
