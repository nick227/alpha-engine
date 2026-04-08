from __future__ import annotations
import uuid
import random
from typing import Dict, List, Tuple
from app.engine.strategy_registry import StrategyRegistry, StrategyGenome
from app.engine.continuous_learning import StrategyPerformance

class GeneticOptimizer:
    """
    Produces new StrategyGenome challengers utilizing fitness-driven breeding.
    Guarantees it never mutates or overrides an active Champion strategy directly.
    """
    def __init__(self, registry: StrategyRegistry, *, rng: random.Random | None = None):
        self.registry = registry
        self._rng = rng or random.Random()

    def _compute_fitness(self, perf: StrategyPerformance) -> float:
        avg_regime = 0.0
        if perf.regime_strength:
            avg_regime = sum(perf.regime_strength.values()) / len(perf.regime_strength)
            
        fitness = (
            (0.30 * perf.win_rate) +
            (0.30 * perf.alpha) +
            (0.20 * perf.stability) +
            (0.20 * avg_regime)
        )
        return fitness

    def run_generation(self, candidate_perfs: Dict[str, StrategyPerformance], current_generation: int, population_size: int = 10):
        """
        Takes the performance results of the previous generation of candidates,
        ranks them by fitness, and spawns the next batch of challengers.
        """
        # Rank by fitness
        ranked: List[Tuple[float, StrategyPerformance]] = []
        for s_id, perf in candidate_perfs.items():
            f = self._compute_fitness(perf)
            ranked.append((f, perf))
            
        ranked.sort(key=lambda x: x[0], reverse=True)
        
        # Select parents (Top 30%)
        num_parents = max(2, int(len(ranked) * 0.3))
        parents = [p[1] for p in ranked[:num_parents]]
        
        new_genomes = []
        
        # 1. Crossover / Mutation
        while len(new_genomes) < (population_size - 2): # leave room for random injection
            if len(parents) < 2:
                break
                
            p1_perf, p2_perf = self._rng.sample(parents, 2)
            p1_genome = self.registry.get_genome(p1_perf.strategy_id)
            p2_genome = self.registry.get_genome(p2_perf.strategy_id)
            
            if not p1_genome or not p2_genome:
                continue

            # Crossover params
            child_params = {}
            for k in p1_genome.params.keys():
                if self._rng.random() > 0.5:
                    child_params[k] = p1_genome.params[k]
                elif k in p2_genome.params:
                    child_params[k] = p2_genome.params[k]
                    
            # Mutate params (10% chance)
            for k, v in child_params.items():
                if self._rng.random() < 0.10:
                    if isinstance(v, float):
                        child_params[k] *= self._rng.uniform(0.8, 1.2) # small parameter drift
                    elif isinstance(v, int):
                        child_params[k] += self._rng.choice([-1, 1])

            kid = uuid.UUID(int=self._rng.getrandbits(128))
            child = StrategyGenome(
                strategy_id=f"G{current_generation+1}_{kid.hex[:8]}",
                generation=current_generation + 1,
                parent_ids=[p1_genome.strategy_id, p2_genome.strategy_id],
                params=child_params,
                tags=["mutation"]
            )
            new_genomes.append(child)
            
        # 2. Random Injection (preventing local minima stagnation)
        for _ in range(2):
            rid = uuid.UUID(int=self._rng.getrandbits(128))
            random_child = StrategyGenome(
                strategy_id=f"G{current_generation+1}_rand_{rid.hex[:8]}",
                generation=current_generation + 1,
                parent_ids=[],
                params={
                    "velocity_window": self._rng.randint(5, 50),
                    "confidence_threshold": self._rng.uniform(0.1, 0.9)
                },
                tags=["random"]
            )
            new_genomes.append(random_child)

        # Register exclusively as CHALLENGERS (Shadow mode)
        for g in new_genomes:
            self.registry.register(g, as_champion=False)
            
        return [g.strategy_id for g in new_genomes]
