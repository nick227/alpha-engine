from __future__ import annotations
from typing import Dict, List, Optional
from pydantic import BaseModel, Field

class StrategyGenome(BaseModel):
    strategy_id: str = Field(..., allow_mutation=False) # Immutable IDs
    generation: int
    parent_ids: List[str]
    params: Dict[str, float | int | str | bool]
    tags: Optional[List[str]] = None

class StrategyRegistry:
    """
    Holds lineage, generation, and parameter data for all strategies.
    Strictly segregates active 'Champions' from shadow 'Challengers'.
    """
    def __init__(self):
        # All known genomes mapped by immutable strategy_id
        self._genomes: Dict[str, StrategyGenome] = {}
        
        # Classifications
        self._champions: set[str] = set()
        self._challengers: set[str] = set()

    def register(self, genome: StrategyGenome, as_champion: bool = False):
        if genome.strategy_id in self._genomes:
            raise ValueError(f"Strategy ID {genome.strategy_id} already exists. IDs must be immutable.")
        
        self._genomes[genome.strategy_id] = genome
        if as_champion:
            self._champions.add(genome.strategy_id)
        else:
            self._challengers.add(genome.strategy_id)

    def get_genome(self, strategy_id: str) -> StrategyGenome | None:
        return self._genomes.get(strategy_id)

    def get_champions(self) -> List[StrategyGenome]:
        return [self._genomes[sid] for sid in self._champions]

    def get_challengers(self) -> List[StrategyGenome]:
        return [self._genomes[sid] for sid in self._challengers]

    def promote_to_champion(self, strategy_id: str):
        """Moves a challenger to champion status."""
        if strategy_id in self._challengers:
            self._challengers.remove(strategy_id)
        if strategy_id in self._genomes:
            self._champions.add(strategy_id)

    def demote_champion(self, strategy_id: str, kill: bool = False):
        """Removes a champion setting it back to a challenger or deleting it entirely."""
        if strategy_id in self._champions:
            self._champions.remove(strategy_id)
        
        if kill:
            if strategy_id in self._genomes:
                del self._genomes[strategy_id]
        else:
            self._challengers.add(strategy_id)
