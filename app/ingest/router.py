from __future__ import annotations
from typing import Dict
from app.ingest.event_model import Event

class EventRouter:
    def __init__(self):
        self.routes = {
            "sentiment": [], # From news
            "quant": [],     # From market
            "regime": [],    # From macro
            "alpha": [],     # From bundles
            "crowd": [],     # From social
            "unrouted": []
        }
        
    def route(self, events: list[Event]) -> Dict[str, list[Event]]:
        """
        Dispatches events into their respective tracks for specialized processing.
        Clears previous dispatch states.
        """
        # reset internal state for new batch
        for k in self.routes:
            self.routes[k] = []
            
        for e in events:
            # Check source type or tags to route
            if e.source_type == "news" or "news" in e.tags:
                self.routes["sentiment"].append(e)
            elif e.source_type == "market" or "market" in e.tags:
                self.routes["quant"].append(e)
            elif e.source_type == "macro" or "macro" in e.tags:
                self.routes["regime"].append(e)
            elif e.source_type == "bundle" or "bundle" in e.tags:
                self.routes["alpha"].append(e)
            elif e.source_type == "social" or "social" in e.tags:
                self.routes["crowd"].append(e)
            else:
                self.routes["unrouted"].append(e)
                
        # Return a copy to avoid accidental mutation
        return {k: list(v) for k, v in self.routes.items() if len(v) > 0}
