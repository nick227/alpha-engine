from __future__ import annotations
import os
from dataclasses import dataclass
from typing import Any
import yaml

@dataclass
class KeyManager:
    config_path: str = "config/keys.yaml"

    def _load(self) -> dict[str, Any]:
        if not os.path.exists(self.config_path):
            return {}
        with open(self.config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    def get(self, provider: str) -> dict[str, str]:
        data = self._load().get(provider, {}) or {}
        resolved: dict[str, str] = {}
        for key, value in data.items():
            if isinstance(value, str) and value.startswith("ENV:"):
                resolved[key] = os.getenv(value.split("ENV:", 1)[1], "")
            else:
                resolved[key] = value
        return resolved
