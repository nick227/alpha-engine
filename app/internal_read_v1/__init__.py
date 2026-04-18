"""Internal read-only HTTP API v1 (trading-platform → alpha-engine on loopback)."""

from app.internal_read_v1.app import app

__all__ = ["app"]
