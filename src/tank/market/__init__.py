"""Market capture, strict semantics and auditable public exchange history."""

from .normalize import SemanticError
from .service import MarketService

__all__ = ["MarketService", "SemanticError"]
