# v3/__init__.py

from .optimization import MetaHeuristicaV3
from .data_loader import prepare_equipes_v3, prepare_pendencias_v3

__all__ = [
    "MetaHeuristicaV3",
    "prepare_equipes_v3",
    "prepare_pendencias_v3",
]