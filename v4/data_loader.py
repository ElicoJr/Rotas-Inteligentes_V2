# v4/data_loader.py
"""
V4 reutiliza os mesmos loaders do V3, que por sua vez usam v2.data_loader.
"""

from v3.data_loader import prepare_equipes_v3, prepare_pendencias_v3

__all__ = ["prepare_equipes_v3", "prepare_pendencias_v3"]
