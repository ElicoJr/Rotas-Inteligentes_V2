# vroom_interface.py
from __future__ import annotations
from typing import List, Tuple, Dict, Optional
import requests


VROOM_URL = "http://localhost:3000"
OSRM_URL = "http://localhost:5000"


def executar_vroom(
    *,
    start: Tuple[float, float],
    end: Optional[Tuple[float, float]],
    jobs: List[Dict],
) -> List[Dict]:
    """
    Faz POST no VROOM (porta 3000). Retorna a lista de steps (route[0]["steps"]).
    start/end: (lon, lat). Se end=None, não força retorno ao depósito.
    """
    if not jobs:
        return []

    vehicles = [
        {
            "id": 0,
            "start": list(start),
            **({"end": list(end)} if end is not None else {}),
        }
    ]
    payload = {"vehicles": vehicles, "jobs": jobs, "options": {"g": False}}
    r = requests.post(VROOM_URL + "/", json=payload, timeout=30)
    r.raise_for_status()
    data = r.json()

    routes = data.get("routes") or []
    if not routes:
        return []
    steps = routes[0].get("steps") or []
    return steps


def osrm_table(coords: List[Tuple[float, float]]) -> Dict:
    """
    Consulta a matrix de durações do OSRM /table (em segundos).
    coords: [(lon, lat), ...]
    """
    if len(coords) < 2:
        return {"durations": [[0.0]]}

    parts = ["{:.6f},{:.6f}".format(lon, lat) for lon, lat in coords]
    url = f"{OSRM_URL}/table/v1/driving/" + ";".join(parts) + "?annotations=duration"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()
