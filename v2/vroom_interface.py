# v2/vroom_interface.py
import os
import json
import requests

VROOM_URL = os.environ.get("VROOM_URL", "http://localhost:3000/")

def executar_vroom(start, end, jobs):
    """
    Envia uma requisição POST ao VROOM.
    - start/end: [lon, lat]
    - jobs: lista de dicts {id, location:[lon,lat], service:segundos}
    Retorna o JSON da rota (dict) ou levanta HTTPError (para main/otimização capturar).
    """
    payload = {
        "vehicles": [{
            "id": 1,
            "start": start,
            **({"end": end} if end is not None else {})
        }],
        "jobs": jobs,
        "options": {"g": False}
    }
    try:
        r = requests.post(VROOM_URL, json=payload, timeout=20)
        if r.status_code >= 400:
            # Log mais explícito
            try:
                err = r.json()
            except Exception:
                err = r.text
            print(f"⚠️  VROOM HTTP {r.status_code}: {err}")
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as he:
        # Propaga para o chamador (será tratado com fallback)
        raise he
    except Exception as e:
        raise e
