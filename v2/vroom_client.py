# v2/vroom_client.py
import json
import requests
from v2 import config


class VroomClient:
    def __init__(self, base_url: str = None, timeout: int = 30):
        self.base_url = base_url or config.VROOM_URL
        self.timeout = timeout

    def _post(self, payload: dict):
        url = f"{self.base_url}"
        if not url.endswith("/"):
            url += "/"
        headers = {"Content-Type": "application/json"}
        resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def route(self, vehicle: dict, jobs: list):
        """
        Chama o endpoint VROOM para um único veículo + lista de jobs.
        Mantido para compatibilidade com V2/V3.
        """
        payload = {
            "vehicles": [vehicle],
            "jobs": jobs,
            "options": {"g": False},
        }
        return self._post(payload)

    def route_multi(self, vehicles: list, jobs: list):
        """
        Chama o endpoint VROOM para múltiplos veículos (multi-veículos) + lista de jobs.
        Usado pelo V4.
        """
        payload = {
            "vehicles": vehicles,
            "jobs": jobs,
            "options": {"g": False},
        }
        return self._post(payload)