import json
import requests
from v2 import config

class VroomClient:
    def __init__(self, base_url: str = None, timeout: int = 30):
        self.base_url = base_url or config.VROOM_URL
        self.timeout = timeout

    def route(self, vehicle: dict, jobs: list):
        """
        Chama o endpoint / para solver route do VROOM.
        """
        url = f"{self.base_url}"
        if not url.endswith("/"):
            url += "/"
        # payload mínimo
        payload = {
            "vehicles": [vehicle],
            "jobs": jobs,
            "options": {"g": False}
        }
        headers = {"Content-Type": "application/json"}
        resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()
