# v2/osrm_client.py
import requests
from v2 import config


class OSRMClient:
    def __init__(self, base_url: str = None, profile: str = "driving", timeout: int = 30):
        self.base_url = (base_url or config.OSRM_URL).rstrip("/")
        self.profile = profile
        self.timeout = timeout

    def _format_coords(self, coords):
        # coords: [(lon,lat), ...] → "lon,lat;lon,lat;..."
        return ";".join([f"{lon},{lat}" for (lon, lat) in coords])

    def table(self, coords):
        url = f"{self.base_url}/table/v1/{self.profile}/{self._format_coords(coords)}?annotations=duration,distance"
        r = requests.get(url, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def route_legs_durations(self, coords):
        """
        Retorna uma lista de durações (segundos) de cada perna:
        coords[0]→coords[1], coords[1]→coords[2], ..., coords[n-2]→coords[n-1]
        e as distâncias (metros) nas mesmas pernas.
        """
        res = self.table(coords)
        dur = res.get("durations")
        dist = res.get("distances")
        n = len(coords)
        legs_dur = []
        legs_dist = []
        for i in range(n - 1):
            legs_dur.append(float(dur[i][i + 1]) if dur and dur[i][i + 1] is not None else 0.0)
            legs_dist.append(float(dist[i][i + 1]) if dist and dist[i][i + 1] is not None else 0.0)
        return legs_dur, legs_dist

    def nearest(self, lon: float, lat: float):
        """
        Usa o endpoint /nearest do OSRM para 'snapar' um ponto à via mais próxima.
        Retorna (lon_corrigido, lat_corrigida). Se falhar, devolve o original.
        """
        url = f"{self.base_url}/nearest/v1/{self.profile}/{lon},{lat}?number=1"
        try:
            r = requests.get(url, timeout=self.timeout)
            r.raise_for_status()
            data = r.json()
            waypoints = data.get("waypoints") or []
            if waypoints:
                loc = waypoints[0].get("location")
                if loc and len(loc) == 2:
                    # OSRM retorna [lon, lat]
                    return float(loc[0]), float(loc[1])
        except Exception:
            pass
        return float(lon), float(lat)