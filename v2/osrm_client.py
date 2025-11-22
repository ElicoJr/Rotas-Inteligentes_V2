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
        """
        Chama /table/v1/{profile}/{coords}?annotations=duration,distance
        e retorna o JSON com durations/distances.
        """
        url = f"{self.base_url}/table/v1/{self.profile}/{self._format_coords(coords)}"
        params = {"annotations": "duration,distance"}
        r = requests.get(url, params=params, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def route_legs_durations(self, coords):
        """
        Retorna duas listas:
        - legs_dur: duração (segundos) de cada perna coords[i] -> coords[i+1]
        - legs_dist: distância (metros) de cada perna coords[i] -> coords[i+1]
        """
        res = self.table(coords)
        dur = res.get("durations")
        dist = res.get("distances")
        n = len(coords)

        legs_dur = []
        legs_dist = []

        if not dur or not dist:
            return [0.0] * (n - 1), [0.0] * (n - 1)

        for i in range(n - 1):
            d_ij = None
            if i < len(dur) and (i + 1) < len(dur[i]):
                d_ij = dur[i][i + 1]
            c_ij = None
            if i < len(dist) and (i + 1) < len(dist[i]):
                c_ij = dist[i][i + 1]

            legs_dur.append(float(d_ij) if d_ij is not None else 0.0)
            legs_dist.append(float(c_ij) if c_ij is not None else 0.0)

        return legs_dur, legs_dist

    def nearest(self, lon: float, lat: float):
        """
        Usa o endpoint /nearest para "snapar" um ponto à via mais próxima.
        Retorna (lon_corrigido, lat_corrigida). Se falhar, devolve o original.
        """
        url = f"{self.base_url}/nearest/v1/{self.profile}/{lon},{lat}"
        params = {"number": 1}
        try:
            r = requests.get(url, params=params, timeout=self.timeout)
            r.raise_for_status()
            data = r.json()
            waypoints = data.get("waypoints") or []
            if waypoints:
                loc = waypoints[0].get("location")
                if loc and len(loc) == 2:
                    return float(loc[0]), float(loc[1])
        except Exception:
            pass
        return float(lon), float(lat)