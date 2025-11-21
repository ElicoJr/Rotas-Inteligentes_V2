import pandas as pd
import numpy as np
import random
from math import exp, isfinite
from datetime import timedelta

from v2.vroom_client import VroomClient
from v2.osrm_client import OSRMClient
from v2.utils import gerar_jobs_com_ids, safe_number, _service_seconds_from_row
from v2 import config


class MetaHeuristica:
    def __init__(self, equipe_row, pend_tec, pend_com, limite_por_equipe: int = 15):
        self.equipe = equipe_row
        self.pool_base = (
            pd.concat([pend_tec, pend_com], ignore_index=True)
            .dropna(subset=["dt_ref"])
            .reset_index(drop=True)
        )
        self.limite_por_equipe = int(limite_por_equipe)
        self.vroom = VroomClient()
        self.osrm = OSRMClient()
        # Base fixa (lon, lat)
        self.base_lon = float(config.BASE_LON)
        self.base_lat = float(config.BASE_LAT)

    # ---------------- PRIORIDADE (score) ----------------
    def _score_base(self, row):
        prioridade = float(row.get("prioridade", 1) or 1)
        tempo_espera = float(row.get("tempo_espera", 0) or 0)  # min
        violacao = float(row.get("violacao", 0) or 0)
        return (2.0 * prioridade) - (0.005 * tempo_espera) - (0.5 * violacao)

    # ---------------- AG ----------------
    def _ag(self, pool, k=10, pop_size=25, gens=15, pmut=0.2):
        n = len(pool)
        k = min(k, n)
        if k <= 0:
            return []

        def fit(sol_idx):
            if not sol_idx:
                return -1e9
            sc = [self._score_base(pool.iloc[i]) for i in sol_idx]
            return float(np.mean(sc))

        pop = [random.sample(range(n), k) for _ in range(pop_size)]
        for _ in range(gens):
            pop.sort(key=fit, reverse=True)
            elite = pop[:10]

            children = []
            while len(children) + len(elite) < pop_size:
                a, b = random.sample(elite, 2)
                cut = random.randint(1, k - 1)
                child = a[:cut] + [x for x in b if x not in a[:cut]]
                child = child[:k]
                if random.random() < pmut and k >= 2:
                    i, j = random.sample(range(k), 2)
                    child[i], child[j] = child[j], child[i]
                miss = [x for x in range(n) if x not in child]
                while len(child) < k and miss:
                    child.append(miss.pop())
                children.append(child[:k])
            pop = elite + children

        pop.sort(key=fit, reverse=True)
        return pop[0]

    # ---------------- SA ----------------
    def _sa(self, pool, sol_idx, t0=100.0, alpha=0.9):
        if not sol_idx:
            return []
        k = len(sol_idx)

        def fit(sol):
            sc = [self._score_base(pool.iloc[i]) for i in sol]
            return float(np.mean(sc)) if sc else -1e9

        cur = sol_idx[:]
        best = sol_idx[:]
        fcur = fit(cur)
        fbest = fcur
        T = t0
        while T >= 1.0:
            i, j = random.sample(range(k), 2)
            viz = cur[:]
            viz[i], viz[j] = viz[j], viz[i]
            fviz = fit(viz)
            if fviz >= fcur or random.random() < exp((fviz - fcur) / T):
                cur, fcur = viz, fviz
                if fviz > fbest:
                    best, fbest = viz[:], fviz
            T *= alpha
        return best

    # ---------------- ACO ----------------
    def _aco(self, pool, sol_sa_idx, k=None, iters=10, ants=10, evap=0.5):
        n = len(pool)
        if n == 0:
            return pd.DataFrame()
        if k is None:
            k = min(self.limite_por_equipe, n)
        pher = np.ones(n, dtype=float)

        def score_subset(idxs):
            if not idxs:
                return -1e9
            sc = [self._score_base(pool.iloc[i]) for i in idxs]
            return float(np.mean(sc))

        best_sol = []
        best_fit = -1e9

        if sol_sa_idx:
            base_fit = score_subset(sol_sa_idx)
            if base_fit > best_fit:
                best_fit, best_sol = base_fit, sol_sa_idx[:]
            for i in sol_sa_idx:
                pher[i] += base_fit / 10.0

        for _ in range(iters):
            for _a in range(ants):
                probs = pher / (pher.sum() if pher.sum() > 0 else 1.0)
                choice = np.random.choice(np.arange(n), size=min(k, n), replace=False, p=probs)
                fit = score_subset(choice.tolist())
                if fit > best_fit:
                    best_fit, best_sol = fit, choice.tolist()
                pher[choice] += fit / 10.0
            pher *= 1.0 - evap

        if not best_sol:
            return pd.DataFrame()
        return pool.iloc[best_sol].copy()

    # ---------------- VROOM + fallbacks ----------------
    def _vroom(self, df_jobs: pd.DataFrame):
        """Calcula ETA/ETD usando VROOM apenas para a sequência de jobs.

        A ordem é determinada pelo VROOM (quando possível) e os tempos
        são calculados sequencialmente por OSRM ou Haversine, permitindo
        que a pausa da equipe seja respeitada.
        """
        lon_e = self.base_lon
        lat_e = self.base_lat

        jobs, df_jobs_tagged = gerar_jobs_com_ids(df_jobs)

        # Garante colunas-chave
        for c in ["dth_chegada_estimada", "dth_final_estimada", "fim_turno_estimado"]:
            if c not in df_jobs_tagged.columns:
                df_jobs_tagged[c] = pd.NaT
        if "eta_source" not in df_jobs_tagged.columns:
            df_jobs_tagged["eta_source"] = pd.Series(index=df_jobs_tagged.index, dtype="string")

        if len(jobs) == 0:
            return None, df_jobs_tagged

        # referência temporal correta: INÍCIO DO TURNO em PVH (naive)
        t0 = pd.to_datetime(self.equipe["inicio_turno"], errors="coerce")

        vehicle = {
            "id": 1,
            "start": [lon_e, lat_e],
            "end": [lon_e, lat_e],
            # arrivals do VROOM são relativos a t=0 -> usamos apenas a ordem
            "time_window": [
                0,
                int((pd.to_datetime(self.equipe["fim_turno"]) - t0).total_seconds()),
            ],
        }

        try:
            resp = self.vroom.route(vehicle, jobs)
        except Exception:
            resp = None

        # pausa da equipe (pode ser ausente)
        pausa_ini = pd.to_datetime(self.equipe.get("dthpausa_ini"), errors="coerce") if "dthpausa_ini" in self.equipe.index else pd.NaT
        pausa_fim = pd.to_datetime(self.equipe.get("dthpausa_fim"), errors="coerce") if "dthpausa_fim" in self.equipe.index else pd.NaT

        routes = resp.get("routes", []) if resp else []
        steps = routes[0].get("steps", []) if routes else []

        if steps:
            # extrair sequência de jobs do VROOM
            job_seq = [
                int(st.get("job"))
                for st in steps
                if st.get("type") == "job" and st.get("job") is not None
            ]

            if job_seq and "job_id_vroom" in df_jobs_tagged.columns:
                # reordenar DF na sequência sugerida pelo VROOM
                base = df_jobs_tagged.set_index("job_id_vroom")
                # filtrar apenas ids presentes em job_seq para evitar KeyError
                job_seq_valid = [jid for jid in job_seq if jid in base.index]
                df_ordered = base.loc[job_seq_valid].reset_index()
            else:
                df_ordered = df_jobs_tagged.copy()

            # calcular tempos sequenciais na ordem do VROOM (OSRM/Haversine + pausa)
            try:
                df_sched = _osrm_eta_etd(
                    self.osrm,
                    df_ordered,
                    self.equipe["inicio_turno"],
                    lon_e,
                    lat_e,
                    pausa_ini=pausa_ini,
                    pausa_fim=pausa_fim,
                )
            except Exception:
                df_sched = _haversine_eta_etd(
                    df_ordered,
                    self.equipe["inicio_turno"],
                    lon_e,
                    lat_e,
                    pausa_ini=pausa_ini,
                    pausa_fim=pausa_fim,
                )

            return resp, df_sched

        # ---- Fallback 1: OSRM (ordem original do DF) ----
        try:
            return resp, _osrm_eta_etd(
                self.osrm,
                df_jobs_tagged,
                self.equipe["inicio_turno"],
                lon_e,
                lat_e,
                pausa_ini=pausa_ini,
                pausa_fim=pausa_fim,
            )
        except Exception:
            pass

        # ---- Fallback 2: Haversine (ordem original do DF) ----
        return resp, _haversine_eta_etd(
            df_jobs_tagged,
            self.equipe["inicio_turno"],
            lon_e,
            lat_e,
            pausa_ini=pausa_ini,
            pausa_fim=pausa_fim,
        )

    def otimizar_para_equipe(self):
        if self.pool_base.empty:
            return None

        dia = pd.to_datetime(self.equipe.get("dt_ref", self.equipe["inicio_turno"])).normalize()
        pool = self.pool_base[self.pool_base["dt_ref"] == dia].reset_index(drop=True)
        if pool.empty:
            return None

        ini_turno = pd.to_datetime(self.equipe["inicio_turno"])
        elig = pd.to_datetime(pool.get("datasol", pd.NaT), errors="coerce")
        pool = pool[elig < ini_turno].reset_index(drop=True)
        if pool.empty:
            return None

        # -------- PRIORIZAÇÃO: AG → SA → ACO --------
        k = self.limite_por_equipe
        sol_ag = self._ag(pool, k=k)
        sol_sa = self._sa(pool, sol_ag)
        cand_aco = self._aco(pool, sol_sa, k=k)
        if cand_aco.empty:
            return None

        # -------- ETA/ETD na ordem do cand_aco (VROOM/OSRM/Haversine) --------
        resp, cand = self._vroom(cand_aco)

        if resp and resp.get("routes"):
            r0 = resp["routes"][0]
            cand["distancia_vroom"] = r0.get("distance")
            cand["duracao_vroom"] = r0.get("duration")

        # metadados da equipe
        cand["equipe"] = self.equipe.get("nome", "N/D")
        for meta_col in [
            "dthaps_ini",
            "dthaps_fim_ajustado",
            "inicio_turno",
            "fim_turno",
            "dthpausa_ini",
            "dthpausa_fim",
        ]:
            if meta_col in self.equipe.index:
                cand[meta_col] = self.equipe[meta_col]

        final = _padronizar_layout_final(cand)
        return {"resp": final}


# ===== Helpers ETA/ETD =====

def _apply_pause(t_cursor, dur_seconds, pausa_ini=None, pausa_fim=None):
    """Aplica a janela de pausa a um intervalo de tempo.

    - t_cursor: instante de início (Timestamp ou similar).
    - dur_seconds: duração do trecho em segundos.
    - pausa_ini/pausa_fim: intervalo de pausa; se inválido, retorna t_cursor + dur.
    """
    t = pd.to_datetime(t_cursor, errors="coerce")
    try:
        dur = int(dur_seconds or 0)
    except Exception:
        dur = 0

    if pd.isna(t) or dur <= 0:
        return t

    if pausa_ini is None or pausa_fim is None:
        return t + pd.to_timedelta(dur, unit="s")

    pausa_ini = pd.to_datetime(pausa_ini, errors="coerce")
    pausa_fim = pd.to_datetime(pausa_fim, errors="coerce")

    if pd.isna(pausa_ini) or pd.isna(pausa_fim) or pausa_fim <= pausa_ini:
        return t + pd.to_timedelta(dur, unit="s")

    t_end = t + pd.to_timedelta(dur, unit="s")

    # caso totalmente antes ou depois da pausa
    if t_end <= pausa_ini or t >= pausa_fim:
        return t_end

    # começa antes da pausa e cruza o início da pausa
    if t < pausa_ini < t_end:
        before = (pausa_ini - t).total_seconds()
        if dur <= before:
            return t + pd.to_timedelta(dur, unit="s")
        remaining = dur - int(before)
        return pausa_fim + pd.to_timedelta(remaining, unit="s")

    # começa dentro da pausa
    if pausa_ini <= t < pausa_fim:
        return pausa_fim + pd.to_timedelta(dur, unit="s")

    return t_end


def _osrm_eta_etd(osrm_client, df_jobs_tagged, inicio_turno_pvh, lon_e, lat_e, pausa_ini=None, pausa_fim=None):
    for c in ["dth_chegada_estimada", "dth_final_estimada", "fim_turno_estimado"]:
        if c not in df_jobs_tagged.columns:
            df_jobs_tagged[c] = pd.NaT
    if "eta_source" not in df_jobs_tagged.columns:
        df_jobs_tagged["eta_source"] = pd.Series(index=df_jobs_tagged.index, dtype="string")

    coords = [(lon_e, lat_e)]
    coords += [
        (float(r["longitude"]), float(r["latitude"])) for _, r in df_jobs_tagged.iterrows()
    ]
    coords.append((lon_e, lat_e))

    leg_durs, _ = osrm_client.route_legs_durations(coords)  # seg/perna
    t_cursor = pd.to_datetime(inicio_turno_pvh, errors="coerce")

    leg_idx = 0
    for i, r in df_jobs_tagged.iterrows():
        travel_s = int(leg_durs[leg_idx]) if leg_idx < len(leg_durs) else 0
        chegada = _apply_pause(t_cursor, travel_s, pausa_ini=pausa_ini, pausa_fim=pausa_fim)
        df_jobs_tagged.at[i, "dth_chegada_estimada"] = chegada

        te_sec = _service_seconds_from_row(r)
        termino = _apply_pause(chegada, te_sec, pausa_ini=pausa_ini, pausa_fim=pausa_fim)
        df_jobs_tagged.at[i, "dth_final_estimada"] = termino

        df_jobs_tagged.at[i, "eta_source"] = "OSRM"
        t_cursor = termino
        leg_idx += 1

    back_s = int(leg_durs[leg_idx]) if leg_idx < len(leg_durs) else 0
    chegada_base = _apply_pause(t_cursor, back_s, pausa_ini=pausa_ini, pausa_fim=pausa_fim)
    df_jobs_tagged["fim_turno_estimado"] = chegada_base
    df_jobs_tagged["eta_source"] = df_jobs_tagged["eta_source"].astype("string")
    return df_jobs_tagged


def _haversine_eta_etd(
    df_jobs_tagged,
    inicio_turno_pvh,
    lon_e,
    lat_e,
    vel_kmh: float = 30.0,
    pausa_ini=None,
    pausa_fim=None,
):
    import math

    for c in ["dth_chegada_estimada", "dth_final_estimada", "fim_turno_estimado"]:
        if c not in df_jobs_tagged.columns:
            df_jobs_tagged[c] = pd.NaT
    if "eta_source" not in df_jobs_tagged.columns:
        df_jobs_tagged["eta_source"] = pd.Series(index=df_jobs_tagged.index, dtype="string")

    def haversine(lon1, lat1, lon2, lat2):
        R = 6371000.0
        phi1, phi2 = math.radians(lat1), math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlamb = math.radians(lon2 - lon1)
        a = (
            math.sin(dphi / 2) ** 2
            + math.cos(phi1) * math.cos(phi2) * math.sin(dlamb / 2) ** 2
        )
        return 2 * R * math.asin(math.sqrt(a))

    v_ms = max(vel_kmh, 1e-3) * 1000 / 3600.0
    t_cursor = pd.to_datetime(inicio_turno_pvh, errors="coerce")
    cur_lon, cur_lat = lon_e, lat_e

    for i, r in df_jobs_tagged.iterrows():
        lon = float(r["longitude"])
        lat = float(r["latitude"])
        dist = haversine(cur_lon, cur_lat, lon, lat)
        travel_s = int(dist / v_ms) if v_ms > 0 else 0

        chegada = _apply_pause(t_cursor, travel_s, pausa_ini=pausa_ini, pausa_fim=pausa_fim)
        df_jobs_tagged.at[i, "dth_chegada_estimada"] = chegada

        te_sec = _service_seconds_from_row(r)
        termino = _apply_pause(chegada, te_sec, pausa_ini=pausa_ini, pausa_fim=pausa_fim)
        df_jobs_tagged.at[i, "dth_final_estimada"] = termino

        df_jobs_tagged.at[i, "eta_source"] = "HAVERSINE"
        t_cursor = termino
        cur_lon, cur_lat = lon, lat

    dist_back = haversine(cur_lon, cur_lat, lon_e, lat_e)
    back_s = int(dist_back / v_ms) if v_ms > 0 else 0
    chegada_base = _apply_pause(t_cursor, back_s, pausa_ini=pausa_ini, pausa_fim=pausa_fim)
    df_jobs_tagged["fim_turno_estimado"] = chegada_base
    df_jobs_tagged["eta_source"] = df_jobs_tagged["eta_source"].astype("string")
    return df_jobs_tagged


def _padronizar_layout_final(df):
    out = df.copy()

    for c in ["dth_chegada_estimada", "dth_final_estimada", "fim_turno_estimado"]:
        if c not in out.columns:
            out[c] = pd.NaT
    if "eta_source" not in out.columns:
        out["eta_source"] = pd.Series(index=out.index, dtype="string")

    if "numos" not in out.columns:
        out["numos"] = pd.NA
    if "datater_trab" not in out.columns:
        out["datater_trab"] = pd.NaT

    if "TE" not in out.columns and "te" in out.columns:
        out["TE"] = pd.to_numeric(out["te"], errors="coerce")
    if "TD" not in out.columns and "td" in out.columns:
        out["TD"] = pd.to_numeric(out["td"], errors="coerce")

    for c in [
        "datasol",
        "dataven",
        "datater_trab",
        "dthaps_ini",
        "dthaps_fim_ajustado",
        "inicio_turno",
        "fim_turno",
        "dthpausa_ini",
        "dthpausa_fim",
        "dth_chegada_estimada",
        "dth_final_estimada",
        "fim_turno_estimado",
    ]:
        if c in out.columns:
            out[c] = pd.to_datetime(out[c], errors="coerce")

    # auditoria: base fixa
    out["base_lon"] = config.BASE_LON
    out["base_lat"] = config.BASE_LAT

    final_cols = [
        "tipo_serv",
        "numos",
        "datasol",
        "dataven",
        "datater_trab",
        "TD",
        "TE",
        "equipe",
        "dthaps_ini",
        "dthaps_fim_ajustado",
        "inicio_turno",
        "fim_turno",
        "dthpausa_ini",
        "dthpausa_fim",
        "dth_chegada_estimada",
        "dth_final_estimada",
        "fim_turno_estimado",
        "eta_source",
        "base_lon",
        "base_lat",
    ]
    for c in final_cols:
        if c not in out.columns:
            out[c] = pd.NA

    out["eta_source"] = out["eta_source"].astype("string")
    return out
