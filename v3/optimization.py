# v3/optimization.py
import math
import pandas as pd
import numpy as np
import random
from math import exp

from v2.vroom_client import VroomClient
from v2.osrm_client import OSRMClient
from v2.utils import gerar_jobs_com_ids, _service_seconds_from_row
from v2 import config


class MetaHeuristicaV3:
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

        # Base específica da equipe, se existir; fallback para base global
        eq_base_lon = self.equipe.get("base_lon", None)
        eq_base_lat = self.equipe.get("base_lat", None)

        if pd.notna(eq_base_lon) and pd.notna(eq_base_lat):
            self.base_lon = float(eq_base_lon)
            self.base_lat = float(eq_base_lat)
        else:
            self.base_lon = float(config.BASE_LON)
            self.base_lat = float(config.BASE_LAT)

        # Snap da base para via mais próxima
        try:
            self.base_lon, self.base_lat = self.osrm.nearest(self.base_lon, self.base_lat)
        except Exception:
            pass

        # Momento de início do turno da equipe (para cálculo de tempo pendente/vencimento)
        self.turno_ini = pd.to_datetime(self.equipe["inicio_turno"], errors="coerce")

    # ---------------- PRIORIDADE (score) ----------------
    def _score_base(self, row: pd.Series) -> float:
        """
        Score de prioridade combinando:
        - OS comercial:
            * mais prioridade quanto mais próxima do vencimento ou já vencida (dataven)
            * mais prioridade quanto maior o EUSD
        - OS técnica:
            * mais prioridade quanto mais tempo pendente (datasol -> inicio_turno)
            * mais prioridade quanto maior o EUSD

        Também aproveita, se existirem, colunas pré-calculadas:
        - prioridade
        - tempo_espera
        - violacao
        """
        tipo = (row.get("tipo_serv") or row.get("tipo") or "").strip().lower()

        # tempo_espera (se já existir no dataframe)
        tempo_espera = float(row.get("tempo_espera", 0) or 0)

        # valor de EUSD (tanto faz se vier como EUSD, eusd, EUSD_FIO_B)
        eusd_raw = row.get("EUSD", row.get("eusd", row.get("EUSD_FIO_B", 0)))
        try:
            eusd = float(eusd_raw or 0.0)
        except Exception:
            eusd = 0.0

        # normalização simples para não explodir o score (log(1 + EUSD))
        eusd_score = math.log1p(eusd) if eusd > 0 else 0.0

        # tempo pendente em dias até o início do turno
        tempo_pendente_dias = 0.0
        datasol = row.get("datasol") or row.get("data_sol")
        if datasol is not None:
            try:
                ds = pd.to_datetime(datasol, errors="coerce")
                if pd.notna(ds) and pd.notna(self.turno_ini):
                    tempo_pendente_dias = max(
                        0.0, (self.turno_ini - ds).total_seconds() / 86400.0
                    )
            except Exception:
                tempo_pendente_dias = 0.0

        # urgência por vencimento (somente comerciais)
        urg_venc = 0.0
        if tipo == "comercial":
            dataven = row.get("dataven") or row.get("data_venc")
            if dataven is not None:
                try:
                    dv = pd.to_datetime(dataven, errors="coerce")
                    if pd.notna(dv) and pd.notna(self.turno_ini):
                        dias_para_venc = (dv - self.turno_ini).total_seconds() / 86400.0
                        # queremos mais score quanto mais próxima de vencer (dias_para_venc pequeno ou negativo)
                        urg_venc = -dias_para_venc
                except Exception:
                    urg_venc = 0.0

        # peso de prioridade/violação pré-existentes, se houverem
        prioridade_base = float(row.get("prioridade", 1) or 1)
        violacao = float(row.get("violacao", 0) or 0)

        # Ponderação final (ajustável):
        # Comerciais: priorizam vencimento + EUSD, técnicos: tempo pendente + EUSD
        if tipo == "comercial":
            score = (
                1.0 * prioridade_base
                + 3.0 * urg_venc       # urgência por vencimento
                + 0.5 * tempo_pendente_dias
                + 1.0 * eusd_score
                - 0.5 * violacao
            )
        elif tipo == "técnico":
            score = (
                1.0 * prioridade_base
                + 2.5 * tempo_pendente_dias
                + 1.0 * eusd_score
                - 0.5 * violacao
            )
        else:
            # tipo indefinido: usa um mix suave
            score = (
                1.0 * prioridade_base
                + 1.0 * tempo_pendente_dias
                + 0.8 * eusd_score
                - 0.5 * violacao
            )

        # também mistura tempo_espera se existir (em minutos)
        score += 0.001 * tempo_espera

        return float(score)

    # ---------------- AG ----------------
    def _ag(self, pool, k=10, pop_size=20, gens=10, pmut=0.2):
        n = len(pool)
        k = min(k, n)
        if k <= 0:
            return []

        # caso trivial: escolher apenas 1 índice (evita randint(1,0))
        if k == 1:
            if n == 0:
                return []
            best_idx = max(range(n), key=lambda i: self._score_base(pool.iloc[i]))
            return [best_idx]

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
        if not sol_idx or len(sol_idx) <= 1:
            return sol_idx
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
    def _aco(self, pool, sol_sa_idx, k=None, iters=8, ants=8, evap=0.5):
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

        # reforça solução do SA, se existir
        if sol_sa_idx:
            base_fit = score_subset(sol_sa_idx)
            if base_fit > best_fit:
                best_fit, best_sol = base_fit, sol_sa_idx[:]
            if base_fit > 0:
                for i in sol_sa_idx:
                    pher[i] += base_fit / 10.0

        for _ in range(iters):
            # garante feromônios sempre positivos
            pher = np.nan_to_num(pher, nan=0.0)
            min_pher = pher.min()
            if min_pher <= 0:
                pher = pher - min_pher + 1e-6  # shift para ficar > 0

            total = pher.sum()
            if not np.isfinite(total) or total <= 0:
                pher = np.ones_like(pher)
                total = pher.sum()

            probs = pher / total

            for _a in range(ants):
                choice = np.random.choice(
                    np.arange(n),
                    size=min(k, n),
                    replace=False,
                    p=probs,
                )
                fit = score_subset(choice.tolist())
                if fit > best_fit:
                    best_fit, best_sol = fit, choice.tolist()
                if fit > 0:
                    pher[choice] += fit / 10.0

            pher *= (1.0 - evap)

        if not best_sol:
            return pd.DataFrame()
        return pool.iloc[best_sol].copy()

    # ---------------- VROOM + fallbacks ----------------
    def _vroom(self, df_jobs: pd.DataFrame):
        """Calcula ETA/ETD com prioridade VROOM; fallback OSRM, último recurso Haversine."""
        lon_e = self.base_lon
        lat_e = self.base_lat

        jobs, df_jobs_tagged = gerar_jobs_com_ids(df_jobs)

        for c in ["dth_chegada_estimada", "dth_final_estimada", "fim_turno_estimado"]:
            if c not in df_jobs_tagged.columns:
                df_jobs_tagged[c] = pd.NaT
        if "eta_source" not in df_jobs_tagged.columns:
            df_jobs_tagged["eta_source"] = pd.Series(index=df_jobs_tagged.index, dtype="string")

        if len(jobs) == 0:
            return None, df_jobs_tagged

        t0 = self.turno_ini

        vehicle = {
            "id": 1,
            "start": [lon_e, lat_e],
            "end": [lon_e, lat_e],
            "time_window": [
                0,
                int((pd.to_datetime(self.equipe["fim_turno"]) - t0).total_seconds()),
            ],
        }

        # --- TENTATIVA 1: VROOM (ordem + tempos) ---
        try:
            resp = self.vroom.route(vehicle, jobs)
        except Exception:
            resp = None

        if resp and resp.get("routes"):
            route = resp["routes"][0]
            steps = route.get("steps", [])

            # mapa job_id -> arrival (segundos)
            eta_map = {}
            end_arrival_s = None
            for st in steps:
                st_type = st.get("type")
                arr_s = st.get("arrival")
                if arr_s is None:
                    continue
                arr_s = int(arr_s)
                if st_type == "job":
                    jid = int(st.get("job"))
                    eta_map[jid] = t0 + pd.to_timedelta(arr_s, unit="s")
                elif st_type == "end":
                    end_arrival_s = arr_s

            if eta_map and "job_id_vroom" in df_jobs_tagged.columns:
                df_jobs_tagged["dth_chegada_estimada"] = (
                    df_jobs_tagged["job_id_vroom"].map(eta_map).astype("datetime64[ns]")
                )
                df_jobs_tagged.loc[df_jobs_tagged["dth_chegada_estimada"].notna(), "eta_source"] = "VROOM"

            # termino = chegada + TE (segundos)
            te_sec = df_jobs_tagged.apply(_service_seconds_from_row, axis=1)
            mask_eta = df_jobs_tagged["dth_chegada_estimada"].notna()
            df_jobs_tagged.loc[mask_eta, "dth_final_estimada"] = pd.to_datetime(
                df_jobs_tagged.loc[mask_eta, "dth_chegada_estimada"], errors="coerce"
            ) + pd.to_timedelta(te_sec[mask_eta].values, unit="s")

            # chegada à base pela arrival do 'end'
            if end_arrival_s is not None:
                df_jobs_tagged["fim_turno_estimado"] = t0 + pd.to_timedelta(end_arrival_s, unit="s")

            return resp, df_jobs_tagged

        # pausa da equipe (usada no fallback)
        pausa_ini = (
            pd.to_datetime(self.equipe.get("dthpausa_ini"), errors="coerce")
            if "dthpausa_ini" in self.equipe.index
            else pd.NaT
        )
        pausa_fim = (
            pd.to_datetime(self.equipe.get("dthpausa_fim"), errors="coerce")
            if "dthpausa_fim" in self.equipe.index
            else pd.NaT
        )

        # --- TENTATIVA 2: OSRM sequencial ---
        try:
            return resp, _osrm_eta_etd(
                self.osrm,
                df_jobs_tagged,
                self.turno_ini,
                lon_e,
                lat_e,
                pausa_ini=pausa_ini,
                pausa_fim=pausa_fim,
            )
        except Exception:
            pass

        # --- TENTATIVA 3: Haversine sequencial ---
        return resp, _haversine_eta_etd(
            df_jobs_tagged,
            self.turno_ini,
            lon_e,
            lat_e,
            pausa_ini=pausa_ini,
            pausa_fim=pausa_fim,
        )

    def otimizar_para_equipe(self):
        if self.pool_base.empty:
            return None

        # Agora NÃO filtramos mais por dt_ref == dia: queremos permitir backlog de dias anteriores,
        # desde que datasol < inicio_turno (feito a seguir).
        pool = self.pool_base.reset_index(drop=True)

        elig = pd.to_datetime(pool.get("datasol", pd.NaT), errors="coerce")
        pool = pool[elig < self.turno_ini].reset_index(drop=True)
        if pool.empty:
            return None

        # ==== PRÉ-FILTRO DE PERFORMANCE COM PRIORIDADE (vencimento, tempo pendente, EUSD) ====
        fator_pool = 4
        max_pool = min(self.limite_por_equipe * fator_pool, len(pool))

        if len(pool) > max_pool:
            pool = pool.copy()
            # flag se é comercial
            pool["__is_com"] = (pool["tipo_serv"] == "comercial").astype(int)
            # datas
            pool["__datasol"] = pd.to_datetime(pool["datasol"], errors="coerce")
            pool["__dataven"] = pd.to_datetime(pool.get("dataven", pd.NaT), errors="coerce")

            # EUSD normalizado
            eusd_col = pool.get("EUSD", pool.get("eusd", pool.get("EUSD_FIO_B", 0)))
            pool["__eusd"] = pd.to_numeric(eusd_col, errors="coerce").fillna(0.0)

            # Comerciais primeiro, ordenando por:
            # - é comercial desc
            # - dataven asc (mais urgente primeiro)
            # - datasol asc (mais antiga primeiro)
            # - EUSD desc (mais valor primeiro)
            pool = pool.sort_values(
                by=["__is_com", "__dataven", "__datasol", "__eusd"],
                ascending=[False, True, True, False],
            ).head(max_pool)

            pool = pool.drop(columns=["__is_com", "__datasol", "__dataven", "__eusd"], errors="ignore")\
                       .reset_index(drop=True)
        # ====== FIM DO PRÉ-FILTRO ======

        # -------- PRIORIZAÇÃO: AG → SA → ACO --------
        k = self.limite_por_equipe

        # atalho: se poucas OS, não rodar meta-heurística pesada
        if len(pool) <= k:
            cand_aco = pool.copy()
        else:
            sol_ag = self._ag(pool, k=k)
            sol_sa = self._sa(pool, sol_ag)
            cand_aco = self._aco(pool, sol_sa, k=k)

        if cand_aco.empty:
            return None

        # -------- ETA/ETD com VROOM/OSRM/Haversine --------
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

        # base da equipe em cada linha
        cand["base_lon"] = self.base_lon
        cand["base_lat"] = self.base_lat

        final = _padronizar_layout_final(cand)
        return {"resp": final}


# ===== Helpers ETA/ETD ===== (iguais à versão anterior)
def _apply_pause(t_cursor, dur_seconds, pausa_ini=None, pausa_fim=None):
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

    if t_end <= pausa_ini or t >= pausa_fim:
        return t_end

    if t < pausa_ini < t_end:
        before = (pausa_ini - t).total_seconds()
        if dur <= before:
            return t + pd.to_timedelta(dur, unit="s")
        remaining = dur - int(before)
        return pausa_fim + pd.to_timedelta(remaining, unit="s")

    if pausa_ini <= t < pausa_fim:
        return pausa_fim + pd.to_timedelta(dur, unit="s")

    return t_end


def _haversine_travel_seconds(lon1, lat1, lon2, lat2, vel_kmh: float = 30.0) -> int:
    import math

    if (lon1, lat1) == (lon2, lat2):
        return 0

    R = 6371000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlamb = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlamb / 2) ** 2
    )
    dist = 2 * R * math.asin(math.sqrt(a))

    v_ms = max(vel_kmh, 1e-3) * 1000 / 3600.0
    return int(dist / v_ms) if v_ms > 0 else 0


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

    leg_durs, _ = osrm_client.route_legs_durations(coords)
    t_cursor = pd.to_datetime(inicio_turno_pvh, errors="coerce")

    leg_idx = 0
    for i, r in df_jobs_tagged.iterrows():
        travel_s = int(leg_durs[leg_idx]) if leg_idx < len(leg_durs) else 0

        lon1, lat1 = coords[leg_idx]
        lon2, lat2 = coords[leg_idx + 1]
        if travel_s <= 0 and (lon1, lat1) != (lon2, lat2):
            travel_s = _haversine_travel_seconds(lon1, lat1, lon2, lat2)

        chegada = _apply_pause(t_cursor, travel_s, pausa_ini=pausa_ini, pausa_fim=pausa_fim)
        df_jobs_tagged.at[i, "dth_chegada_estimada"] = chegada

        te_sec = _service_seconds_from_row(r)
        termino = _apply_pause(chegada, te_sec, pausa_ini=pausa_ini, pausa_fim=pausa_fim)
        df_jobs_tagged.at[i, "dth_final_estimada"] = termino

        df_jobs_tagged.at[i, "eta_source"] = "OSRM"
        t_cursor = termino
        leg_idx += 1

    back_s = int(leg_durs[leg_idx]) if leg_idx < len(leg_durs) else 0
    lon1, lat1 = coords[leg_idx]
    lon2, lat2 = coords[leg_idx + 1]
    if back_s <= 0 and (lon1, lat1) != (lon2, lat2):
        back_s = _haversine_travel_seconds(lon1, lat1, lon2, lat2)

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
    for c in ["dth_chegada_estimada", "dth_final_estimada", "fim_turno_estimado"]:
        if c not in df_jobs_tagged.columns:
            df_jobs_tagged[c] = pd.NaT
    if "eta_source" not in df_jobs_tagged.columns:
        df_jobs_tagged["eta_source"] = pd.Series(index=df_jobs_tagged.index, dtype="string")

    t_cursor = pd.to_datetime(inicio_turno_pvh, errors="coerce")
    cur_lon, cur_lat = lon_e, lat_e

    for i, r in df_jobs_tagged.iterrows():
        lon = float(r["longitude"])
        lat = float(r["latitude"])
        travel_s = _haversine_travel_seconds(cur_lon, cur_lat, lon, lat, vel_kmh=vel_kmh)

        chegada = _apply_pause(t_cursor, travel_s, pausa_ini=pausa_ini, pausa_fim=pausa_fim)
        df_jobs_tagged.at[i, "dth_chegada_estimada"] = chegada

        te_sec = _service_seconds_from_row(r)
        termino = _apply_pause(chegada, te_sec, pausa_ini=pausa_ini, pausa_fim=pausa_fim)
        df_jobs_tagged.at[i, "dth_final_estimada"] = termino

        df_jobs_tagged.at[i, "eta_source"] = "HAVERSINE"
        t_cursor = termino
        cur_lon, cur_lat = lon, lat

    back_s = _haversine_travel_seconds(cur_lon, cur_lat, lon_e, lat_e, vel_kmh=vel_kmh)
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

    if "base_lon" not in out.columns:
        out["base_lon"] = config.BASE_LON
    if "base_lat" not in out.columns:
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