# optimization.py
from __future__ import annotations
import math
from dataclasses import dataclass
from datetime import timedelta
from typing import Dict, List, Tuple

import pandas as pd
import requests

from vroom_interface import executar_vroom, osrm_table

# Base fixa (lon, lat)
BASE_LONLAT = (-63.88547754489104, -8.738553348981176)

# tolerância de estouro de HH (+1%)
OVERRUN_FRAC = 0.01


@dataclass
class Solucao:
    atendidos: List[Dict]
    fim_estimado: pd.Timestamp      # chegada na base
    chegada_base: pd.Timestamp      # igual a fim_estimado
    vroom_400: bool


class MetaHeuristica:
    def __init__(
        self,
        equipe_row: pd.Series,
        te_df: pd.DataFrame,
        co_df: pd.DataFrame,
        limite_por_equipe: int = 15,
        return_to_depot: bool = True,
    ):
        self.equipe = equipe_row
        self.te_all = te_df.copy()
        self.co_all = co_df.copy()
        self.limite = int(limite_por_equipe)
        self.return_to_depot = return_to_depot

        self.turno_ini: pd.Timestamp = pd.to_datetime(self.equipe["turno_ini"])
        self.turno_fim: pd.Timestamp = pd.to_datetime(self.equipe["turno_fim"])

        # HH total disponível (min)
        hh_total_min = max(0, int((self.turno_fim - self.turno_ini).total_seconds() // 60))
        self.hh_limite_min = int(hh_total_min * (1 + OVERRUN_FRAC))

        self.base = BASE_LONLAT
        self.vroom_400_flag = False

        # filtrar só o que está disponível até o INÍCIO DO TURNO
        self.te = self.te_all[self.te_all["data_sol"] <= self.turno_ini].copy()
        self.co = self.co_all[self.co_all["data_sol"] <= self.turno_ini].copy()

    # ---------- utilidades ----------
    @staticmethod
    def _haversine_minutes(a: Tuple[float, float], b: Tuple[float, float]) -> float:
        # 1 grau ~ 111km; velocidade média ~ 40 km/h => min = dist/40*60
        (lon1, lat1), (lon2, lat2) = a, b
        R = 6371.0
        p1, p2 = math.radians(lat1), math.radians(lat2)
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        x = math.sin(dlat / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2) ** 2
        d = 2 * R * math.asin(math.sqrt(x))
        return (d / 40.0) * 60.0

    def _dur_matrix_minutes(self, coords: List[Tuple[float, float]]) -> List[List[float]]:
        try:
            resp = osrm_table(coords)
            if not resp or resp.get("durations") is None:
                raise RuntimeError("OSRM sem durations")
            # OSRM em segundos -> minutos
            return [[(c if c is not None else 0.0) / 60.0 for c in row] for row in resp["durations"]]
        except Exception:
            # fallback haversine
            n = len(coords)
            M = [[0.0] * n for _ in range(n)]
            for i in range(n):
                for j in range(n):
                    if i != j:
                        M[i][j] = self._haversine_minutes(coords[i], coords[j])
            return M

    def _ordenar_por_prioridade(self) -> pd.DataFrame:
        co = self.co.copy()
        te = self.te.copy()

        co["tem_prazo"] = co["data_venc"].notna()
        co["vencida"] = co["tem_prazo"] & (co["data_venc"] < self.turno_ini)

        co_ok = co[co["tem_prazo"] & ~co["vencida"]].copy()
        co_venc = co[co["vencida"]].copy()
        te_ok = te.copy()

        # ordenação
        co_ok = co_ok.sort_values(["data_venc", "data_sol"])
        co_venc = co_venc.sort_values(["data_venc", "data_sol"])
        te_ok = te_ok.sort_values(["data_sol"])

        return pd.concat([co_ok, co_venc, te_ok], ignore_index=True)

    # ---------- solver ----------
    def otimizar_para_equipe(self) -> Solucao:
        candidatos = self._ordenar_por_prioridade()

        selecionados: List[Dict] = []
        times_fim: List[pd.Timestamp] = []

        # posição atual é a base
        atual = self.base
        tempo_total_min = 0

        while len(selecionados) < self.limite and not candidatos.empty:
            melhor_i = None
            melhor_custo = float("inf")

            for i, row in candidatos.iterrows():
                if pd.isna(row["latitude"]) or pd.isna(row["longitude"]):
                    continue
                alvo = (float(row["longitude"]), float(row["latitude"]))

                M = self._dur_matrix_minutes([atual, alvo, self.base])
                desloc = M[0][1]               # atual -> alvo
                volta = M[1][2] if self.return_to_depot else 0.0

                te_min = int(row["te"])
                td_min = int(row.get("td") or 0)
                custo_servico = te_min + td_min

                novo_total = tempo_total_min + desloc + custo_servico + volta
                if novo_total <= self.hh_limite_min:
                    # leve bônus para comerciais no prazo (puxar antes de vencer)
                    penal = 0.0
                    if row["tipo"] == "comercial" and pd.notna(row["data_venc"]) and row["data_venc"] >= self.turno_ini:
                        penal = -5.0
                    custo = desloc + penal
                    if custo < melhor_custo:
                        melhor_custo = custo
                        melhor_i = i

            if melhor_i is None:
                break

            job = candidatos.loc[melhor_i]
            alvo = (float(job["longitude"]), float(job["latitude"]))
            M = self._dur_matrix_minutes([atual, alvo, self.base])
            desloc = M[0][1]
            volta = M[1][2] if self.return_to_depot else 0.0
            te_min = int(job["te"])
            td_min = int(job.get("td") or 0)
            custo_servico = te_min + td_min

            # início e fim deste serviço
            inicio_job = self.turno_ini + timedelta(minutes=tempo_total_min + desloc)
            fim_job = inicio_job + timedelta(minutes=custo_servico)

            selecionados.append(job.to_dict())
            times_fim.append(fim_job)

            tempo_total_min += desloc + custo_servico
            atual = alvo
            candidatos = candidatos.drop(index=melhor_i)

        # Se nada foi selecionado mas há candidatos, tentar VROOM (pode salvar)
        if not selecionados and not self._ordenar_por_prioridade().empty:
            jobs = []
            for _, r in self._ordenar_por_prioridade().head(self.limite).iterrows():
                if pd.isna(r["latitude"]) or pd.isna(r["longitude"]):
                    continue
                jobs.append(
                    {
                        "id": int(r["numos"]),
                        "location": [float(r["longitude"]), float(r["latitude"])],
                        "service": int(max(0, int(r["te"])) * 60),  # segundos
                    }
                )
            steps = []
            try:
                steps = executar_vroom(start=self.base, end=(self.base if self.return_to_depot else None), jobs=jobs)
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code == 400:
                    self.vroom_400_flag = True
                    print("❌ VROOM 400 Bad Request — fallback guloso ativado.")
                else:
                    sc = e.response.status_code if e.response is not None else "?"
                    print(f"❌ VROOM HTTPError (status {sc}) — fallback guloso ativado.")
                steps = []
            except Exception as e:
                print(f"❌ VROOM falhou ({type(e).__name__}) — fallback guloso ativado.")
                steps = []

            if steps:
                # construir fim_job sequencial simples (sem tempos de deslocamento intra-steps)
                t_cursor = self.turno_ini
                selecionados = []
                times_fim = []
                # mapear job->linha
                pool = self._ordenar_por_prioridade()
                for st in steps:
                    if st.get("type") != "job":
                        continue
                    jid = int(st["job"])
                    row = pool[pool["numos"].astype("int64") == jid]
                    if row.empty:
                        continue
                    r = row.iloc[0].to_dict()
                    te_min = int(max(0, int(r["te"])))
                    fim = t_cursor + timedelta(minutes=te_min)
                    selecionados.append(r)
                    times_fim.append(fim)
                    t_cursor = fim

        # calcular chegada à base
        if selecionados:
            ultimo_fim = max(times_fim)
            if self.return_to_depot:
                M = self._dur_matrix_minutes([(float(selecionados[-1]["longitude"]), float(selecionados[-1]["latitude"])), self.base])
                back = M[0][1] if M and len(M[0]) > 1 else self._haversine_minutes(
                    (float(selecionados[-1]["longitude"]), float(selecionados[-1]["latitude"])), self.base
                )
            else:
                back = 0.0
            chegada_base = ultimo_fim + timedelta(minutes=back)
        else:
            chegada_base = self.turno_ini

        # montar saída
        out = []
        for r, fim in zip(selecionados, times_fim):
            out.append(
                {
                    "equipe": self.equipe["equipe"],
                    "numos": int(r["numos"]),
                    "tipo": r["tipo"],
                    "codserv": r["codserv"],
                    "data_sol": r["data_sol"],
                    "data_venc": r["data_venc"],
                    "latitude": r["latitude"],
                    "longitude": r["longitude"],
                    "te": int(r["te"]),
                    "td": int(r.get("td") or 0),
                    "fim_os": pd.Timestamp(fim),
                }
            )

        fim_estimado = pd.Timestamp(chegada_base)
        return Solucao(atendidos=out, fim_estimado=fim_estimado, chegada_base=fim_estimado, vroom_400=self.vroom_400_flag)
