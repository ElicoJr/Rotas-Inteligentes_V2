# main.py
from __future__ import annotations
import os
from typing import List, Dict, Tuple

import pandas as pd

from data_loader import prepare_equipes, prepare_pendencias
from optimization import MetaHeuristica, Solucao


RESULTS_DIR = "results"
os.makedirs(RESULTS_DIR, exist_ok=True)


def _pendentes_para_equipe(te_all: pd.DataFrame, co_all: pd.DataFrame, turno_ini: pd.Timestamp) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Filtra pendências cujo data_sol <= turno_ini da equipe.
    """
    te_ok = te_all[te_all["data_sol"] <= turno_ini].copy()
    co_ok = co_all[co_all["data_sol"] <= turno_ini].copy()
    return te_ok, co_ok


def simular(
    df_eq_raw: pd.DataFrame,
    df_te_raw: pd.DataFrame,
    df_co_raw: pd.DataFrame,
    *,
    limite_por_equipe: int = 15,
    return_to_depot: bool = True,
    descartar_co_vencido_antes_da_solicitacao: bool = True,
) -> None:
    # normalizações
    equipes = prepare_equipes(df_eq_raw)
    pend_te, pend_co = prepare_pendencias(
        df_te_raw, df_co_raw, descartar_comerciais_vencidos_antes_da_solicitacao=descartar_co_vencido_antes_da_solicitacao
    )

    # Simulação por dia
    dias = sorted(equipes["dt_ref"].dropna().unique())
    registros: List[Dict] = []

    print("\n====================================================================================================\n")
    for dia in dias:
        dia = pd.to_datetime(dia)
        print(f"🗓️  Processando dia: {dia}")

        eq_dia = equipes[equipes["turno_ini"].dt.normalize() == dia].copy()
        eq_dia = eq_dia.sort_values("turno_ini")
        print(f"👥 Equipes no dia: {len(eq_dia)}")

        vroom400_count = 0
        atribuicoes_dia = 0
        atrib_te = 0
        atrib_co = 0

        for _, equipe in eq_dia.iterrows():
            # pendentes elegíveis neste momento (data_sol <= início do turno)
            te_ok, co_ok = _pendentes_para_equipe(pend_te, pend_co, equipe["turno_ini"])

            pend_tec_count = len(te_ok)
            pend_com_count = len(co_ok)

            mh = MetaHeuristica(
                equipe_row=equipe,
                te_df=te_ok,
                co_df=co_ok,
                limite_por_equipe=limite_por_equipe,
                return_to_depot=return_to_depot,
            )
            sol: Solucao = mh.otimizar_para_equipe()

            # remover do pool global os que foram atendidos
            atendidos_ids = [r["numos"] for r in sol.atendidos]
            if atendidos_ids:
                pend_te = pend_te[~pend_te["numos"].astype("int64").isin(atendidos_ids)]
                pend_co = pend_co[~pend_co["numos"].astype("int64").isin(atendidos_ids)]

            n_tec = sum(1 for r in sol.atendidos if r["tipo"] == "tecnico")
            n_com = sum(1 for r in sol.atendidos if r["tipo"] == "comercial")
            atribuicoes_dia += (n_tec + n_com)
            atrib_te += n_tec
            atrib_co += n_com
            vroom400_count += (1 if sol.vroom_400 else 0)

            # logging da equipe
            if sol.atendidos:
                print(
                    f"🚚 Equipe: {equipe['equipe']:<8} | 🕒 Inicio: {equipe['turno_ini']} | 🕒 Fim: {equipe['turno_fim']} "
                    f"| Pendentes agora: Tec={pend_tec_count} | Com={pend_com_count} "
                    f"| ✅ Atribuídos: {n_tec + n_com} (Tec={n_tec} | Com={n_com}) | 🕒 Fim estimado: {sol.fim_estimado}"
                )
            else:
                if sol.vroom_400:
                    print(f"⚠️  {equipe['equipe']}: Nenhuma OS atribuída (VROOM 400 — usado fallback, nada viável dentro do HH).")
                else:
                    print(f"⚠️  {equipe['equipe']}: Nenhuma OS atribuída")

            # acumula registros finais
            registros.extend(sol.atendidos)

        # resumo do dia
        pend_tec_total = len(pend_te)
        pend_com_total = len(pend_co)
        print("-" * 100)
        print(f"📊 RESUMO DO DIA {dia.date()}")
        print(f"   ✔ Serviços Atribuídos hoje     : {atribuicoes_dia}")
        print(f"     ↳ Técnicos                   : {atrib_te}")
        print(f"     ↳ Comerciais                 : {atrib_co}")
        if vroom400_count:
            print(f"   ❗ Ocorrências de VROOM 400     : {vroom400_count}")
        print(f"   🔁 Pendentes para o próximo dia: {pend_tec_total + pend_com_total} (Tec={pend_tec_total} | Com={pend_com_total})")
        print("-" * 100)
        print("\n====================================================================================================\n")

    # salvar resultado final
    if registros:
        df_out = pd.DataFrame(registros)
        out_path = os.path.join(RESULTS_DIR, "atribuicoes.parquet")
        df_out.to_parquet(out_path, index=False)
        print(f"📄 Arquivo gerado: {out_path}")
    else:
        print("⚠️ Nenhuma atribuição realizada. Verifique filtros e dados.")


if __name__ == "__main__":
    # Carrega as fontes originais
    df_eq = pd.read_parquet("data/equipes.parquet")
    df_te = pd.read_parquet("data/atendTec.parquet")
    df_co = pd.read_parquet("data/ServCom.parquet")

    simular(
        df_eq,
        df_te,
        df_co,
        limite_por_equipe=15,
        return_to_depot=True,
        descartar_co_vencido_antes_da_solicitacao=True,
    )
