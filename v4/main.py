# v4/main.py
"""
V4 - Otimização Multi-Veículo com Restrições de Capacidade

NOVIDADES:
- Usa VROOM multi-veículos para otimização global por grupo de turno
- Adiciona restrições de capacidade para equilibrar distribuição:
  * Cada veículo tem capacity=[limite_por_equipe]
  * Cada job tem delivery=[1]
  * Garante que nenhuma equipe pegue mais que o limite
  * Força distribuição espacial mais equilibrada
  * Reduz cruzamentos de rotas naturalmente

BENEFÍCIOS:
- Mais serviços atendidos (todas as equipes trabalham)
- Menos cruzamentos entre rotas
- Distribuição equilibrada de carga
"""
import sys
import os
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple, Set
import math  # necessário para log1p em _score_job

import pandas as pd

# permitir rodar de qualquer pasta
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from v4.data_loader import prepare_equipes_v3, prepare_pendencias_v3
from v4 import config as v4_config
from v2.vroom_client import VroomClient
from v2 import config

RESULTS_DIR = Path("results_v4")
RESULTS_DIR.mkdir(exist_ok=True)

REQUIRED_COLS = [
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
    "chegada_base",
]

def log(msg: str) -> None:
    print(msg, flush=True)

def _ensure_result_schema(df: pd.DataFrame) -> pd.DataFrame:
    for c in REQUIRED_COLS:
        if c not in df.columns:
            df[c] = pd.NA

    dt_cols = [
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
        "chegada_base",
    ]
    for c in dt_cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    df["eta_source"] = df["eta_source"].astype("string")

    ordered = [c for c in REQUIRED_COLS if c in df.columns]
    extras = [c for c in df.columns if c not in ordered]
    return df[ordered + extras]

def _score_job(row: pd.Series, turno_ini: pd.Timestamp) -> float:
    """
    Score de prioridade semelhante ao V3 (tipo, vencimento, tempo pendente, EUSD).
    """
    tipo = (row.get("tipo_serv") or "").strip().lower()

    tempo_espera = float(row.get("tempo_espera", 0) or 0)

    eusd_raw = row.get("EUSD", row.get("eusd", row.get("EUSD_FIO_B", 0)))
    try:
        eusd = float(eusd_raw or 0.0)
    except Exception:
        eusd = 0.0
    eusd_score = math.log1p(eusd) if eusd > 0 else 0.0

    tempo_pendente_dias = 0.0
    datasol = row.get("datasol") or row.get("data_sol")
    if datasol is not None:
        try:
            ds = pd.to_datetime(datasol, errors="coerce")
            if pd.notna(ds) and pd.notna(turno_ini):
                tempo_pendente_dias = max(
                    0.0, (turno_ini - ds).total_seconds() / 86400.0
                )
        except Exception:
            tempo_pendente_dias = 0.0

    urg_venc = 0.0
    if tipo == "comercial":
        dataven = row.get("dataven") or row.get("data_venc")
        if dataven is not None:
            try:
                dv = pd.to_datetime(dataven, errors="coerce")
                if pd.notna(dv) and pd.notna(turno_ini):
                    dias_para_venc = (dv - turno_ini).total_seconds() / 86400.0
                    urg_venc = -dias_para_venc
            except Exception:
                urg_venc = 0.0

    prioridade_base = float(row.get("prioridade", 1) or 1)
    violacao = float(row.get("violacao", 0) or 0)

    if tipo == "comercial":
        score = (
            1.0 * prioridade_base
            + 3.0 * urg_venc
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
        score = (
            1.0 * prioridade_base
            + 1.0 * tempo_pendente_dias
            + 0.8 * eusd_score
            - 0.5 * violacao
        )

    score += 0.001 * tempo_espera
    return float(score)

def _solve_group_vroom(
    eq_group: pd.DataFrame,
    pend_tec_global: pd.DataFrame,
    pend_com_global: pd.DataFrame,
    limite_por_equipe: int,
) -> Tuple[pd.DataFrame, Set[str]]:
    """
    Resolve um grupo de equipes que têm o MESMO inicio_turno usando VROOM multi-veículos.
    
    Se o grupo for muito grande (>6 equipes), divide em sub-grupos para evitar sobrecarga do VROOM.

    Retorna:
      df_result_group: DataFrame com atribuições desse grupo
      assigned_numos: set de numos atribuídos (para remover do backlog)
    """
    if eq_group.empty:
        return pd.DataFrame(), set()

    group_ini = pd.to_datetime(eq_group["inicio_turno"].iloc[0], errors="coerce")
    if pd.isna(group_ini):
        return pd.DataFrame(), set()
    
    # Se grupo muito grande, dividir em sub-grupos
    if len(eq_group) > v4_config.MAX_EQUIPES_POR_SUBGRUPO:
        log(f"   ⚙️  Grupo grande ({len(eq_group)} equipes) - Dividindo em sub-grupos de {MAX_EQUIPES_POR_SUBGRUPO}")
        all_results = []
        all_assigned = set()
        
        for i in range(0, len(eq_group), MAX_EQUIPES_POR_SUBGRUPO):
            sub_group = eq_group.iloc[i:i+MAX_EQUIPES_POR_SUBGRUPO]
            log(f"      Sub-grupo {i//MAX_EQUIPES_POR_SUBGRUPO + 1}: {len(sub_group)} equipes")
            
            df_sub_res, assigned_sub = _solve_group_vroom_single(
                sub_group, pend_tec_global, pend_com_global, limite_por_equipe
            )
            
            if not df_sub_res.empty:
                all_results.append(df_sub_res)
                all_assigned.update(assigned_sub)
                
                # Remove os atribuídos do backlog para os próximos sub-grupos
                if "numos" in pend_tec_global.columns:
                    pend_tec_global = pend_tec_global[
                        ~pend_tec_global["numos"].astype(str).isin(assigned_sub)
                    ]
                if "numos" in pend_com_global.columns:
                    pend_com_global = pend_com_global[
                        ~pend_com_global["numos"].astype(str).isin(assigned_sub)
                    ]
        
        if all_results:
            return pd.concat(all_results, ignore_index=True), all_assigned
        else:
            return pd.DataFrame(), set()
    
    # Grupo pequeno - processar normalmente
    return _solve_group_vroom_single(eq_group, pend_tec_global, pend_com_global, limite_por_equipe)

def _solve_group_vroom_single(
    eq_group: pd.DataFrame,
    pend_tec_global: pd.DataFrame,
    pend_com_global: pd.DataFrame,
    limite_por_equipe: int,
) -> Tuple[pd.DataFrame, Set[str]]:
    """
    Resolve um sub-grupo de equipes usando VROOM multi-veículos (implementação interna).
    """
    if eq_group.empty:
        return pd.DataFrame(), set()

    group_ini = pd.to_datetime(eq_group["inicio_turno"].iloc[0], errors="coerce")
    if pd.isna(group_ini):
        return pd.DataFrame(), set()

    # Pendências elegíveis: datasol <= inicio_turno do grupo
    pool_parts = []

    if not pend_tec_global.empty:
        ds_tec = pd.to_datetime(pend_tec_global["datasol"], errors="coerce")
        pool_parts.append(pend_tec_global[ds_tec <= group_ini])

    if not pend_com_global.empty:
        ds_com = pd.to_datetime(pend_com_global["datasol"], errors="coerce")
        pool_parts.append(pend_com_global[ds_com <= group_ini])

    if not pool_parts:
        return pd.DataFrame(), set()

    pool = pd.concat(pool_parts, ignore_index=True)
    pool = pool.dropna(subset=["latitude", "longitude"])
    if pool.empty:
        return pd.DataFrame(), set()

    # Pré-filtro de performance com limite absoluto para evitar sobrecarga do VROOM
    fator_pool = 3  # Reduzido de 4 para 3
    max_jobs_absoluto = 150  # Limite absoluto para evitar erro 500 no VROOM
    n_veic = len(eq_group)
    max_jobs_calculado = limite_por_equipe * n_veic * fator_pool
    max_jobs = min(max_jobs_calculado, max_jobs_absoluto, len(pool))

    if len(pool) > max_jobs:
        pool = pool.copy()
        pool["__score"] = pool.apply(lambda r: _score_job(r, group_ini), axis=1)
        pool = pool.sort_values("__score", ascending=False).head(max_jobs)
        pool = pool.drop(columns=["__score"])
    
    # Log de debug para diagnóstico
    if len(pool) > 100:
        log(f"   ⚠️  Pool grande: {len(pool)} jobs para {n_veic} veículos (limite absoluto: {max_jobs_absoluto})")

    pool = pool.reset_index(drop=True)
    pool["job_id_vroom"] = pool.index + 1

    # Monta jobs VROOM com delivery=1 para controle de capacidade
    jobs = []
    for idx, row in pool.iterrows():
        try:
            lon = float(row["longitude"])
            lat = float(row["latitude"])
        except Exception:
            continue
        te_raw = row.get("TE", 0)
        if pd.isna(te_raw):
            te_min = 0.0
        else:
            te_min = float(te_raw)
        service_sec = max(int(te_min * 60), 0)
        jobs.append(
            {
                "id": int(pool.at[idx, "job_id_vroom"]),
                "location": [lon, lat],
                "service": service_sec,
                "delivery": [1],  # Cada job consome 1 unidade de capacidade
            }
        )

    if not jobs:
        return pd.DataFrame(), set()

    # Monta veículos VROOM com capacidade limitada
    vehicles = []
    veh_id_to_nome: Dict[int, str] = {}

    for v_id, (_, erow) in enumerate(eq_group.iterrows(), start=1):
        base_lon = erow.get("base_lon")
        base_lat = erow.get("base_lat")
        if pd.isna(base_lon) or pd.isna(base_lat):
            base_lon = config.BASE_LON
            base_lat = config.BASE_LAT

        inicio = pd.to_datetime(erow["inicio_turno"], errors="coerce")
        fim = pd.to_datetime(erow["fim_turno"], errors="coerce")
        if pd.isna(inicio) or pd.isna(fim):
            horizon = 8 * 3600
        else:
            horizon = max(int((fim - inicio).total_seconds()), 0)

        vehicle = {
            "id": v_id,
            "start": [float(base_lon), float(base_lat)],
            "end": [float(base_lon), float(base_lat)],
            "time_window": [0, horizon],
            "capacity": [limite_por_equipe],  # Limite máximo de OS por equipe
        }
        vehicles.append(vehicle)
        veh_id_to_nome[v_id] = str(erow["nome"])

    # Log de debug do payload
    log(f"   📤 Enviando ao VROOM: {len(vehicles)} veículos × {len(jobs)} jobs (cap={limite_por_equipe} cada)")
    
    vc = VroomClient()
    try:
        resp = vc.route_multi(vehicles, jobs)
    except Exception as e:
        error_msg = str(e)
        if "500" in error_msg:
            log(f"💥 VROOM sobrecarga (500): {len(vehicles)} veículos, {len(jobs)} jobs - Payload muito grande!")
            log(f"   💡 Sugestão: Reduza --limite ou ajuste max_jobs_absoluto no código")
        else:
            log(f"💥 Falha VROOM multi-veículos para grupo {group_ini}: {e}")
        return pd.DataFrame(), set()

    routes = resp.get("routes", [])
    if not routes:
        log(f"⚠️ VROOM não retornou rotas para grupo {group_ini}")
        return pd.DataFrame(), set()

    job_to_equipe: Dict[int, str] = {}
    job_to_arrival: Dict[int, pd.Timestamp] = {}
    job_to_fim_turno: Dict[int, pd.Timestamp] = {}

    for route in routes:
        v_id = route.get("vehicle")
        equipe_nome = veh_id_to_nome.get(v_id, "N/D")
        steps = route.get("steps", [])
        route_end_arr_s = route.get("arrival")
        end_dt = (
            group_ini + pd.to_timedelta(int(route_end_arr_s), unit="s")
            if route_end_arr_s is not None
            else pd.NaT
        )
        for st in steps:
            if st.get("type") == "job":
                jid = int(st.get("job"))
                arr_s = st.get("arrival")
                if arr_s is None:
                    continue
                arr_dt = group_ini + pd.to_timedelta(int(arr_s), unit="s")
                job_to_equipe[jid] = equipe_nome
                job_to_arrival[jid] = arr_dt
                job_to_fim_turno[jid] = end_dt

    if not job_to_equipe:
        return pd.DataFrame(), set()

    assigned_ids = set(job_to_equipe.keys())
    df_assigned = pool[pool["job_id_vroom"].isin(assigned_ids)].copy()

    df_assigned["equipe"] = df_assigned["job_id_vroom"].map(job_to_equipe)
    df_assigned["dth_chegada_estimada"] = df_assigned["job_id_vroom"].map(job_to_arrival)

    te_series = pd.to_numeric(df_assigned["TE"], errors="coerce").fillna(0.0)
    df_assigned["dth_final_estimada"] = pd.to_datetime(
        df_assigned["dth_chegada_estimada"], errors="coerce"
    ) + pd.to_timedelta(te_series.values, unit="m")

    df_assigned["fim_turno_estimado"] = df_assigned["job_id_vroom"].map(job_to_fim_turno)
    df_assigned["eta_source"] = "VROOM"

    return df_assigned, set(df_assigned["numos"].astype(str))

def simular_v4(
    df_eq: pd.DataFrame,
    df_te: pd.DataFrame,
    df_co: pd.DataFrame,
    limite_por_equipe: int = 15,
    debug: bool = False,
) -> None:
    """
    V4:
    - Usa VROOM multi-veículos para cada grupo de equipes com o MESMO inicio_turno.
    - Mantém backlog entre dias.
    - Mantém regra datasol <= inicio_turno para elegibilidade.
    - Cada numos só é atendida uma vez.
    """

    dias = sorted(pd.to_datetime(df_eq["dt_ref"].dropna().unique()))
    if not dias:
        log("⚠️  Nenhum dia encontrado em Equipes.")
        return

    log(f"\n📆 Simulação V4 de {len(dias)} dias ({dias[0].date()} → {dias[-1].date()})\n")

    pend_tec_global = df_te.copy()
    pend_com_global = df_co.copy()

    pend_tec_global["dt_ref"] = pd.to_datetime(pend_tec_global["dt_ref"], errors="coerce").dt.normalize()
    pend_com_global["dt_ref"] = pd.to_datetime(pend_com_global["dt_ref"], errors="coerce").dt.normalize()

    for i, dia in enumerate(dias, 1):
        log("=" * 120)
        log(f"🗓️  Dia {i}/{len(dias)} — {dia.date()}")

        eq_dia = df_eq[df_eq["dt_ref"] == dia].copy()
        num_equipes = len(eq_dia)
        log(f"👥 Equipes no dia: {num_equipes}")

        if eq_dia.empty:
            log("⚠️  Nenhuma equipe para este dia.")
            continue

        eq_dia = eq_dia.sort_values("inicio_turno")
        ini_turno_min = pd.to_datetime(eq_dia["inicio_turno"], errors="coerce").min()

        # Pendências novas vs backlog (para log)
        if not pend_tec_global.empty:
            ds_tec = pd.to_datetime(pend_tec_global["datasol"], errors="coerce")
            mask_tec_elig = ds_tec <= ini_turno_min
            mask_tec_new = mask_tec_elig & (pend_tec_global["dt_ref"] == dia)
            mask_tec_backlog = mask_tec_elig & (pend_tec_global["dt_ref"] < dia)
            pend_new_tec = mask_tec_new.sum()
            pend_backlog_tec = mask_tec_backlog.sum()
        else:
            pend_new_tec = pend_backlog_tec = 0

        if not pend_com_global.empty:
            ds_com = pd.to_datetime(pend_com_global["datasol"], errors="coerce")
            mask_com_elig = ds_com <= ini_turno_min
            mask_com_new = mask_com_elig & (pend_com_global["dt_ref"] == dia)
            mask_com_backlog = mask_com_elig & (pend_com_global["dt_ref"] < dia)
            pend_new_com = mask_com_new.sum()
            pend_backlog_com = mask_com_backlog.sum()
        else:
            pend_new_com = pend_backlog_com = 0

        total_new = pend_new_tec + pend_new_com
        total_backlog = pend_backlog_tec + pend_backlog_com
        total_pend = total_new + total_backlog

        log(
            f"📦 Pendências no início do dia: total={total_new} "
            f"(Tec={pend_new_tec} | Com={pend_new_com})"
        )
        log(
            f"📦 Pendências backlog: total={total_backlog} "
            f"(Tec={pend_backlog_tec} | Com={pend_backlog_com})"
        )
        log(
            f"📦 Total Pendencias: total={total_pend} "
            f"(Tec={pend_new_tec + pend_backlog_tec} | Com={pend_new_com + pend_backlog_com})"
        )

        atribs_dia: List[pd.DataFrame] = []

        # Agrupa equipes por inicio_turno
        for inicio_turno_val, eq_group in eq_dia.groupby("inicio_turno"):
            eq_group = eq_group.copy()
            log(f"🔁 Grupo inicio_turno = {inicio_turno_val} com {len(eq_group)} equipes")

            df_group_res, assigned_nums = _solve_group_vroom(
                eq_group,
                pend_tec_global,
                pend_com_global,
                limite_por_equipe,
            )

            if df_group_res.empty or not assigned_nums:
                log(f"⚠️ Nenhuma OS atribuída para grupo {inicio_turno_val}")
                continue

            # Atualiza backlog global removendo numos atribuídos
            if "numos" in pend_tec_global.columns:
                pend_tec_global = pend_tec_global[
                    ~pend_tec_global["numos"].astype(str).isin(assigned_nums)
                ]
            if "numos" in pend_com_global.columns:
                pend_com_global = pend_com_global[
                    ~pend_com_global["numos"].astype(str).isin(assigned_nums)
                ]

            # Log de distribuição por equipe no grupo
            distribuicao = df_group_res.groupby("equipe").size().to_dict()
            total_grupo = len(df_group_res)
            log(f"   ✅ Total atribuído no grupo: {total_grupo} OS | Distribuição: {distribuicao}")
            
            # Log detalhado por equipe dentro do grupo
            for nome_eq, df_eq_res in df_group_res.groupby("equipe"):
                ini_turno_eq = pd.to_datetime(
                    eq_group[eq_group["nome"] == nome_eq]["inicio_turno"].iloc[0],
                    errors="coerce",
                )
                qtd = len(df_eq_res)
                num_tec_eq = (df_eq_res["tipo_serv"] == "técnico").sum()
                num_com_eq = (df_eq_res["tipo_serv"] == "comercial").sum()

                # Pendências restantes atendíveis para essa equipe
                rest_tec = rest_com = 0
                if not pend_tec_global.empty:
                    ds_tec_glob = pd.to_datetime(pend_tec_global["datasol"], errors="coerce")
                    rest_tec = (ds_tec_glob <= ini_turno_eq).sum()
                if not pend_com_global.empty:
                    ds_com_glob = pd.to_datetime(pend_com_global["datasol"], errors="coerce")
                    rest_com = (ds_com_glob <= ini_turno_eq).sum()
                rest_tot = rest_tec + rest_com

                log(
                    f"🚚 {nome_eq} | {ini_turno_eq} → {qtd} OS "
                    f"(Tec={num_tec_eq} | Com={num_com_eq}) | "
                    f"📦→ {rest_tot} (Tec={rest_tec} | Com={rest_com})"
                )

            atribs_dia.append(df_group_res)

        if atribs_dia:
            out = _ensure_result_schema(pd.concat(atribs_dia, ignore_index=True))
            RESULTS_DIR.mkdir(exist_ok=True)
            out_file = RESULTS_DIR / f"atribuicoes_{dia.date()}.parquet"
            out.to_parquet(out_file, index=False)
            log(f"📊 {len(out)} registros salvos → {out_file}")

            if debug:
                cols_chk = [
                    "dth_chegada_estimada",
                    "dth_final_estimada",
                    "fim_turno_estimado",
                    "chegada_base",
                ]
                log(
                    "   • "
                    + " | ".join(
                        [f"{c}: {out[c].notna().sum()} preenchidas" for c in cols_chk if c in out.columns]
                    )
                )
                if "eta_source" in out.columns:
                    log(f"   • eta_source: {dict(out['eta_source'].value_counts(dropna=False))}")
        else:
            log("⚠️ Nenhum registro atribuído neste dia.")

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limite", type=int, default=15, help="Limite máximo de OS por equipe")
    parser.add_argument("--debug", action="store_true", help="Imprimir estatísticas adicionais")
    args = parser.parse_args()

    log("=" * 120)
    log(f"🚀 Simulação V4 iniciada às {datetime.now():%H:%M:%S}")

    df_eq = prepare_equipes_v3()
    df_te, df_co = prepare_pendencias_v3()

    simular_v4(df_eq, df_te, df_co, limite_por_equipe=args.limite, debug=args.debug)

    log("\n✅ PROCESSO V4 FINALIZADO COM SUCESSO!")
    log(f"📂 Resultados em: {RESULTS_DIR.resolve()}")

if __name__ == "__main__":
    main()
