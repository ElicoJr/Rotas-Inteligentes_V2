# v3/main.py
import sys
import os
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Dict

import pandas as pd

# permitir rodar de qualquer pasta
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from v3.data_loader import prepare_equipes_v3, prepare_pendencias_v3
from v3.optimization import MetaHeuristicaV3


RESULTS_DIR = Path("results_v3")
RESULTS_DIR.mkdir(exist_ok=True)

# colunas mínimas que queremos garantir no resultado
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
    """Garante um layout estável para o V3."""
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


def _tem_pendencias_atendiveis(
    pend_tec_global: pd.DataFrame,
    pend_com_global: pd.DataFrame,
    ini_turno_min: pd.Timestamp,
) -> bool:
    """Retorna True se ainda existem OS com datasol <= menor início de turno do dia."""
    if not pend_tec_global.empty:
        ds_tec = pd.to_datetime(pend_tec_global["datasol"], errors="coerce")
        if (ds_tec <= ini_turno_min).any():
            return True
    if not pend_com_global.empty:
        ds_com = pd.to_datetime(pend_com_global["datasol"], errors="coerce")
        if (ds_com <= ini_turno_min).any():
            return True
    return False


def simular_v3(
    df_eq: pd.DataFrame,
    df_te: pd.DataFrame,
    df_co: pd.DataFrame,
    limite_por_equipe: int = 15,
    debug: bool = False,
) -> None:
    """Simulação V3:
    - Equipe inicia/termina na própria base (base_lon/base_lat).
    - Cada OS (numos) é atendida no máximo uma vez.
    - Backlog: OS não atribuídas com datasol <= inicio_turno_min são herdadas para os próximos dias.
    - Enquanto houver OS atendíveis e alguma equipe tiver capacidade, o algoritmo tenta atribuir OS (rodadas).
    - Deslocamento prioritário via VROOM; fallback OSRM; último recurso Haversine.
    """

    dias = sorted(pd.to_datetime(df_eq["dt_ref"].dropna().unique()))
    if not dias:
        log("⚠️  Nenhum dia encontrado em Equipes.")
        return

    log(f"\n📆 Simulação V3 de {len(dias)} dias ({dias[0].date()} → {dias[-1].date()})\n")

    pend_tec_global = df_te.copy()
    pend_com_global = df_co.copy()

    # Normaliza dt_ref nas pendências para comparação com dia
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

        # ordenar equipes por início de turno para processar em ordem temporal
        eq_dia = eq_dia.sort_values("inicio_turno")
        ini_turno_min = pd.to_datetime(eq_dia["inicio_turno"], errors="coerce").min()

        # --- Cálculo de pendências novas, backlog e total (apenas para log) ---
        # novas do dia (dt_ref == dia) e datasol <= inicio_turno_min
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

        # Logs no formato solicitado
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

        # mapa equipe -> OS já atribuídas (para respeitar limite diário)
        atrib_por_equipe: Dict[str, int] = {
            str(row["nome"]): 0 for _, row in eq_dia.iterrows()
        }

        rodada = 0
        while True:
            rodada += 1
            any_assigned_this_round = False

            # condição de parada: não há mais OS atendíveis para este dia
            if not _tem_pendencias_atendiveis(pend_tec_global, pend_com_global, ini_turno_min):
                break

            # condição de parada: nenhuma equipe tem capacidade restante
            if all(atrib_por_equipe[nome] >= limite_por_equipe for nome in atrib_por_equipe):
                break

            log(f"🔁 Rodada {rodada} de atribuição no dia {dia.date()}")

            for _, equipe_row in eq_dia.iterrows():
                nome_eq = str(equipe_row.get("nome", "N/D"))
                ini_turno_eq = pd.to_datetime(equipe_row.get("inicio_turno"), errors="coerce")
                ja_atribuidas = atrib_por_equipe.get(nome_eq, 0)
                capacidade_restante = limite_por_equipe - ja_atribuidas

                if capacidade_restante <= 0:
                    continue

                # snapshot atual das pendências globais para esta equipe
                pend_tec_dia = pend_tec_global.copy()
                pend_com_dia = pend_com_global.copy()

                mh = MetaHeuristicaV3(equipe_row, pend_tec_dia, pend_com_dia, capacidade_restante)
                try:
                    sol = mh.otimizar_para_equipe()
                except Exception as e:
                    log(f"💥 Falha na equipe {nome_eq}: {e}")
                    continue

                if not sol or not isinstance(sol.get("resp"), pd.DataFrame) or sol["resp"].empty:
                    continue

                df_resp = sol["resp"].copy()
                # chegada_base = fim_turno_estimado
                if "fim_turno_estimado" in df_resp.columns:
                    df_resp["chegada_base"] = df_resp["fim_turno_estimado"]

                df_resp = _ensure_result_schema(df_resp)
                qtd = len(df_resp)

                # contagem por tipo de serviço (técnico/comercial)
                num_tec_eq = (
                    (df_resp["tipo_serv"] == "técnico").sum()
                    if "tipo_serv" in df_resp.columns
                    else 0
                )
                num_com_eq = (
                    (df_resp["tipo_serv"] == "comercial").sum()
                    if "tipo_serv" in df_resp.columns
                    else 0
                )

                # LOG no formato solicitado para equipe:
                # 🚚 PVLPL46 | {inicio_turno} → 15 OS (Tec=7 | Com=8) | 📦→ 22 (Tec=7 | Com=15)
                atribs_dia.append(df_resp)
                any_assigned_this_round = True
                atrib_por_equipe[nome_eq] = ja_atribuidas + qtd

                # Remover OS atribuídas (numos) dos pools globais
                rest_tec = rest_com = 0
                if "numos" in df_resp.columns:
                    atendidos = (
                        df_resp["numos"]
                        .dropna()
                        .astype(str)
                        .unique()
                        .tolist()
                    )

                    if atendidos:
                        if "numos" in pend_tec_global.columns:
                            pend_tec_global = pend_tec_global[
                                ~pend_tec_global["numos"].astype(str).isin(atendidos)
                            ]
                        if "numos" in pend_com_global.columns:
                            pend_com_global = pend_com_global[
                                ~pend_com_global["numos"].astype(str).isin(atendidos)
                            ]

                rest_tec = len(pend_tec_global)
                rest_com = len(pend_com_global)
                rest_tot = rest_tec + rest_com

                log(
                    f"🚚 {nome_eq} | {ini_turno_eq} → {qtd} OS "
                    f"(Tec={num_tec_eq} | Com={num_com_eq}) | "
                    f"📦→ {rest_tot} (Tec={rest_tec} | Com={rest_com})"
                )

            # se, após percorrer todas as equipes nesta rodada, ninguém recebeu OS,
            # quer dizer que as OS restantes (se existirem) são inviáveis para todas as equipes
            if not any_assigned_this_round:
                break

        if atribs_dia:
            out = _ensure_result_schema(pd.concat(atribs_dia, ignore_index=True))
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
    log(f"🚀 Simulação V3 iniciada às {datetime.now():%H:%M:%S}")

    try:
        df_eq = prepare_equipes_v3()
        df_te, df_co = prepare_pendencias_v3()
    except Exception as e:
        log(f"💥 Erro ao carregar dataframes: {e}")
        raise

    simular_v3(df_eq, df_te, df_co, limite_por_equipe=args.limite, debug=args.debug)

    log("\n✅ PROCESSO V3 FINALIZADO COM SUCESSO!")
    log(f"📂 Resultados em: {RESULTS_DIR.resolve()}")
    log(f"🚀 Simulação V3 finalizada às {datetime.now():%H:%M:%S}")


if __name__ == "__main__":
    main()