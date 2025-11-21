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


def _tem_pendencias_atendiveis(pend_tec_dia: pd.DataFrame, pend_com_dia: pd.DataFrame, ini_turno_min: pd.Timestamp) -> bool:
    """
    Verifica rapidamente se ainda existem OS com datasol <= menor início de turno do dia.
    Isso evita tentar rodar rodadas se só restaram OS "futuras".
    """
    if not pend_tec_dia.empty:
        ds_tec = pd.to_datetime(pend_tec_dia["datasol"], errors="coerce")
        if (ds_tec <= ini_turno_min).any():
            return True
    if not pend_com_dia.empty:
        ds_com = pd.to_datetime(pend_com_dia["datasol"], errors="coerce")
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
    - Enquanto houver OS atendíveis (datasol <= inicio_turno_min) e alguma equipe tiver capacidade,
      o algoritmo tenta atribuir OS (em várias rodadas).
    - Deslocamento prioritário via VROOM; fallback OSRM; último recurso Haversine.
    """

    dias = sorted(pd.to_datetime(df_eq["dt_ref"].dropna().unique()))
    if not dias:
        log("⚠️  Nenhum dia encontrado em Equipes.")
        return

    log(f"\n📆 Simulação V3 de {len(dias)} dias ({dias[0].date()} → {dias[-1].date()})\n")

    pend_tec_global = df_te.copy()
    pend_com_global = df_co.copy()

    for i, dia in enumerate(dias, 1):
        log("=" * 120)
        log(f"🗓️  Dia {i}/{len(dias)} — {dia.date()}")

        eq_dia = df_eq[df_eq["dt_ref"] == dia].copy()
        num_equipes = len(eq_dia)
        log(f"👥 Equipes no dia: {num_equipes}")

        if eq_dia.empty:
            log("⚠️  Nenhuma equipe para este dia.")
            continue

        # snapshot de pendências do dia (antes de qualquer atribuição)
        pend_tec_dia = pend_tec_global[pend_tec_global["dt_ref"] == dia].copy()
        pend_com_dia = pend_com_global[pend_com_global["dt_ref"] == dia].copy()

        num_pend_tec = len(pend_tec_dia)
        num_pend_com = len(pend_com_dia)
        num_pend_total = num_pend_tec + num_pend_com

        log(
            f"📦 Pendências no início do dia: total={num_pend_total} "
            f"(Tec={num_pend_tec} | Com={num_pend_com})"
        )

        atribs_dia: List[pd.DataFrame] = []

        # ordenar equipes por início de turno para processar em ordem temporal
        eq_dia = eq_dia.sort_values("inicio_turno")
        # mapa equipe -> OS já atribuídas (para respeitar limite diário)
        atrib_por_equipe: Dict[str, int] = {
            str(row["nome"]): 0 for _, row in eq_dia.iterrows()
        }

        # menor início de turno do dia (para teste rápido de datasol <= inicio_turno)
        ini_turno_min = pd.to_datetime(eq_dia["inicio_turno"], errors="coerce").min()

        rodada = 0
        while True:
            rodada += 1
            any_assigned_this_round = False

            # condição de parada: não há mais OS atendíveis para este dia
            if not _tem_pendencias_atendiveis(pend_tec_dia, pend_com_dia, ini_turno_min):
                break

            # condição de parada: nenhuma equipe tem capacidade restante
            if all(atrib_por_equipe[nome] >= limite_por_equipe for nome in atrib_por_equipe):
                break

            log(f"🔁 Rodada {rodada} de atribuição no dia {dia.date()}")

            for _, equipe_row in eq_dia.iterrows():
                nome_eq = str(equipe_row.get("nome", "N/D"))
                ja_atribuidas = atrib_por_equipe.get(nome_eq, 0)
                capacidade_restante = limite_por_equipe - ja_atribuidas

                if capacidade_restante <= 0:
                    # esta equipe já atingiu seu limite diário
                    continue

                # se não há mais pendências no dia, podemos sair
                if pend_tec_dia.empty and pend_com_dia.empty:
                    break

                mh = MetaHeuristicaV3(equipe_row, pend_tec_dia, pend_com_dia, capacidade_restante)
                try:
                    sol = mh.otimizar_para_equipe()
                except Exception as e:
                    log(f"💥 Falha na equipe {nome_eq}: {e}")
                    continue

                if not sol or not isinstance(sol.get("resp"), pd.DataFrame) or sol["resp"].empty:
                    # nada atribuído para esta equipe nesta rodada
                    continue

                df_resp = sol["resp"].copy()
                # chegada_base = fim_turno_estimado
                if "fim_turno_estimado" in df_resp.columns:
                    df_resp["chegada_base"] = df_resp["fim_turno_estimado"]

                df_resp = _ensure_result_schema(df_resp)
                qtd = len(df_resp)
                log(f"🚚 Equipe {nome_eq} (rodada {rodada}) → {qtd} serviços atribuídos")

                atribs_dia.append(df_resp)
                any_assigned_this_round = True
                atrib_por_equipe[nome_eq] = ja_atribuidas + qtd

                # Remover OS atribuídas (numos) dos pools do dia e globais
                if "numos" in df_resp.columns:
                    atendidos = (
                        df_resp["numos"]
                        .dropna()
                        .astype(str)
                        .unique()
                        .tolist()
                    )

                    if atendidos:
                        # remover dos pools do DIA
                        if "numos" in pend_tec_dia.columns:
                            pend_tec_dia = pend_tec_dia[
                                ~pend_tec_dia["numos"].astype(str).isin(atendidos)
                            ]
                        if "numos" in pend_com_dia.columns:
                            pend_com_dia = pend_com_dia[
                                ~pend_com_dia["numos"].astype(str).isin(atendidos)
                            ]

                        # remover dos pools GLOBAIS (para não voltar em outros dias)
                        if "numos" in pend_tec_global.columns:
                            pend_tec_global = pend_tec_global[
                                ~pend_tec_global["numos"].astype(str).isin(atendidos)
                            ]
                        if "numos" in pend_com_global.columns:
                            pend_com_global = pend_com_global[
                                ~pend_com_global["numos"].astype(str).isin(atendidos)
                            ]

                # LOG solicitado: pendências restantes para o dia após esta equipe
                rest_tec = len(pend_tec_dia)
                rest_com = len(pend_com_dia)
                rest_tot = rest_tec + rest_com
                log(
                    f"📦 Pendências restantes após equipe {nome_eq}: "
                    f"total={rest_tot} (Tec={rest_tec} | Com={rest_com})"
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


if __name__ == "__main__":
    main()