import sys
import os
import argparse
from pathlib import Path
from datetime import datetime
from typing import List

import pandas as pd

# permitir rodar de qualquer pasta
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from v3.data_loader import prepare_equipes_v3, prepare_pendencias_v3
from v2.optimization import MetaHeuristica


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
    """Garante um layout estável para o V3.

    Mantém todas as colunas do V2 e adiciona campos de pausa e chegada_base.
    """
    for c in REQUIRED_COLS:
        if c not in df.columns:
            df[c] = pd.NA

    # normalizar datas relevantes
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

    # garante ordem base, mantendo colunas extras ao final
    ordered = [c for c in REQUIRED_COLS if c in df.columns]
    extras = [c for c in df.columns if c not in ordered]
    return df[ordered + extras]


def simular_v3(
    df_eq: pd.DataFrame,
    df_te: pd.DataFrame,
    df_co: pd.DataFrame,
    limite_por_equipe: int = 15,
    debug: bool = False,
) -> None:
    """Simulação V3 com regras:
    - Equipes começam e terminam na base fixa.
    - Cada OS é atribuída no máximo uma vez.
    - Pausa da equipe respeitada (sem deslocamento/serviço no intervalo).
    """

    # dias vindos do DT_REF das equipes
    dias = sorted(pd.to_datetime(df_eq["dt_ref"].dropna().unique()))
    if not dias:
        log("⚠️  Nenhum dia encontrado em Equipes.")
        return

    log(f"\n📆 Simulação V3 de {len(dias)} dias ({dias[0].date()} → {dias[-1].date()})\n")

    # pools globais de pendências (podem ser multi-dia)
    pend_tec_global = df_te.copy()
    pend_com_global = df_co.copy()

    for i, dia in enumerate(dias, 1):
        log("=" * 120)
        log(f"🗓️  Dia {i}/{len(dias)} — {dia.date()}")

        # equipes do dia
        eq_dia = df_eq[df_eq["dt_ref"] == dia].copy()
        log(f"👥 Equipes no dia: {len(eq_dia)}")

        if eq_dia.empty:
            log("⚠️  Nenhuma equipe para este dia.")
            continue

        # pendências disponíveis para o dia (antes de qualquer atribuição)
        pend_tec_dia = pend_tec_global[pend_tec_global["dt_ref"] == dia].copy()
        pend_com_dia = pend_com_global[pend_com_global["dt_ref"] == dia].copy()

        atribs_dia: List[pd.DataFrame] = []

        for _, equipe_row in eq_dia.iterrows():
            nome_eq = equipe_row.get("nome", "N/D")

            # criar meta-heurística com o snapshot atual de pendências do dia
            mh = MetaHeuristica(equipe_row, pend_tec_dia, pend_com_dia, limite_por_equipe)
            try:
                sol = mh.otimizar_para_equipe()
            except Exception as e:
                log(f"💥 Falha na equipe {nome_eq}: {e}")
                continue

            if not sol or not isinstance(sol.get("resp"), pd.DataFrame) or sol["resp"].empty:
                log(f"⚠️  {nome_eq}: Nenhuma OS atribuída")
                continue

            df_resp = sol["resp"].copy()
            # chegada_base == fim_turno_estimado
            if "fim_turno_estimado" in df_resp.columns:
                df_resp["chegada_base"] = df_resp["fim_turno_estimado"]

            df_resp = _ensure_result_schema(df_resp)
            log(f"🚚 Equipe {nome_eq} → {len(df_resp)} serviços atribuídos")
            atribs_dia.append(df_resp)

            # remover OS atribuídas dos pools (dia + global) para garantir exclusividade
            if "numos" in df_resp.columns:
                try:
                    atendidos = (
                        df_resp["numos"]
                        .dropna()
                        .astype("int64", errors="ignore")
                        .astype(str)
                        .unique()
                        .tolist()
                    )
                except Exception:
                    atendidos = []

                if atendidos:
                    # converter numos para string nas pendências para comparação robusta
                    for dname, d in (
                        ("pend_tec_dia", pend_tec_dia),
                        ("pend_com_dia", pend_com_dia),
                        ("pend_tec_global", pend_tec_global),
                        ("pend_com_global", pend_com_global),
                    ):
                        if "numos" in d.columns:
                            mask = ~d["numos"].astype(str).isin(atendidos)
                            locals()[dname] = d[mask]

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
