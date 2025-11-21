import sys, os
import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime

# permitir rodar de qualquer pasta
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from v2.data_loader import prepare_equipes, prepare_pendencias
from v2.optimization import MetaHeuristica

RESULTS_DIR = Path("results")
RESULTS_DIR.mkdir(exist_ok=True)

REQUIRED_COLS = [
    "tipo_serv","numos","datasol","dataven","datater_trab","TD","TE",
    "equipe","dthaps_ini","dthaps_fim_ajustado","inicio_turno","fim_turno",
    "dth_chegada_estimada","dth_final_estimada","fim_turno_estimado","eta_source",
    "base_lon","base_lat"
]

def log(msg): print(msg, flush=True)

def _ensure_result_schema(df: pd.DataFrame) -> pd.DataFrame:
    # garante todas as colunas e dtypes estáveis
    for c in REQUIRED_COLS:
        if c not in df.columns:
            df[c] = pd.NA

    # normalizar datas
    dt_cols = [
        "datasol","dataven","datater_trab","dthaps_ini","dthaps_fim_ajustado",
        "inicio_turno","fim_turno","dth_chegada_estimada","dth_final_estimada","fim_turno_estimado"
    ]
    for c in dt_cols:
        df[c] = pd.to_datetime(df[c], errors="coerce")

    # 'eta_source' é texto
    df["eta_source"] = df["eta_source"].astype("string")
    return df[[c for c in REQUIRED_COLS] + [c for c in df.columns if c not in REQUIRED_COLS]]

def simular(df_eq, df_te, df_co, limite_por_equipe=15, debug=False):
    # dias vindos do DT_REF de Equipes (já normalizado)
    dias = sorted(pd.to_datetime(df_eq["dt_ref"].dropna().unique()))
    if not dias:
        log("⚠️  Nenhum dia encontrado.")
        return

    log(f"\n📆 Simulação de {len(dias)} dias ({dias[0].date()} → {dias[-1].date()})\n")

    for i, dia in enumerate(dias, 1):
        log("=" * 100)
        log(f"🗓️  Dia {i}/{len(dias)} — {dia.date()}")

        eq_dia = df_eq[df_eq["dt_ref"] == dia]
        log(f"👥 Equipes no dia: {len(eq_dia)}")

        atribs = []
        for _, equipe_row in eq_dia.iterrows():
            nome = equipe_row.get("nome", "N/D")
            try:
                mh  = MetaHeuristica(equipe_row, df_te, df_co, limite_por_equipe)
                sol = mh.otimizar_para_equipe()
                if sol and isinstance(sol.get("resp"), pd.DataFrame) and not sol["resp"].empty:
                    df_resp = _ensure_result_schema(sol["resp"].copy())
                    log(f"🚚 Equipe {nome} → {len(df_resp)} serviços atribuídos")
                    atribs.append(df_resp)
                else:
                    log(f"⚠️  {nome}: Nenhuma OS atribuída")
            except Exception as e:
                log(f"💥 Falha na equipe {nome}: {e}")

        if atribs:
            out = _ensure_result_schema(pd.concat(atribs, ignore_index=True))
            out_file = RESULTS_DIR / f"atribuicoes_{dia.date()}.parquet"
            out.to_parquet(out_file, index=False)
            log(f"📊 {len(out)} registros salvos → {out_file.name}")
            if debug:
                cols_chk = ["dth_chegada_estimada","dth_final_estimada","fim_turno_estimado"]
                log("   • " + " | ".join([f"{c}: {out[c].notna().sum()} preenchidas" for c in cols_chk]))
                log(f"   • eta_source: {dict(out['eta_source'].value_counts(dropna=False))}")
        else:
            log("⚠️ Nenhum registro atribuído neste dia.")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limite", type=int, default=15)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    log("=" * 100)
    log(f"🚀 Simulação iniciada às {datetime.now():%H:%M:%S}")

    try:
        df_eq = prepare_equipes()
        df_te, df_co = prepare_pendencias()
    except Exception as e:
        log(f"💥 Erro ao carregar dataframes: {e}")
        raise

    if args.debug:
        log(f"🔎 Colunas EQUIPES: {list(df_eq.columns)}")
        log(f"🔎 Colunas TEC:     {list(df_te.columns)}")
        log(f"🔎 Colunas COM:     {list(df_co.columns)}")

    simular(df_eq, df_te, df_co, limite_por_equipe=args.limite, debug=args.debug)

    log("\n✅ PROCESSO FINALIZADO COM SUCESSO!")
    log(f"📄 Resultados em: {RESULTS_DIR}")

if __name__ == "__main__":
    main()
