# data_loader.py
from __future__ import annotations
import os
from typing import Tuple
import pandas as pd


def _ensure_results_dir() -> None:
    os.makedirs("results", exist_ok=True)


def prepare_equipes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza colunas de equipes e expõe:
      equipe, dt_ref (date), turno_ini, turno_fim, pausa_ini, pausa_fim.
    Preferimos DTHAPS_FIM_AJUSTADO como fim do turno quando existir.
    """
    _ensure_results_dir()
    df = df.copy()
    df.columns = df.columns.str.lower()

    # datas
    df["turno_ini"] = pd.to_datetime(df["data_inicio_turno"], errors="coerce")
    if "dthaps_fim_ajustado" in df.columns:
        df["turno_fim"] = pd.to_datetime(df["dthaps_fim_ajustado"], errors="coerce")
    else:
        df["turno_fim"] = pd.to_datetime(df["data_fim_turno"], errors="coerce")

    # pausas (podem ser NaT)
    df["pausa_ini"] = pd.to_datetime(df.get("dthpausa_ini"), errors="coerce")
    df["pausa_fim"] = pd.to_datetime(df.get("dthpausa_fim"), errors="coerce")

    # referência diária (se existir DT_REF usamos; senão, do turno_ini)
    if "dt_ref" in df.columns:
        df["dt_ref"] = pd.to_datetime(df["dt_ref"], errors="coerce").dt.normalize()
    else:
        df["dt_ref"] = df["turno_ini"].dt.normalize()

    out = df[["equipe", "dt_ref", "turno_ini", "turno_fim", "pausa_ini", "pausa_fim"]].dropna(
        subset=["equipe", "turno_ini", "turno_fim"]
    )
    out = out.sort_values(["turno_ini", "equipe"]).reset_index(drop=True)
    out.to_parquet("results/equipes_norm.parquet", index=False)
    return out


def _normalize_latlon(df: pd.DataFrame) -> pd.DataFrame:
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    return df


def prepare_pendencias(
    df_te: pd.DataFrame,
    df_co: pd.DataFrame,
    *,
    descartar_comerciais_vencidos_antes_da_solicitacao: bool = True,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Retorna dois dataframes harmonizados:
      - Técnicos (tipo="tecnico"): sem data_venc.
      - Comerciais (tipo="comercial"): com data_venc, priorizados por prazo.
    Colunas harmonizadas:
      numos, tipo, data_sol, data_venc, latitude, longitude, te, td, codserv, equipe, eusd, eusd_fio_b
    Observações:
      - TE já está em minutos (mantemos).
      - TD se existir é somado no custo de serviço (minutos).
    """
    _ensure_results_dir()

    # ---------------- Técnicos ----------------
    te = df_te.copy()
    te.columns = te.columns.str.lower()
    te["numos"] = te["numos"]
    te["tipo"] = "tecnico"
    te["data_sol"] = pd.to_datetime(te["dh_inicio"], errors="coerce")
    te["data_venc"] = pd.NaT  # técnicos não possuem prazo
    te["te"] = pd.to_numeric(te.get("te"), errors="coerce").fillna(0).astype(int)  # minutos
    te["td"] = pd.to_numeric(te.get("td"), errors="coerce").fillna(0).astype(int)  # min extras se houver
    te["codserv"] = pd.NA
    te["equipe"] = te.get("equipe")
    te["eusd"] = te.get("eusd")
    te["eusd_fio_b"] = te.get("eusd_fio_b")
    # renomear lat/lon se já estão
    if "latitude" not in te.columns and "noy" in te.columns:
        te["latitude"] = te["noy"]
    if "longitude" not in te.columns and "nox" in te.columns:
        te["longitude"] = te["nox"]
    te = _normalize_latlon(te)

    # ---------------- Comerciais ----------------
    co = df_co.copy()
    co.columns = co.columns.str.lower()
    co["numos"] = co["numos"]
    co["tipo"] = "comercial"
    co["data_sol"] = pd.to_datetime(co["data_sol"], errors="coerce")
    co["data_venc"] = pd.to_datetime(co["data_venc"], errors="coerce")
    if descartar_comerciais_vencidos_antes_da_solicitacao:
        co = co[(co["data_venc"].isna()) | (co["data_venc"] >= co["data_sol"])]

    co["te"] = pd.to_numeric(co.get("te"), errors="coerce").fillna(0).astype(int)
    co["td"] = pd.to_numeric(co.get("td"), errors="coerce").fillna(0).astype(int)
    co["codserv"] = pd.to_numeric(co.get("codserv"), errors="coerce")
    co["equipe"] = co.get("equipe")
    co["eusd"] = co.get("eusd")
    co["eusd_fio_b"] = co.get("eusd_fio_b")
    co = _normalize_latlon(co)

    keep = [
        "numos",
        "tipo",
        "data_sol",
        "data_venc",
        "latitude",
        "longitude",
        "te",
        "td",
        "codserv",
        "equipe",
        "eusd",
        "eusd_fio_b",
    ]
    te = te.reindex(columns=keep, fill_value=pd.NA)
    co = co.reindex(columns=keep, fill_value=pd.NA)

    # filtrar coords válidas
    te = te.dropna(subset=["latitude", "longitude", "data_sol"])
    co = co.dropna(subset=["latitude", "longitude", "data_sol"])

    te.to_parquet("results/pend_te.parquet", index=False)
    co.to_parquet("results/pend_co.parquet", index=False)
    return te, co
