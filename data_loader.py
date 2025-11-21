import pandas as pd
from pathlib import Path

DATA_DIR = Path("data")

def _detectar_coluna(df, candidatos):
    for c in candidatos:
        if c in df.columns:
            return c
    return None

def prepare_equipes():
    df = pd.read_parquet(DATA_DIR / "equipes.parquet")
    df.columns = df.columns.str.lower()

    # nome da equipe
    col_nome = _detectar_coluna(df, ["nome", "equipe", "cod_equipe", "nome_equipe", "veiculo", "id_equipe"])
    if not col_nome:
        raise KeyError(f"[equipes] Não encontrei coluna de nome. Colunas: {list(df.columns)}")
    df.rename(columns={col_nome: "nome"}, inplace=True)

    # datas de turno
    col_ini = _detectar_coluna(df, ["data_inicio_turno", "inicio_turno", "dt_inicio", "inicio"])
    col_fim = _detectar_coluna(df, ["data_fim_turno", "fim_turno", "dt_fim", "fim"])
    if not col_ini or not col_fim:
        raise KeyError(f"[equipes] Não encontrei colunas de início/fim de turno. Colunas: {list(df.columns)}")
    df["data_inicio_turno"] = pd.to_datetime(df[col_ini], errors="coerce")
    df["data_fim_turno"]     = pd.to_datetime(df[col_fim], errors="coerce")

    # coordenadas
    for col in ["latitude", "longitude"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["nome"] = df["nome"].astype(str)
    return df

def _construir_dt_ref(df):
    col_dh = _detectar_coluna(df, ["dh_inicio", "dh_abertura", "data_abertura", "inicio"])
    if col_dh:
        return pd.to_datetime(df[col_dh], errors="coerce").dt.normalize()

    if "ano" in df.columns and "mes" in df.columns:
        s = pd.to_datetime(
            df["ano"].astype(str) + "-" + df["mes"].astype(str).str.zfill(2) + "-01",
            errors="coerce"
        )
        return s.dt.normalize()

    col_dt = _detectar_coluna(df, ["dt_ref", "data_ref", "data", "dt", "dia", "dt_servico"])
    if col_dt:
        return pd.to_datetime(df[col_dt], errors="coerce").dt.normalize()

    return pd.NaT

def _to_utc_series(series_like):
    s = pd.to_datetime(series_like, errors="coerce")
    try:
        if getattr(s.dtype, "tz", None) is not None:
            return s.dt.tz_convert("UTC")
        else:
            return s.dt.tz_localize("America/Porto_Velho", nonexistent="NaT", ambiguous="NaT").dt.tz_convert("UTC")
    except Exception:
        return pd.to_datetime(series_like, errors="coerce", utc=True)

def prepare_pendencias():
    df_te = pd.read_parquet(DATA_DIR / "atendTec.parquet")
    df_co = pd.read_parquet(DATA_DIR / "ServCom.parquet")

    for df in (df_te, df_co):
        df.columns = df.columns.str.lower()

        df["dt_ref"] = _construir_dt_ref(df)

        col_dhab = _detectar_coluna(df, ["dh_abertura", "data_abertura", "inicio", "hora_abertura", "dh_ini", "dh_inicio"])
        if col_dhab:
            df["dh_abertura"] = _to_utc_series(df[col_dhab])
        else:
            df["dh_abertura"] = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns, UTC]")

        for col in ["latitude", "longitude", "nox", "noy"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if "prioridade" not in df.columns:
            df["prioridade"] = 1

        if "violacao" not in df.columns:
            df["violacao"] = 0.0

        now_utc = pd.Timestamp.now(tz="UTC")
        df["tempo_espera"] = (now_utc - df["dh_abertura"]).dt.total_seconds() / 60.0

    return df_te, df_co
