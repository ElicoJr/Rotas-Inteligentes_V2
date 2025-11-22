# v2/data_loader.py
from pathlib import Path
from typing import Optional, Sequence

import numpy as np
import pandas as pd


# Diretórios padrão para busca dos .parquet
DATA_DIRS: Sequence[Path] = (Path("data"), Path("/data"), Path("."))


def _read_parquet_any(name: str) -> pd.DataFrame:
    """
    Procura um arquivo parquet com o nome dado em data/, /data e .,
    carrega e normaliza valores infinitos para NA.

    Substitui o antigo uso de:
        with pd.option_context("mode.use_inf_as_na", True):
            pd.read_parquet(...)
    que gerava FutureWarning.
    """
    for base in DATA_DIRS:
        p = base / name
        if p.exists():
            df = pd.read_parquet(p)
            # converte inf/-inf para NA explicitamente
            df.replace([np.inf, -np.inf], pd.NA, inplace=True)
            return df
    raise FileNotFoundError(f"Não encontrei {name} em {', '.join(str(d) for d in DATA_DIRS)}")


def _prep_tecnicos() -> pd.DataFrame:
    """
    Carrega e normaliza a base de serviços técnicos (atendTec.parquet) para o layout V3.

    Espera uma estrutura semelhante a:

    - TIPO_BASE / TIPSERV
    - NUMOS
    - DH_INICIO (data/hora início)
    - DH_ALOCACAO
    - DH_CHEGADA
    - DH_FINAL (data/hora término)
    - TE (tempo de execução em minutos)
    - TD (tempo de deslocamento em minutos) [opcional]
    - LATITUDE, LONGITUDE (coordenadas)
    - EUSD, EUSD_FIO_B [opcionais]
    """
    df = _read_parquet_any("atendTec.parquet").copy()

    # normaliza nomes de colunas
    df.columns = df.columns.str.lower()
    df = df.drop_duplicates(subset="numos")

    # rename mínimo para padronizar
    rename_map = {
        "numos": "numos",
        "dh_inicio": "datasol",
        "dh_allocacao": "datasaida",     # se existir; alguns datasets usam dh_alocacao
        "dh_alocacao": "datasaida",
        "dh_chegada": "datainitrab",
        "dh_final": "datater_trab",
        "latitude": "latitude",
        "longitude": "longitude",
        "te": "te",
        "td": "td",
        "eusd": "EUSD",
        "eusd_fio_b": "EUSD_FIO_B",
    }
    df = df.rename(columns=rename_map)

    # caso original do usuário (se as colunas estiverem com nomes específicos)
    # esses renames não conflitam, apenas garantem compatibilidade
    if "dh_inicio" in df.columns and "datasol" not in df.columns:
        df["datasol"] = pd.to_datetime(df["dh_inicio"], errors="coerce")
    else:
        df["datasol"] = pd.to_datetime(df.get("datasol"), errors="coerce")

    df["datater_trab"] = pd.to_datetime(df.get("datater_trab"), errors="coerce")

    # TIPO: todos técnicos
    df["tipo_serv"] = "técnico"

    # numos como string consistente
    if "numos" in df.columns:
        df["numos"] = df["numos"].astype(str)
    else:
        df["numos"] = pd.NA

    # TD/TE numéricos (minutos)
    df["TE"] = pd.to_numeric(df.get("te", df.get("TE", 0)), errors="coerce").fillna(0).astype(float)
    df["TD"] = pd.to_numeric(df.get("td", df.get("TD", 0)), errors="coerce").fillna(0).astype(float)

    # Coordenadas
    df["latitude"] = pd.to_numeric(df.get("latitude"), errors="coerce")
    df["longitude"] = pd.to_numeric(df.get("longitude"), errors="coerce")

    # dt_ref: dia de referência = data de solicitação normalizada
    df["dt_ref"] = pd.to_datetime(df["datasol"], errors="coerce").dt.normalize()

    # EUSD / EUSD_FIO_B se existirem
    if "EUSD" not in df.columns and "eusd" in df.columns:
        df["EUSD"] = pd.to_numeric(df["eusd"], errors="coerce")
    if "EUSD_FIO_B" not in df.columns and "eusd_fio_b" in df.columns:
        df["EUSD_FIO_B"] = pd.to_numeric(df["eusd_fio_b"], errors="coerce")

    # Seleciona colunas principais
    cols = [
        "tipo_serv",
        "numos",
        "datasol",
        "datater_trab",
        "TD",
        "TE",
        "latitude",
        "longitude",
        "dt_ref",
        "EUSD",
        "EUSD_FIO_B",
    ]
    # garante existência das colunas (mesmo que NA)
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA

    return df[cols].copy()


def _prep_comercial() -> pd.DataFrame:
    """
    Carrega e normaliza a base de serviços comerciais (ServCom.parquet) para o layout V3.

    Espera uma estrutura semelhante a:

    - NUMOS
    - TIPSERV / TIPO_BASE
    - DATASOL (DATA_SOL)
    - DATAVENC (DATA_VENC)
    - DATATERTRAB
    - LATITUDE, LONGITUDE
    - TE, TD (minutos)
    - EUSD, EUSD_FIO_B [opcionais]
    """
    df = _read_parquet_any("ServCom.parquet").copy()
    

    # normaliza nomes de colunas
    df.columns = df.columns.str.lower()
    df = df.drop_duplicates(subset="numos")
    df = df[df["dataven"] > df["datasol"]]

    rename_map = {
        "numos": "numos",
        "data_sol": "datasol",
        "datasol": "datasol",
        "data_venc": "dataven",
        "data_vencimento": "dataven",
        "datatertrab": "datater_trab",
        "latitude": "latitude",
        "longitude": "longitude",
        "te": "te",
        "td": "td",
        "eusd": "EUSD",
        "eusd_fio_b": "EUSD_FIO_B",
    }
    df = df.rename(columns=rename_map)

    # tipo sempre comercial
    df["tipo_serv"] = "comercial"

    # datas
    df["datasol"] = pd.to_datetime(df.get("datasol"), errors="coerce")
    df["dataven"] = pd.to_datetime(df.get("dataven"), errors="coerce")
    df["datater_trab"] = pd.to_datetime(df.get("datater_trab"), errors="coerce")

    # numos como string
    if "numos" in df.columns:
        df["numos"] = df["numos"].astype(str)
    else:
        df["numos"] = pd.NA

    # TD/TE numéricos (minutos)
    df["TE"] = pd.to_numeric(df.get("te", df.get("TE", 0)), errors="coerce").fillna(0).astype(float)
    df["TD"] = pd.to_numeric(df.get("td", df.get("TD", 0)), errors="coerce").fillna(0).astype(float)

    # Coordenadas
    df["latitude"] = pd.to_numeric(df.get("latitude"), errors="coerce")
    df["longitude"] = pd.to_numeric(df.get("longitude"), errors="coerce")

    # dt_ref: dia de referência = data de solicitação normalizada
    df["dt_ref"] = pd.to_datetime(df["datasol"], errors="coerce").dt.normalize()

    # EUSD / EUSD_FIO_B se existirem
    if "EUSD" not in df.columns and "eusd" in df.columns:
        df["EUSD"] = pd.to_numeric(df["eusd"], errors="coerce")
    if "EUSD_FIO_B" not in df.columns and "eusd_fio_b" in df.columns:
        df["EUSD_FIO_B"] = pd.to_numeric(df["eusd_fio_b"], errors="coerce")

    cols = [
        "tipo_serv",
        "numos",
        "datasol",
        "dataven",
        "datater_trab",
        "TD",
        "TE",
        "latitude",
        "longitude",
        "dt_ref",
        "EUSD",
        "EUSD_FIO_B",
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA

    return df[cols].copy()