# v3/data_loader.py
from pathlib import Path
import pandas as pd

from v2.data_loader import _read_parquet_any, _prep_tecnicos, _prep_comercial


DATA_DIRS = [Path("data"), Path("/data")]


def prepare_equipes_v3() -> pd.DataFrame:
    """Carrega Equipes.parquet mantendo colunas de pausa e base da equipe.

    Campos principais padronizados:
    - tip_equipe
    - nome (EQUIPE)
    - dt_ref (normalizado por dia)
    - dthaps_ini
    - dthaps_fim_ajustado
    - inicio_turno (DATA_INICIO_TURNO)
    - fim_turno    (DATA_FIM_TURNO)
    - dthpausa_ini
    - dthpausa_fim
    - base_lon, base_lat (base específica da equipe)
    """
    df = _read_parquet_any("Equipes.parquet").copy()
    df.columns = df.columns.str.lower()

    rename = {
        "tip_equipe": "tip_equipe",
        "tipo_equipe": "tip_equipe",
        "equipe": "nome",
        "dt_ref": "dt_ref",
        "dthaps_ini": "dthaps_ini",
        "dthaps_fim": "dthaps_fim",
        "data_inicio_turno": "data_inicio_turno",
        "data_fim_turno": "data_fim_turno",
        "dthaps_fim_ajustado": "dthaps_fim_ajustado",
        "dthpausa_ini": "dthpausa_ini",
        "dthpausa_fim": "dthpausa_fim",
        # Se suas colunas de base tiverem outros nomes, mapeie aqui:
        # "x_base": "base_lon",
        # "y_base": "base_lat",
    }
    df = df.rename(columns=rename)

    # datas
    for c in [
        "dthaps_ini",
        "dthaps_fim",
        "data_inicio_turno",
        "data_fim_turno",
        "dthaps_fim_ajustado",
        "dthpausa_ini",
        "dthpausa_fim",
    ]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    # dt_ref como date normalizado
    if "dt_ref" in df.columns:
        df["dt_ref"] = pd.to_datetime(df["dt_ref"], errors="coerce").dt.normalize()
    else:
        df["dt_ref"] = pd.to_datetime(df["data_inicio_turno"], errors="coerce").dt.normalize()

    # chaves de turno consolidadas
    df["inicio_turno"] = pd.to_datetime(df.get("data_inicio_turno"), errors="coerce")
    df["fim_turno"] = pd.to_datetime(df.get("data_fim_turno"), errors="coerce")
    df["nome"] = df["nome"].astype(str)

    # garantir base_lon/base_lat como numérico, se existirem
    for c in ("base_lon", "base_lat"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # descartar coordenadas irrelevantes de equipes (mas manter base_lon/base_lat)
    for col in ("longitude", "latitude", "lon", "lat", "nox", "noy", "x", "y"):
        if col in df.columns:
            df.drop(columns=[col], inplace=True)

    keep = [
        "tip_equipe",
        "nome",
        "dt_ref",
        "dthaps_ini",
        "dthaps_fim_ajustado",
        "inicio_turno",
        "fim_turno",
        "dthpausa_ini",
        "dthpausa_fim",
        "base_lon",
        "base_lat",
    ]
    keep = [c for c in keep if c in df.columns]
    return df[keep].copy()


def prepare_pendencias_v3():
    """Carrega pendências técnicas e comerciais para o V3.

    - Reutiliza o pré-processamento do V2.
    - Normaliza coordenadas.
    - Remove linhas sem latitude/longitude.
    - Descarta coluna "equipe" das bases técnicas e comerciais.
    """
    tec = _prep_tecnicos()
    com = _prep_comercial()
    com.to_parquet("E:/Rotas-Inteligentes/data/BaseCom.parquet")
    tec.to_parquet("E:/Rotas-Inteligentes/data/BaseTec.parquet")

    for d in (tec, com):
        for c in ["latitude", "longitude"]:
            d[c] = pd.to_numeric(d[c], errors="coerce")
        # remover linhas sem coordenadas válidas
        d.dropna(subset=["latitude", "longitude"], inplace=True)
        # descartar equipe histórica
        if "equipe" in d.columns:
            d.drop(columns=["equipe"], inplace=True)

    return tec.reset_index(drop=True), com.reset_index(drop=True)