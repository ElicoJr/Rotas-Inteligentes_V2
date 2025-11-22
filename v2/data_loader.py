from pathlib import Path
import pandas as pd

DATA_DIRS = [Path("data"), Path("/data")]

def _read_parquet_any(name: str) -> pd.DataFrame:
    for d in DATA_DIRS:
        p = d / name
        if p.exists():
            return pd.read_parquet(p)
    raise FileNotFoundError(f"Arquivo não encontrado: {name} em {DATA_DIRS}")

def prepare_equipes() -> pd.DataFrame:
    # Equipes.parquet (maiúsculas)
    df = _read_parquet_any("Equipes.parquet").copy()
    # normalizar nomes
    df.columns = df.columns.str.lower()

    # mapeamentos esperados
    rename = {
        "tip_equipe": "tip_equipe",
        "equipe": "nome",
        "dt_ref": "dt_ref",
        "dthaps_ini": "dthaps_ini",
        "dthaps_fim": "dthaps_fim",
        "data_inicio_turno": "data_inicio_turno",
        "data_fim_turno": "data_fim_turno",
        "dthaps_fim_ajustado": "dthaps_fim_ajustado",
        "dthpausa_ini": "dthpausa_ini",
        "dthpausa_fim": "dthpausa_fim",
    }
    df = df.rename(columns=rename)

    # datas
    for c in ["dthaps_ini","dthaps_fim","data_inicio_turno","data_fim_turno",
              "dthaps_fim_ajustado","dthpausa_ini","dthpausa_fim"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    # dt_ref como date -> datetime normalizado
    if "dt_ref" in df.columns:
        df["dt_ref"] = pd.to_datetime(df["dt_ref"], errors="coerce").dt.normalize()
    else:
        # fallback: pelo início de turno
        df["dt_ref"] = pd.to_datetime(df["data_inicio_turno"], errors="coerce").dt.normalize()

    # chaves de turno consolidadas
    df["inicio_turno"] = pd.to_datetime(df["data_inicio_turno"], errors="coerce")
    df["fim_turno"]    = pd.to_datetime(df["data_fim_turno"], errors="coerce")
    df["nome"] = df["nome"].astype(str)

    # ⚠️ descartar quaisquer coordenadas em equipes
    for col in ("longitude","latitude","lon","lat","nox","noy","x","y"):
        if col in df.columns:
            df.drop(columns=[col], inplace=True)

    # manter colunas úteis
    keep = ["tip_equipe","nome","dt_ref","dthaps_ini","dthaps_fim_ajustado",
            "inicio_turno","fim_turno"]
    keep = [c for c in keep if c in df.columns]
    return df[keep].copy()

def _prep_tecnicos() -> pd.DataFrame:
    df = _read_parquet_any("atendTec.parquet").copy()
    df.columns = df.columns.str.lower()
    df = df.drop_duplicates(subset=["numos"])
    ren = {
        "tipo_base":"tipo_base",
        "numos":"numos",
        "abrangencia":"abrangencia",
        "defeito_falha":"defeito_falha",
        "localidade":"localidade",
        "localizacao":"localizacao",
        "nox":"nox","noy":"noy",
        "dh_inicio":"dh_inicio",
        "dh_alocacao":"dh_alocacao",
        "dh_chegada":"dh_chegada",
        "dh_final":"dh_final",
        "tp":"tp","td":"td","te":"te",
        "equipe":"equipe",
        "ano":"ano","mes":"mes",
        "eusd":"eusd","eusd_fio_b":"eusd_fio_b",
        "latitude":"latitude","longitude":"longitude",
    }
    df = df.rename(columns=ren)

    # datas
    for c in ["dh_inicio","dh_alocacao","dh_chegada","dh_final"]:
        df[c] = pd.to_datetime(df[c], errors="coerce")

    # padronizar nomes de campos-alvo do motor
    df["tipo_serv"]    = "técnico"
    df["datasol"]      = df["dh_inicio"]
    df["dataven"]      = pd.NaT
    df["datater_trab"] = df["dh_final"]
    df["dt_ref"]       = pd.to_datetime(df["dh_inicio"], errors="coerce").dt.normalize()

    # TE/TD como minutos (já vem em minutos)
    df["TE"] = pd.to_numeric(df["te"], errors="coerce")
    df["TD"] = pd.to_numeric(df["td"], errors="coerce")

    # prioridade/violação (placeholders – podem ser refinados)
    df["prioridade"] = 1.0
    df["violacao"]   = 0.0

    return df

def _prep_comercial() -> pd.DataFrame:
    df = _read_parquet_any("ServCom.parquet").copy()
    df.columns = df.columns.str.lower()
    df = df.drop_duplicates(subset=["numos"])
    df = df[df["data_venc"] >= df["data_sol"]]

    ren = {
        "numos":"numos",
        "tipserv":"tipserv",
        "codserv":"codserv",
        "data_sol":"data_sol",
        "data_venc":"data_venc",
        "datasaida":"datasaida",
        "datainitrab":"datainitrab",
        "datatertrab":"datatertrab",
        "equipe":"equipe",
        "td":"td","te":"te",
        "latitude":"latitude","longitude":"longitude",
        "eusd_fio_b":"eusd_fio_b","eusd":"eusd"
    }
    df = df.rename(columns=ren)

    for c in ["data_sol","data_venc","datasaida","datainitrab","datatertrab"]:
        df[c] = pd.to_datetime(df[c], errors="coerce")

    df["tipo_serv"]    = "comercial"
    df["datasol"]      = df["data_sol"]
    df["dataven"]      = df["data_venc"]
    df["datater_trab"] = df["datatertrab"]
    df["dt_ref"]       = pd.to_datetime(df["data_sol"], errors="coerce").dt.normalize()

    df["TE"] = pd.to_numeric(df["te"], errors="coerce")
    df["TD"] = pd.to_numeric(df["td"], errors="coerce")

    # prioridade: exemplo — aproximar vencimento mais próximo
    df["prioridade"] = 1.0
    with pd.option_context("mode.use_inf_as_na", True):
        prox = (df["dataven"] - df["datasol"]).dt.total_seconds() / 3600.0
    df.loc[prox.notna(), "prioridade"] = 1.0 + (24.0 / (prox.clip(lower=1.0)))
    df["violacao"]   = 0.0
    return df

def prepare_pendencias():
    tec = _prep_tecnicos()
    com = _prep_comercial()

    # sanitização mínima de coordenadas
    for d in (tec, com):
        for c in ["latitude","longitude"]:
            d[c] = pd.to_numeric(d[c], errors="coerce")

    # remover linhas sem coordenadas válidas
    tec = tec.dropna(subset=["latitude","longitude"])
    com = com.dropna(subset=["latitude","longitude"])
    com.to_parquet('/data/ServicosComerciais.parquet', index=False)
    tec.to_parquet('/data/ServicosTecnicos.parquet', index=False)

    return tec.reset_index(drop=True), com.reset_index(drop=True)
