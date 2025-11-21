import pandas as pd
import numpy as np
from math import radians, sin, cos, asin, sqrt

def safe_number(x):
    try:
        v = float(x)
        if np.isnan(v):
            return float("nan")
        return v
    except Exception:
        return float("nan")

def _service_seconds_from_row(row) -> int:
    """
    Retorna TE em segundos (TE vem em minutos).
    """
    te_min = row.get("TE", row.get("te", 0))
    try:
        te_min = float(te_min)
        if np.isnan(te_min):
            te_min = 0.0
    except Exception:
        te_min = 0.0
    return int(max(0.0, te_min) * 60.0)

def _dedup_ids(int_ids):
    """
    Remove duplicados preservando ordem; se houver duplicados,
    aplica offset incremental para ficar único (evitar 'Duplicate job id').
    """
    seen = {}
    out = []
    for i in int_ids:
        base = int(i)
        if base not in seen:
            seen[base] = 0
            out.append(base)
        else:
            seen[base] += 1
            out.append(int(f"{base}{seen[base]}"))  # ex.: 123 → 1231, 1232...
    return out

def gerar_jobs_com_ids(df_jobs: pd.DataFrame):
    """
    Constrói a lista de jobs para VROOM e um DF anotado com 'job_id_vroom'.
    """
    df = df_jobs.copy()
    # id preferencial: NUMOS; se não houver, usa índice.
    if "numos" in df.columns and df["numos"].notna().any():
        base_ids = df["numos"].fillna(-1).astype("int64").tolist()
    else:
        base_ids = list(range(1, len(df) + 1))

    job_ids = _dedup_ids(base_ids)
    df["job_id_vroom"] = job_ids

    jobs = []
    for jid, (_, r) in zip(job_ids, df.iterrows()):
        lon = float(r["longitude"]); lat = float(r["latitude"])
        service = _service_seconds_from_row(r)  # segundos
        jobs.append({"id": int(jid), "location": [lon, lat], "service": int(service)})
    return jobs, df
