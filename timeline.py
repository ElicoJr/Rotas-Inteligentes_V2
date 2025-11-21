from datetime import timedelta
import numpy as np
import pandas as pd
from vroom_interface import osrm_table

# Tolerância de 1% na jornada
TOLERANCIA_FRAC = 0.01

# Serviços comerciais 739 e 741 só em horário diurno
CODSERV_DIURNO = {739, 741}
DIURNO_INI = 8   # 08:00
DIURNO_FIM = 18  # 18:00

def build_schedule(equipe_row, df_jobs_ord, base_lon, base_lat):
    """
    Gera timestamps de despacho/chegada/início e término do serviço:
      - deslocamentos reais via OSRM
      - execução TE (minutos)
      - janela diurna p/ codserv 739,741
      - pausa almoço (se houver)
      - retorno à base
      - tolerância 1% sobre a jornada
    Retorna: registros list[dict], fim_estimado datetime, cortou_por_jornada bool
    """
    if df_jobs_ord.empty:
        return [], equipe_row["turno_ini"], False

    # Coordenadas: base + jobs + base
    locs = [[base_lon, base_lat]] + df_jobs_ord[["longitude", "latitude"]].astype(float).values.tolist() + [[base_lon, base_lat]]

    # Matriz OSRM (em segundos)
    M = osrm_table(locs)
    if M.size == 0:
        M = np.zeros((len(locs), len(locs)))

    # Jornada
    start_turno = pd.to_datetime(equipe_row["turno_ini"])
    end_turno = pd.to_datetime(equipe_row.get("fim_ajustado", equipe_row.get("turno_fim", start_turno)))
    jornada_secs = max(0, int((end_turno - start_turno).total_seconds()))
    limite = start_turno + timedelta(seconds=int(jornada_secs * (1 + TOLERANCIA_FRAC)))

    # Pausa (se houver)
    pausa_ini = pd.to_datetime(equipe_row.get("pausa_ini"))
    pausa_fim = pd.to_datetime(equipe_row.get("pausa_fim"))
    tem_pausa = pd.notna(pausa_ini) and pd.notna(pausa_fim) and pausa_fim > pausa_ini

    now = start_turno
    registros = []
    cortou_por_jornada = False

    # i no M: 0 base, 1..N jobs, N+1 base
    for i, row in enumerate(df_jobs_ord.itertuples(index=False), start=1):
        # deslocamento de i-1 -> i
        travel_sec = int(M[i - 1, i]) if not np.isnan(M[i - 1, i]) else 0
        despacho = now
        chegada = now + timedelta(seconds=travel_sec)

        # Comerciais 739/741: só 08-18
        if getattr(row, "tipo", None) == "comercial":
            cod = int(getattr(row, "codserv", 0) or 0)
            if cod in CODSERV_DIURNO:
                chegada = _ajustar_janela_diurna(chegada, DIURNO_INI, DIURNO_FIM)

        # pausa almoço
        if tem_pausa and _intersecta_intervalo(chegada, chegada, pausa_ini, pausa_fim):
            chegada = pausa_fim

        te_min = float(getattr(row, "te", 0.0) or 0.0)
        dur_serv = max(60, int(round(te_min * 60)))  # em segundos
        termino = chegada + timedelta(seconds=dur_serv)

        if termino > limite:
            cortou_por_jornada = True
            break

        registros.append({
            "equipe": equipe_row["equipe"],
            "dt_ref": equipe_row["dt_ref"],
            "tipo": getattr(row, "tipo", None),
            "numos": getattr(row, "numos"),
            "datahora_origem": despacho,
            "datahora_chegada": chegada,
            "te_min": te_min,
            "td_sec": travel_sec,
            "datahora_termino": termino,   # <= ✅ TÉRMINO DO SERVIÇO
            "data_sol": getattr(row, "data_sol", pd.NaT),
            "data_venc": getattr(row, "data_venc", pd.NaT),
            "codserv": getattr(row, "codserv", pd.NA),
            "latitude": getattr(row, "latitude", pd.NA),
            "longitude": getattr(row, "longitude", pd.NA),
        })

        now = termino

    # retorno à base
    if len(registros) > 0:
        last_idx = len(df_jobs_ord.index)
        retorno_sec = int(M[last_idx, last_idx + 1]) if not np.isnan(M[last_idx, last_idx + 1]) else 0
        fim_rotina = now + timedelta(seconds=retorno_sec)
    else:
        fim_rotina = start_turno

    return registros, fim_rotina, cortou_por_jornada

def _ajustar_janela_diurna(dt, h_ini, h_fim):
    if dt.hour < h_ini:
        return dt.replace(hour=h_ini, minute=0, second=0, microsecond=0)
    if dt.hour >= h_fim:
        nxt = dt + timedelta(days=1)
        return nxt.replace(hour=h_ini, minute=0, second=0, microsecond=0)
    return dt

def _intersecta_intervalo(a_ini, a_fim, b_ini, b_fim):
    return max(a_ini, b_ini) <= min(a_fim, b_fim)
