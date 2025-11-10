import pandas as pd
import requests
import json
from datetime import datetime, timedelta

VROOM_URL = "http://localhost:3000"
MAX_JOBS = 15

def filtrar_servicos(df_tecnicos, df_comerciais, equipe_linha):
    data_ref_date = pd.to_datetime(equipe_linha['dt_ref']).date()
    
    if pd.isna(equipe_linha['dthaps_fim_ajustado']):
        print(f"⚠️ Equipe {equipe_linha['equipe']} sem 'dthaps_fim_ajustado'. Pulando filtragem.")
        return pd.DataFrame(columns=df_tecnicos.columns), pd.DataFrame(columns=df_comerciais.columns)
        
    fim_turno_dt = equipe_linha['dthaps_fim_ajustado']

    tecnicos = df_tecnicos[
        (df_tecnicos['DH_INICIO'].dt.date <= data_ref_date) &
        (df_tecnicos['DH_INICIO'] <= fim_turno_dt)
    ]
    
    comerciais = df_comerciais[
        (df_comerciais['DATA_SOL'].dt.date <= data_ref_date) &
        (df_comerciais['DATA_SOL'] <= fim_turno_dt)
    ]
    
    comerciais = comerciais[~((comerciais['CODSERV'].isin([739, 741])) & 
                              ((comerciais['DATA_SOL'].dt.hour < 8) | (comerciais['DATA_SOL'].dt.hour > 18)))]
    
    return tecnicos, comerciais

def preparar_jobs(tecnicos, comerciais):
    jobs = []
    job_id_counter = 1
    job_id_to_numos = {}
    
    for idx, row in tecnicos.iterrows():
        if pd.isna(row['LONGITUDE']) or pd.isna(row['LATITUDE']) or pd.isna(row['DH_INICIO']) or pd.isna(row['TE']):
            continue
        job = {
            "id": job_id_counter,
            "description": f"tec_{row['NUMOS']}",
            "location": [float(row['LONGITUDE']), float(row['LATITUDE'])],
            "service": int(row['TE'] * 60),
            "time_windows": [[int(row['DH_INICIO'].timestamp()), int((row['DH_INICIO'] + timedelta(days=1)).timestamp())]],
            "priority": 10
        }
        jobs.append(job)
        job_id_to_numos[job_id_counter] = row['NUMOS']
        job_id_counter += 1
    
    for idx, row in comerciais.iterrows():
        if pd.isna(row['LONGITUDE']) or pd.isna(row['LATITUDE']) or pd.isna(row['DATA_SOL']) or pd.isna(row['DATA_VENC']) or pd.isna(row['TE']):
            continue
        
        # A validação V15 (DATA_VENC < DATA_SOL) foi movida para o data_loader.py
        data_sol = row['DATA_SOL']
        data_venc = row['DATA_VENC']

        job = {
            "id": job_id_counter,
            "description": f"com_{row['NUMOS']}",
            "location": [float(row['LONGITUDE']), float(row['LATITUDE'])],
            "service": int(row['TE'] * 60),
            "time_windows": [[int(data_sol.timestamp()), int(data_venc.timestamp())]],
            "priority": 5
        }
        jobs.append(job)
        job_id_to_numos[job_id_counter] = row['NUMOS']
        job_id_counter += 1
    
    return jobs, job_id_to_numos

def preparar_vroom_input(equipe_linha, jobs_batch, data_ref, vehicle_id):
    start_time = equipe_linha['dthaps_ini']
    end_time = equipe_linha['dthaps_fim_ajustado']
    if pd.isna(start_time) or pd.isna(end_time):
        return None
    
    depot = {"id": 0, "location": [-63.88547754489104, -8.738553348981176]}
    vehicle = {
        "id": vehicle_id,
        "description": f"Equipe {equipe_linha['equipe']}",
        "start": depot["location"],
        "end": depot["location"],
        "time_window": [int(start_time.timestamp()), int(end_time.timestamp())],
        "capacity": [1],
        "skills": [1]
    }
    
    breaks = []
    if pd.notna(equipe_linha['dthpausa_ini']) and pd.notna(equipe_linha['dthpausa_fim']):
        breaks.append({
            "id": vehicle_id + 1000,
            "time_windows": [[int(equipe_linha['dthpausa_ini'].timestamp()), int(equipe_linha['dthpausa_fim'].timestamp())]],
            "service": 0
        })
    
    if breaks:
        vehicle["breaks"] = breaks
    
    return {
        "vehicles": [vehicle],
        "jobs": jobs_batch
    }

def validar_e_limpar_lote(jobs_batch):
    seen_locations = set()
    cleaned_jobs = []
    for job in jobs_batch:
        loc = tuple(job['location'])
        if loc not in seen_locations and all(isinstance(coord, (int, float)) and not pd.isna(coord) for coord in loc):
            seen_locations.add(loc)
            cleaned_jobs.append(job)
    return cleaned_jobs

def chamar_vroom(vroom_input):
    headers = {'Content-Type': 'application/json'}
    url = VROOM_URL.rstrip("/") + "/"
    try:
        resp = requests.post(url, json=vroom_input, headers=headers, timeout=60)
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro de requisição ao VROOM ({url}): {e}")
        return None
    
    if resp.status_code == 200:
        return resp.json()
    else:
        print(f"❌ VROOM retornou {resp.status_code} para {url}. Corpo: {resp.text}")
        return None

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]