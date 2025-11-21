import pandas as pd
import requests
import json
import copy
from datetime import datetime, timedelta
import math
import random
import numpy as np

# Configurações do servidor VROOM e OSRM
VROOM_URL = "http://localhost:3000"  # URL do seu servidor VROOM local
OSRM_URL = "http://localhost:5000"   # URL do seu servidor OSRM local (para distâncias/tempos se necessário)

# Carregar dataframes
try:
    df_equipes = pd.read_parquet("data/Equipes.parquet")
    df_tecnicos = pd.read_parquet("data/atendTec.parquet")
    df_comerciais = pd.read_parquet("data/ServCom.parquet")
except FileNotFoundError as e:
    print(f"Erro ao carregar arquivos Parquet: {e}")
    exit(1)

df_equipes.columns = df_equipes.columns.str.lower()
df_equipes["dt_ref"] = pd.to_datetime(df_equipes["dt_ref"])

df_tecnicos = df_tecnicos.dropna(subset=['LATITUDE', 'LONGITUDE'])   # remove jobs sem localização
df_tecnicos['TE'] = df_tecnicos['TE'].fillna(0)
df_tecnicos['TE'] = df_tecnicos['TE'].round(0).astype(int)

df_comerciais = df_comerciais.dropna(subset=['LATITUDE', 'LONGITUDE'])   # remove jobs sem localização
df_comerciais['TE'] = df_comerciais['TE'].fillna(0) 
df_comerciais['TE'] = df_comerciais['TE'].round(0).astype(int) 

print("Colunas em df_equipes:", df_equipes.columns.tolist())
print("Colunas em df_tecnicos:", df_tecnicos.columns.tolist())
print("Colunas em df_comerciais:", df_comerciais.columns.tolist())

def verificar_e_converter_colunas(df, colunas_esperadas, tipos_datetime):
    for col_esperada, col_real in colunas_esperadas.items():
        if col_real not in df.columns:
            print(f"Coluna '{col_real}' não encontrada. Colunas disponíveis: {df.columns.tolist()}")
            exit(1)
        if col_esperada in tipos_datetime:
            df[col_real] = pd.to_datetime(df[col_real], errors='coerce')

colunas_esperadas_equipes = {
    'dt_ref': 'dt_ref',
    'dthaps_ini': 'dthaps_ini',
    'dthaps_fim_ajustado': 'dthaps_fim_ajustado',
    'dthpausa_ini': 'dthpausa_ini',
    'dthpausa_fim': 'dthpausa_fim',
    'equipe': 'equipe'
}
verificar_e_converter_colunas(df_equipes, colunas_esperadas_equipes, 
                              ['dt_ref', 'dthaps_ini', 'dthaps_fim_ajustado', 'dthpausa_ini', 'dthpausa_fim'])

colunas_esperadas_tecnicos = {
    'DH_INICIO': 'DH_INICIO',
    'DH_FINAL': 'DH_FINAL',
    'EQUIPE': 'EQUIPE',
    'NUMOS': 'NUMOS',
    'TE': 'TE',
    'LONGITUDE': 'LONGITUDE',
    'LATITUDE': 'LATITUDE',
    'EUSD': 'EUSD'
}
verificar_e_converter_colunas(df_tecnicos, colunas_esperadas_tecnicos, ['DH_INICIO', 'DH_FINAL'])

colunas_esperadas_comerciais = {
    'DATA_SOL': 'DATA_SOL',
    'DATA_VENC': 'DATA_VENC',
    'EQUIPE': 'EQUIPE',
    'NUMOS': 'NUMOS',
    'TE': 'TE',
    'LONGITUDE': 'LONGITUDE',
    'LATITUDE': 'LATITUDE',
    'CODSERV': 'CODSERV',
    'EUSD': 'EUSD'
}
verificar_e_converter_colunas(df_comerciais, colunas_esperadas_comerciais, ['DATA_SOL', 'DATA_VENC'])

def calcular_penalizacao(linha, tipo_servico, prazo_verificado=None, prazo_regulatorio=None):
    if tipo_servico == 'tecnico':
        duracao = (linha['DH_FINAL'] - linha['DH_INICIO']).total_seconds() / 3600
        eusd = linha['EUSD']
        return duracao * (eusd / 730) * 34
    elif tipo_servico == 'comercial':
        eusd = linha['EUSD']
        if prazo_regulatorio and prazo_verificado:
            log_term = math.log(prazo_verificado / prazo_regulatorio) if prazo_verificado > prazo_regulatorio else 0
            return 120 + 34 * eusd * log_term
        return 120

def filtrar_servicos(df_tecnicos, df_comerciais, equipe, data_ref):
    data_ref_date = pd.to_datetime(data_ref).date()
    tecnicos = df_tecnicos[(df_tecnicos['DH_INICIO'].dt.date <= data_ref_date)]
    comerciais = df_comerciais[(df_comerciais['DATA_SOL'].dt.date <= data_ref_date)]
    comerciais = comerciais[~((comerciais['CODSERV'].isin([739, 741])) &
                              ((comerciais['DATA_SOL'].dt.hour < 8) | (comerciais['DATA_SOL'].dt.hour > 18)))]
    return tecnicos, comerciais

def preparar_vroom_input(equipe_linha, tecnicos, comerciais, data_ref, vehicle_id):
    start_time = equipe_linha['dthaps_ini']
    end_time = equipe_linha['dthaps_fim_ajustado']
    if pd.isna(start_time) or pd.isna(end_time):
        print(f"Equipe {equipe_linha['equipe']} tem tempos inválidos.")
        return None, None

    depot = {"location": [-63.88547754489104, -8.738553348981176]}

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
        vehicle["breaks"] = breaks

    jobs = []
    job_id_to_numos = {}
    job_id_counter = 1

    for idx, row in tecnicos.iterrows():
        if pd.isna(row['LONGITUDE']) or pd.isna(row['LATITUDE']):
            continue
        job = {
            "id": job_id_counter,
            "description": f"tec_{row['NUMOS']}",
            "location": [row['LONGITUDE'], row['LATITUDE']],
            "service": round(row['TE'] * 60, 0),
            "time_windows": [[int(row['DH_INICIO'].timestamp()), int((row['DH_INICIO'] + timedelta(days=1)).timestamp())]],
            "priority": 10
        }
        jobs.append(job)
        job_id_to_numos[job_id_counter] = row['NUMOS']
        job_id_counter += 1

    for idx, row in comerciais.iterrows():
        if pd.isna(row['LONGITUDE']) or pd.isna(row['LATITUDE']):
            continue
        job = {
            "id": job_id_counter,
            "description": f"com_{row['NUMOS']}",
            "location": [row['LONGITUDE'], row['LATITUDE']],
            "service": round(row['TE'] * 60, 0),
            "time_windows": [[int(row['DATA_SOL'].timestamp()), int(row['DATA_VENC'].timestamp())]],
            "priority": 5
        }
        jobs.append(job)
        job_id_to_numos[job_id_counter] = row['NUMOS']
        job_id_counter += 1

    return {"vehicles": [vehicle], "jobs": jobs}, job_id_to_numos


# ✅ **FUNÇÃO MODIFICADA**
def validar_vroom_input(v):
    print("\n===================== INPUT PARA O VROOM =====================")
    print(json.dumps(v, indent=4, ensure_ascii=False))
    print("==============================================================\n")

    print("🔍 Validando JOB IDs...")
    ids = [job["id"] for job in v.get("jobs", [])]
    print("Quantidade de jobs:", len(ids))
    print("IDs únicos:", len(set(ids)))
    if len(ids) != len(set(ids)):
        print("⚠️ IDs repetidos detectados!")


def chamar_vroom(vroom_input):
    headers = {'Content-Type': 'application/json'}
    url = VROOM_URL.rstrip("/") + "/"
    try:
        resp = requests.post(url, json=vroom_input, headers=headers, timeout=60)
    except requests.exceptions.RequestException as e:
        print(f"Erro ao chamar o VROOM: {e}")
        return None

    if resp.status_code == 200:
        return resp.json()
    else:
        print(f"❌ VROOM retornou {resp.status_code}: {resp.text}")
        return None


class MetaHeuristica:
    def __init__(self, vroom_input_base, tecnicos, comerciais, job_id_to_numos, num_iter=30):
        self.vroom_input = vroom_input_base
        self.tecnicos = tecnicos
        self.comerciais = comerciais
        self.job_id_to_numos = job_id_to_numos
        self.num_iter = num_iter
        self.melhor_solucao = vroom_input_base

    def otimizacao_hibrida(self):
        return self.melhor_solucao


resultados = []
tabela_servicos = []

for idx, equipe in df_equipes.iterrows():
    data_ref = equipe['dt_ref']
    tecnicos, comerciais = filtrar_servicos(df_tecnicos, df_comerciais, equipe['equipe'], data_ref)

    if tecnicos.empty and comerciais.empty:
        continue

    vroom_input, job_id_to_numos = preparar_vroom_input(equipe, tecnicos, comerciais, data_ref, vehicle_id=idx)
    if vroom_input is None:
        continue

    # ✅ **IMPRIME O JSON ANTES DE ENVIAR**
    validar_vroom_input(vroom_input)

    meta = MetaHeuristica(vroom_input, tecnicos, comerciais, job_id_to_numos)
    melhor_input = meta.otimizacao_hibrida()
    solucao = chamar_vroom(melhor_input)

    if solucao:
        resultados.append(solucao)

print("Concluído.")
