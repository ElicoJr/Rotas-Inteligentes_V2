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
MAX_JOBS = 50

# Carregar os dataframes (assumindo que você tem os arquivos CSV ou os dataframes já carregados)
# Substitua pelos seus caminhos ou carregue diretamente
try:
    df_equipes = pd.read_parquet("data/Equipes.parquet")  # DataFrame de equipes
    df_tecnicos = pd.read_parquet("data/atendTec.parquet")  # DataFrame de serviços técnicos
    df_comerciais = pd.read_parquet("data/ServCom.parquet")  # DataFrame de serviços comerciais
except FileNotFoundError as e:
    print(f"Erro ao carregar arquivos Parquet: {e}")
    print("Verifique se os caminhos dos arquivos estão corretos.")
    exit(1)

df_equipes.columns = df_equipes.columns.str.lower()
df_equipes["dt_ref"] = pd.to_datetime(df_equipes["dt_ref"])

df_tecnicos = df_tecnicos.dropna(subset=['LATITUDE', 'LONGITUDE'])   # remove jobs sem localização
df_tecnicos['TE'] = df_tecnicos['TE'].fillna(0)
df_tecnicos['TE'] = df_tecnicos['TE'].round(0).astype(int)

df_comerciais = df_comerciais.dropna(subset=['LATITUDE', 'LONGITUDE'])   # remove jobs sem localização
df_comerciais['TE'] = df_comerciais['TE'].fillna(0) 
df_comerciais['TE'] = df_comerciais['TE'].round(0).astype(int) 

# Verificar e imprimir colunas para depuração
print("Colunas em df_equipes:", df_equipes.columns.tolist())
print("Colunas em df_tecnicos:", df_tecnicos.columns.tolist())
print("Colunas em df_comerciais:", df_comerciais.columns.tolist())

# Mapeamento de colunas esperadas (ajuste se necessário baseado na saída acima)
colunas_esperadas_equipes = {
    'dt_ref': 'dt_ref',  # Ajuste se o nome real for diferente, e.g., 'DT_REF'
    'dthaps_ini': 'dthaps_ini',
    'dthaps_fim_ajustado': 'dthaps_fim_ajustado',
    'dthpausa_ini': 'dthpausa_ini',
    'dthpausa_fim': 'dthpausa_fim',
    'equipe': 'equipe'
}

# Função auxiliar para verificar e converter colunas
def verificar_e_converter_colunas(df, colunas_esperadas, tipos_datetime):
    for col_esperada, col_real in colunas_esperadas.items():
        if col_real not in df.columns:
            print(f"Coluna '{col_real}' não encontrada em {df}. Colunas disponíveis: {df.columns.tolist()}")
            exit(1)
        if col_esperada in tipos_datetime:
            df[col_real] = pd.to_datetime(df[col_real], errors='coerce')

# Converter colunas de data para datetime
tipos_datetime_equipes = ['dt_ref', 'dthaps_ini', 'dthaps_fim_ajustado', 'dthpausa_ini', 'dthpausa_fim']
verificar_e_converter_colunas(df_equipes, colunas_esperadas_equipes, tipos_datetime_equipes)

# Para df_tecnicos e df_comerciais
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

# Função para calcular penalizações
def calcular_penalizacao(linha, tipo_servico, prazo_verificado=None, prazo_regulatorio=None):
    if tipo_servico == 'tecnico':
        # Penalização: duração da interrupção * (EUSD/730) * 34
        # Duração em horas
        duracao = (linha['DH_FINAL'] - linha['DH_INICIO']).total_seconds() / 3600
        eusd = linha['EUSD']
        return duracao * (eusd / 730) * 34
    elif tipo_servico == 'comercial':
        # Penalização: 120 + 34 * EUSD * log(prazo_verificado / prazo_regulatorio)
        # Assumindo prazo_regulatorio como o tempo entre DATA_SOL e DATA_VENC em dias
        # Prazo verificado como o tempo decorrido até a execução (simplificado como atraso em dias)
        eusd = linha['EUSD']
        if prazo_regulatorio and prazo_verificado:
            log_term = math.log(prazo_verificado / prazo_regulatorio) if prazo_verificado > prazo_regulatorio else 0
            return 120 + 34 * eusd * log_term
        return 120  # Penalização base se não houver prazo

# Função para filtrar serviços por data e equipe
def filtrar_servicos(df_tecnicos, df_comerciais, equipe, data_ref):
    # Converter data_ref para date para comparação consistente
    data_ref_date = pd.to_datetime(data_ref).date()
    
    # Serviços técnicos: filtrar por EQUIPE e DH_INICIO <= data_ref
    tecnicos = df_tecnicos[(df_tecnicos['EQUIPE'] == equipe) & (df_tecnicos['DH_INICIO'].dt.date <= data_ref_date)]
    
    # Serviços comerciais: filtrar por EQUIPE e DATA_SOL <= data_ref
    comerciais = df_comerciais[(df_comerciais['EQUIPE'] == equipe) & (df_comerciais['DATA_SOL'].dt.date <= data_ref_date)]
    
    # Restrição: Serviços comerciais com CODSERV 739 ou 741 só entre 8h e 18h
    comerciais = comerciais[~((comerciais['CODSERV'].isin([739, 741])) & 
                              ((comerciais['DATA_SOL'].dt.hour < 8) | (comerciais['DATA_SOL'].dt.hour > 18)))]
    
    return tecnicos, comerciais

# Função para preparar input para VROOM
def preparar_vroom_input(equipe_linha, tecnicos, comerciais, data_ref, vehicle_id):
    # Verificar se os tempos da equipe são válidos
    start_time = equipe_linha['dthaps_ini']
    end_time = equipe_linha['dthaps_fim_ajustado']
    if pd.isna(start_time) or pd.isna(end_time):
        print(f"Equipe {equipe_linha['equipe']} tem tempos inválidos. Pulando.")
        return None
    
    # Ponto de partida e retorno: (-8.738553348981176, -63.88547754489104)
    depot = {"id": 0, "location": [-63.88547754489104, -8.738553348981176]}  # [lon, lat]
    
    # Veículo: equipe com time window baseada em dthaps_ini e dthaps_fim_ajustado
    vehicle = {
        "id": vehicle_id,  # Usar ID inteiro único
        "description": f"Equipe {equipe_linha['equipe']}",
        "start": depot["location"],
        "end": depot["location"],
        "time_window": [int(start_time.timestamp()), int(end_time.timestamp())],  # Unix timestamp
        "capacity": [1],  # Capacidade para 1 serviço por vez
        "skills": [1]  # Habilidade para serviços
    }
    
    # Pausas: se dthpausa_ini e dthpausa_fim não vazios
    breaks = []
    if pd.notna(equipe_linha['dthpausa_ini']) and pd.notna(equipe_linha['dthpausa_fim']):
        breaks.append({
            "id": vehicle_id + 1000,  # Usar ID inteiro único para breaks
            "time_windows": [[int(equipe_linha['dthpausa_ini'].timestamp()), int(equipe_linha['dthpausa_fim'].timestamp())]],
            "service": 0  # Tempo de pausa
        })
    
    # Jobs: serviços
    jobs = []
    job_id_counter = 1
    job_id_to_numos = {}  # Mapeamento de job_id para NUMOS
    for idx, row in tecnicos.iterrows():
        # Pular se dados essenciais são NaN
        if pd.isna(row['LONGITUDE']) or pd.isna(row['LATITUDE']) or pd.isna(row['DH_INICIO']) or pd.isna(row['TE']):
            continue
        job = {
            "id": job_id_counter,
            "description": f"tec_{row['NUMOS']}",
            "location": [row['LONGITUDE'], row['LATITUDE']],
            "service": row['TE'] * 60,  # TE em minutos para segundos
            "time_windows": [[int(row['DH_INICIO'].timestamp()), int((row['DH_INICIO'] + timedelta(days=1)).timestamp())]],  # Janela flexível para técnicos
            "priority": 10  # Alta prioridade para técnicos
        }
        jobs.append(job)
        job_id_to_numos[job_id_counter] = row['NUMOS']
        job_id_counter += 1
    
    for idx, row in comerciais.iterrows():
        # Pular se dados essenciais são NaN
        if pd.isna(row['LONGITUDE']) or pd.isna(row['LATITUDE']) or pd.isna(row['DATA_SOL']) or pd.isna(row['DATA_VENC']) or pd.isna(row['TE']):
            continue
        job = {
            "id": int(job_id_counter),
            "description": f"com_{row['NUMOS']}",
            "location": [row['LONGITUDE'], row['LATITUDE']],
            "service": row['TE'] * 60,
            "time_windows": [[int(row['DATA_SOL'].timestamp()), int(row['DATA_VENC'].timestamp())]],  # Preferir não extrapolar DATA_VENC
            "priority": 5  # Menor prioridade
        }
        jobs.append(job)
        job_id_to_numos[job_id_counter] = row['NUMOS']
        job_id_counter = int(job_id_counter) + 1

    
        # Input completo para VROOM
        # Se houver pausas, adicione dentro do veículo
        if breaks:
            vehicle["breaks"] = breaks

        vroom_input = {
            "vehicles": [vehicle],
            "jobs": jobs
        }
    return vroom_input, job_id_to_numos

def validar_vroom_input(v):
    print("\nValidando JOB IDs:")
    ids = [job["id"] for job in v["jobs"]]
    print("Quantidade de jobs:", len(ids))
    print("IDs únicos:", len(set(ids)))
    if len(ids) != len(set(ids)):
        print("⚠️ IDs repetidos detectados!")
    
    for i, job in enumerate(v["jobs"]):
        if not isinstance(job["id"], int):
            print(f"⚠️ Job {i} possui ID inválido:", job["id"], type(job["id"]))
        if "location" not in job or job["location"] is None:
            print(f"⚠️ Job {i} sem location:", job)


# Função para chamar VROOM (simplificada para usar apenas "/")
def chamar_vroom(vroom_input):
    headers = {'Content-Type': 'application/json'}
    url = VROOM_URL.rstrip("/") + "/"
    try:
        resp = requests.post(url, json=vroom_input, headers=headers, timeout=60)
    except requests.exceptions.RequestException as e:
        print(f"Erro de requisição ao VROOM ({url}): {e}")
        return None

    if resp.status_code == 200:
        try:
            return resp.json()
        except Exception as e:
            print(f"Resposta JSON inválida do VROOM ({url}): {e}")
            return None
    else:
        body = None
        try:
            body = resp.text
        except:
            body = "<não foi possível ler body>"
        print(f"VROOM retornou {resp.status_code} para {url}. Corpo:\n{body}\n")
        return None

# Implementação básica de metaheurística híbrida (GA + SA + ACO simplificada)
# Isso é uma versão simplificada; em produção, use bibliotecas como DEAP para GA, etc.
class MetaHeuristica:
    def __init__(self, vroom_input_base, tecnicos, comerciais, job_id_to_numos, num_iter=100):
        self.vroom_input = vroom_input_base
        self.tecnicos = tecnicos
        self.comerciais = comerciais
        self.job_id_to_numos = job_id_to_numos
        self.num_iter = num_iter
        self.melhor_solucao = None
        self.melhor_custo = float('inf')
    
    def avaliar_solucao(self, solucao):
        # Simular custo baseado na resposta do VROOM (tempo total, penalizações)
        resposta = chamar_vroom(solucao)
        if resposta:
            custo = resposta['summary']['cost']  # Custo total do VROOM
            # Adicionar penalizações personalizadas
            for route in resposta['routes']:
                for step in route['steps']:
                    if step.get('type') == 'job':
                        job_id = step['job']
                        arrival_ts = step['arrival']
                        service_ts = step['service']
                        
                        # Usar mapeamento para obter NUMOS
                        numos = self.job_id_to_numos.get(job_id)
                        if numos is None:
                            continue
                        
                        # Identificar tipo
                        if numos in self.tecnicos['NUMOS'].values:
                            tipo = 'tecnico'
                            df_servico = self.tecnicos
                        elif numos in self.comerciais['NUMOS'].values:
                            tipo = 'comercial'
                            df_servico = self.comerciais
                        else:
                            continue
                        
                        # Buscar linha do serviço
                        linha = df_servico[df_servico['NUMOS'] == numos]
                        if not linha.empty:
                            linha = linha.iloc[0]
                            
                            # Para técnico, atualizar DH_FINAL com arrival + service
                            if tipo == 'tecnico':
                                linha = linha.copy()
                                linha['DH_FINAL'] = datetime.fromtimestamp(arrival_ts + service_ts)
                            
                            # Para comercial, calcular prazo_verificado (tempo até execução em dias)
                            if tipo == 'comercial':
                                prazo_regulatorio = (linha['DATA_VENC'] - linha['DATA_SOL']).total_seconds() / (3600 * 24)  # Dias
                                prazo_verificado = (datetime.fromtimestamp(arrival_ts) - linha['DATA_SOL']).total_seconds() / (3600 * 24)
                                penal = calcular_penalizacao(linha, tipo, prazo_verificado, prazo_regulatorio)
                            else:
                                penal = calcular_penalizacao(linha, tipo)
                            
                            custo += penal
            return custo
        return float('inf')
    
    def gerar_vizinho(self, solucao):
        # Modificar ordem de jobs ou time windows (simplificado)
        nova_solucao = copy.deepcopy(solucao)
        # Exemplo: trocar dois jobs
        if len(nova_solucao['jobs']) > 1:
            i, j = random.sample(range(len(nova_solucao['jobs'])), 2)
            nova_solucao['jobs'][i], nova_solucao['jobs'][j] = nova_solucao['jobs'][j], nova_solucao['jobs'][i]
        return nova_solucao
    
    def otimizacao_hibrida(self):
        # Inicialização com ACO (feromônios simples)
        feromonios = {job['id']: 1.0 for job in self.vroom_input['jobs']}
        
        # GA: População inicial
        populacao = [copy.deepcopy(self.vroom_input) for _ in range(10)]
        
        for _ in range(self.num_iter):
            # Avaliar população
            custos = [self.avaliar_solucao(ind) for ind in populacao]
            melhor_idx = np.argmin(custos)
            if custos[melhor_idx] < self.melhor_custo:
                self.melhor_custo = custos[melhor_idx]
                self.melhor_solucao = populacao[melhor_idx]
            
            # SA: Aceitar vizinhos
            for ind in populacao:
                vizinho = self.gerar_vizinho(ind)
                custo_viz = self.avaliar_solucao(vizinho)
                if custo_viz < self.avaliar_solucao(ind) or random.random() < math.exp((self.avaliar_solucao(ind) - custo_viz) / 100):  # Temperatura simplificada
                    ind = vizinho
            
            # ACO: Atualizar feromônios (simplificado)
            for job_id in feromonios:
                feromonios[job_id] *= 0.9  # Evaporação
            # Reforçar melhores (simplificado, não implementado totalmente)
            
            # GA: Cruzamento e mutação (simplificado)
            nova_pop = []
            for _ in range(len(populacao)):
                pai1, pai2 = random.sample(populacao, 2)
                filho = pai1.copy()  # Cruzamento simples
                if random.random() < 0.1:  # Mutação
                    filho = self.gerar_vizinho(filho)
                nova_pop.append(filho)
            populacao = nova_pop
        
        return self.melhor_solucao

# Loop principal: para cada equipe, para cada data
resultados = []
tabela_servicos = []  # Lista para coletar dados da tabela

for idx, equipe in df_equipes.iterrows():
    data_ref = equipe['dt_ref']
    tecnicos, comerciais = filtrar_servicos(df_tecnicos, df_comerciais, equipe['equipe'], data_ref)
    
    if tecnicos.empty and comerciais.empty:
        continue
    
    vroom_input, job_id_to_numos = preparar_vroom_input(equipe, tecnicos, comerciais, data_ref, vehicle_id=idx)
    if vroom_input is None:
        continue  # Pular se input inválido
    
    # Usar metaheurística para otimizar
    meta = MetaHeuristica(vroom_input, tecnicos, comerciais, job_id_to_numos, num_iter=50)
    melhor_input = meta.otimizacao_hibrida()
    
    # Obter solução final
    solucao = chamar_vroom(melhor_input)
    if solucao:
        resultados.append({
            'equipe': equipe['equipe'],
            'data': data_ref,
            'solucao': solucao
        })
        
        # Processar a rota para extrair tabela
        for route in solucao['routes']:
            for step in route['steps']:
                if step.get('type') == 'job':
                    job_id = step['job']
                    arrival_ts = step['arrival']  # Timestamp Unix
                    service_time = step['service']  # Em segundos
                    
                    # Usar mapeamento para obter NUMOS
                    numos = job_id_to_numos[job_id]
                    if numos in tecnicos['NUMOS'].values:
                        tipo = 'tecnico'
                        df_servico = tecnicos
                    elif numos in comerciais['NUMOS'].values:
                        tipo = 'comercial'
                        df_servico = comerciais
                    else:
                        continue
                    
                    # Buscar linha do serviço
                    linha = df_servico[df_servico['NUMOS'] == numos]
                    if not linha.empty:
                        tabela_servicos.append({
                            'equipe': equipe['equipe'],
                            'data': data_ref,
                            'numos': numos,
                            'tipo': tipo,
                            'linha': linha['Linha'].iloc[0],
                            'inicio': arrival_ts,
                            'fim': arrival_ts + service_time
                        })
resultados_df = pd.DataFrame(resultados)
tabela_servicos_df = pd.DataFrame(tabela_servicos)

#Exibir resultados
print(resultados_df.head(5))
print(tabela_servicos_df.head(5))