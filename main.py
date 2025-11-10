import multiprocessing as mp
from data_loader import load_data, prepare_columns
from vroom_interface import (
    filtrar_servicos, preparar_jobs, preparar_vroom_input,
    chamar_vroom, chunks, MAX_JOBS, validar_e_limpar_lote
)
from optimization import MetaHeuristica
import pandas as pd
import time
import concurrent.futures
import numpy as np 

# (Função processar_lote SEM ALTERAÇÕES - V14)
def processar_lote(batch, equipe, tecnicos, comerciais, job_id_to_numos, data_ref, vehicle_id):
    """
    MODIFICADO (V14):
    - Passa a 'equipe' (equipe_linha) para a MetaHeuristica 
      para o cálculo de penalidade.
    """
    batch = validar_e_limpar_lote(batch)
    if not batch:
        return None
    
    vroom_input = preparar_vroom_input(equipe, batch, data_ref, vehicle_id)
    if vroom_input is None:
        return None
    
    meta = MetaHeuristica(vroom_input, tecnicos, comerciais, job_id_to_numos, 
                          num_iter=10, equipe_linha=equipe) 
    
    solucao = meta.otimizacao_hibrida()
    
    if solucao:
        tabela_servicos = []
        for route in solucao['routes']:
            for step in route['steps']:
                if step.get('type') == 'job':
                    job_id = step['job']
                    arrival_ts = step['arrival']
                    service_time = step['service']
                    numos = job_id_to_numos[job_id]
                    if numos in tecnicos['NUMOS'].values:
                        tipo = 'tecnico'
                        df_servico = tecnicos
                    elif numos in comerciais['NUMOS'].values:
                        tipo = 'comercial'
                        df_servico = comerciais
                    else:
                        continue
                    linha = df_servico[df_servico['NUMOS'] == numos].iloc[0]
                    tabela_servicos.append({
                        'equipe': equipe['equipe'],
                        'data': data_ref,
                        'numos': numos,
                        'tipo': tipo,
                        'inicio': arrival_ts,
                        'fim': arrival_ts + service_time
                    })
        return {'solucao': solucao, 'tabela': tabela_servicos}
    
    return None


# --- INÍCIO DA MODIFICAÇÃO (V17) ---
# Definindo o limite máximo de serviços que uma equipe pode ter
MAX_SERVICOS_POR_EQUIPE = 12
# --- FIM DA MODIFICAÇÃO ---

# (Função processar_equipe_sequencial ATUALIZADA - V17)
def processar_equipe_sequencial(equipe_linha, df_tecnicos_pendentes, df_comerciais_pendentes, script_start_time):
    """
    Processa uma ÚNICA equipe, avaliando TODOS os serviços pendentes.
    
    MODIFICADO (V17):
    - Remove o 'ThreadPool' (V12)
    - Processa lotes (batches) em SÉRIE (um por um)
    - Para de processar lotes quando 'MAX_SERVICOS_POR_EQUIPE' (12) é atingido.
    """
    
    print(f"\n--- Processando equipe {equipe_linha['equipe']} (Início: {equipe_linha['dthaps_ini']}) ---")
    resultados_equipe = []
    tabela_servicos_equipe = []
    data_ref = equipe_linha['dt_ref']
    
    # 1. Filtra serviços que a equipe pode atender
    tecnicos_aptos, comerciais_aptos = filtrar_servicos(
        df_tecnicos_pendentes, df_comerciais_pendentes, equipe_linha
    )
    
    if tecnicos_aptos.empty and comerciais_aptos.empty:
        print(f"⚠️ Nenhuma tarefa apta para equipe {equipe_linha['equipe']} (pós-filtro de turno). Pulando.")
        return resultados_equipe, tabela_servicos_equipe
    
    # 2. Preparar TODOS os jobs aptos
    jobs, job_id_to_numos = preparar_jobs(tecnicos_aptos, comerciais_aptos)
    if not jobs:
        print(f"⚠️ Nenhum job válido (lat/lon) para equipe {equipe_linha['equipe']}. Pulando.")
        return resultados_equipe, tabela_servicos_equipe
    
    print(f"ℹ️  Equipe {equipe_linha['equipe']}: {len(jobs)} jobs aptos (pré-filtro de priorização).")
    
    # 3. Ordenar os jobs
    def get_start_time(job):
        try:
            return job['time_windows'][0][0]
        except (IndexError, KeyError, TypeError):
            return float('inf')
            
    jobs.sort(key=get_start_time) 
    jobs.sort(key=lambda j: j.get('priority', 0), reverse=True)
    
    # 4. Dividir em lotes
    job_batches = list(chunks(jobs, MAX_JOBS))
    
    print(f"📋 Equipe {equipe_linha['equipe']}: {len(jobs)} jobs aptos, priorizados e divididos em {len(job_batches)} lotes de {MAX_JOBS}.")

    # --- INÍCIO DA MODIFICAÇÃO (V17) ---
    # 5. Processar lotes em SÉRIE, até atingir o limite
    
    if not job_batches:
        print(f"⚠️ Nenhum lote criado para {equipe_linha['equipe']}.")
        return resultados_equipe, tabela_servicos_equipe
        
    print(f"  -> Otimizando lotes (máx {MAX_SERVICOS_POR_EQUIPE} serviços) para esta equipe...")
    
    total_servicos_atribuidos_equipe = 0

    # (Removemos o ThreadPoolExecutor)
    for i, batch in enumerate(job_batches):
        
        # Otimiza o lote atual
        result = processar_lote(
            batch, 
            equipe_linha, 
            tecnicos_aptos,
            comerciais_aptos, 
            job_id_to_numos, 
            data_ref, 
            equipe_linha.name
        )
        
        if result:
            servicos_neste_lote = len(result['tabela'])
            print(f"  -> Lote {i+1}/{len(job_batches)} processado. {servicos_neste_lote} serviços atribuídos.")
            
            resultados_equipe.append({
                'equipe': equipe_linha['equipe'],
                'data': data_ref,
                'solucao': result['solucao']
            })
            tabela_servicos_equipe.extend(result['tabela'])
            total_servicos_atribuidos_equipe += servicos_neste_lote
            
            # 6. Verifica se atingiu o limite MÁXIMO
            if total_servicos_atribuidos_equipe >= MAX_SERVICOS_POR_EQUIPE:
                print(f"  -> Limite de {MAX_SERVICOS_POR_EQUIPE} serviços atingido/ultrapassado. Parando de alocar para esta equipe.")
                break # Para de processar mais lotes para esta equipe
                
        else:
            print(f"  -> Lote {i+1}/{len(job_batches)} processado. 0 serviços atribuídos (Meta-heurística não encontrou solução viável).")
    
    # --- FIM DA MODIFICAÇÃO ---

    elapsed_total = time.time() - script_start_time
    print(f"✅ Equipe {equipe_linha['equipe']} concluída. Total de serviços atribuídos: {len(tabela_servicos_equipe)}. (Tempo total: {elapsed_total:.2f}s)")
    
    return resultados_equipe, tabela_servicos_equipe

if __name__ == '__main__':
    # (O loop principal permanece como na V13)
    start_time = time.time()
    print("🚀 Iniciando script de roteirização (Simulação Sequencial por Bloco de Início)...")
    
    df_equipes_full, df_tecnicos_full, df_comerciais_full = load_data()
    df_equipes_full, df_tecnicos_full, df_comerciais_full = prepare_columns(
        df_equipes_full, df_tecnicos_full, df_comerciais_full
    )
    print("DataFrames completos carregados.")

    datas_simulacao = sorted(df_equipes_full['dt_ref'].dt.date.unique())
    print(f"📅 Simulação de {datas_simulacao[0]} até {datas_simulacao[-1]}.")

    servicos_atribuidos_numos = set()
    todos_resultados_finais = []
    toda_tabela_servicos_finais = []

    n_dias_total = len(datas_simulacao)
    for dia_idx, dia_simulacao in enumerate(datas_simulacao):
        print("\n========================================================")
        print(f"☀️ Processando dia: {dia_simulacao} ({dia_idx + 1} de {n_dias_total})")
        print("========================================================")
        dia_sim_start_time = time.time()

        equipes_do_dia_full = df_equipes_full[
            (df_equipes_full['dt_ref'].dt.date == dia_simulacao) &
            (pd.notna(df_equipes_full['dthaps_ini']))
        ].copy()
        
        if equipes_do_dia_full.empty:
            print("... Nenhuma equipe trabalhando neste dia. Pulando.")
            continue

        df_tecnicos_pendentes = df_tecnicos_full[
            (df_tecnicos_full['DH_INICIO'].dt.date <= dia_simulacao) &
            (~df_tecnicos_full['NUMOS'].isin(servicos_atribuidos_numos))
        ].copy()
        
        df_comerciais_pendentes = df_comerciais_full[
            (df_comerciais_full['DATA_SOL'].dt.date <= dia_simulacao) &
            (~df_comerciais_full['NUMOS'].isin(servicos_atribuidos_numos))
        ].copy()
        
        if df_tecnicos_pendentes.empty and df_comerciais_pendentes.empty:
            print("... Nenhum serviço pendente para este dia. Pulando.")
            continue
            
        print(f"🔄 {len(equipes_do_dia_full)} equipes encontradas | {len(df_tecnicos_pendentes)} téc. pendentes | {len(df_comerciais_pendentes)} com. pendentes")

        horarios_inicio_distintos = sorted(equipes_do_dia_full['dthaps_ini'].unique())
        
        n_blocos = len(horarios_inicio_distintos)
        qtd_blocosProcessados = 0
        print(f"   (Agrupadas em {n_blocos} blocos de início distintos)")
        
        servicos_atribuidos_hoje_set = set()

        for horario_inicio_bloco in horarios_inicio_distintos:
            
            qtd_blocosProcessados += 1
            
            equipes_do_bloco = equipes_do_dia_full[
                equipes_do_dia_full['dthaps_ini'] == horario_inicio_bloco
            ]
            
            equipes_do_bloco_shuffled = equipes_do_bloco.sample(frac=1)
            
            print(f"\n--- Processando Bloco {qtd_blocosProcessados}/{n_blocos} (Início: {horario_inicio_bloco}, {len(equipes_do_bloco_shuffled)} equipes) ---")

            for idx, equipe_linha in equipes_do_bloco_shuffled.iterrows():
                
                resultados_equipe, tabela_servicos_equipe = processar_equipe_sequencial(
                    equipe_linha, 
                    df_tecnicos_pendentes, 
                    df_comerciais_pendentes,
                    start_time 
                )

                if tabela_servicos_equipe:
                    todos_resultados_finais.extend(resultados_equipe)
                    toda_tabela_servicos_finais.extend(tabela_servicos_equipe)
                    
                    numos_atribuidos_agora = {item['numos'] for item in tabela_servicos_equipe}
                    servicos_atribuidos_hoje_set.update(numos_atribuidos_agora)
                    
                    df_tecnicos_pendentes = df_tecnicos_pendentes[
                        ~df_tecnicos_pendentes['NUMOS'].isin(numos_atribuidos_agora)
                    ]
                    df_comerciais_pendentes = df_comerciais_pendentes[
                        ~df_comerciais_pendentes['NUMOS'].isin(numos_atribuidos_agora)
                    ]
                    
                    print(f"... {len(df_tecnicos_pendentes)} téc. restantes | {len(df_comerciais_pendentes)} com. restantes para o próximo.")
            
        servicos_atribuidos_numos.update(servicos_atribuidos_hoje_set)
        
        dia_sim_elapsed = time.time() - dia_sim_start_time
        print(f"\n✅ Dia {dia_simulacao} concluído. {len(servicos_atribuidos_hoje_set)} jobs atribuídos hoje. Tempo: {dia_sim_elapsed:.2f}s")
        print(f"📈 Total de serviços atribuídos na simulação: {len(servicos_atribuidos_numos)}")

    print("\n🎉 Simulação sequencial (por bloco de início) concluída!")
    elapsed_total = time.time() - start_time

    if toda_tabela_servicos_finais:
        resultados_df = pd.DataFrame(todos_resultados_finais)
        tabela_servicos_df = pd.DataFrame(toda_tabela_servicos_finais)
        
        print("📊 Resultados finais consolidados:")
        print(tabela_servicos_df.head(5))
        
        import os
        os.makedirs('results', exist_ok=True)
        
        resultados_df.to_parquet('results/resultados_simulacao_sequencial.parquet', index=False, engine='pyarrow')
        tabela_servicos_df.to_parquet('results/tabela_servicos_simulacao_sequencial.parquet', index=False, engine='pyarrow')
        print("💾 Resultados salvos em 'results/'.")
    else:
        print("⚠️ Nenhum serviço foi atribuído durante toda a simulação.")
    
    print(f"🎉 Script concluído! Tempo total: {elapsed_total:.2f}s.")