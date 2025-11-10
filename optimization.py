import copy
from datetime import datetime
import random
import numpy as np
from vroom_interface import chamar_vroom

def calcular_penalizacao(linha, tipo_servico, prazo_verificado=None, prazo_regulatorio=None, turno_inicio_dt=None):
    if tipo_servico == 'tecnico':
        dh_inicio_falha = linha['DH_INICIO']
        if turno_inicio_dt:
            duracao_segundos = (turno_inicio_dt - dh_inicio_falha).total_seconds()
        else:
            duracao_segundos = (linha['DH_FINAL'] - dh_inicio_falha).total_seconds()
        
        duracao = max(0, duracao_segundos) / 3600
        eusd = linha['EUSD']
        return duracao * (eusd / 730) * 34
    
    elif tipo_servico == 'comercial':
        eusd = linha['EUSD']
        if prazo_regulatorio and prazo_verificado:
            log_term = np.log(prazo_verificado / prazo_regulatorio) if prazo_verificado > prazo_regulatorio else 0
            return 120 + 34 * eusd * log_term
        return 120

class MetaHeuristica:
    def __init__(self, vroom_input_base, tecnicos, comerciais, job_id_to_numos, num_iter=10, equipe_linha=None):
        self.vroom_input_base = vroom_input_base
        self.tecnicos = tecnicos
        self.comerciais = comerciais
        self.job_id_to_numos = job_id_to_numos
        self.equipe_linha = equipe_linha
    
    def avaliar_solucao(self, solucao_input):
        """
        MODIFICADO (V16):
        - Se 0 jobs forem enviados, cria manualmente uma resposta vazia 
          em vez de chamar o VROOM (que causa o erro 400).
        """
        
        total_jobs_input = len(solucao_input.get('jobs', []))
        
        # --- INÍCIO DA MODIFICAÇÃO (V16) ---
        if total_jobs_input == 0:
            # VROOM retorna 400 se chamado com 'jobs: []'.
            # Criamos manualmente uma resposta JSON vazia e válida.
            resposta_vazia_manual = {
                'summary': {'cost': 0, 'unassigned': 0, 'delivery': [0], 'service': 0, 'duration': 0, 'waiting_time': 0},
                'routes': []
            }
            return (0, resposta_vazia_manual)
        # --- FIM DA MODIFICAÇÃO ---

        resposta = chamar_vroom(solucao_input)
        
        if resposta:
            custo = resposta['summary']['cost']
            
            jobs_nao_alocados = resposta['summary'].get('unassigned', 0)
            
            # Lógica V10: Se VROOM rejeitou TUDO, é uma falha (inf)
            if jobs_nao_alocados == total_jobs_input and total_jobs_input > 0:
                return (float('inf'), None)
            
            custo += 1000000 * jobs_nao_alocados

            for route in resposta['routes']:
                for step in route['steps']:
                    if step.get('type') == 'job':
                        job_id = step['job']
                        arrival_ts = step['arrival']
                        service_ts = step['service']
                        numos = self.job_id_to_numos.get(job_id)
                        if numos is None: continue
                        
                        if numos in self.tecnicos['NUMOS'].values:
                            tipo = 'tecnico'
                            df_servico = self.tecnicos
                        elif numos in self.comerciais['NUMOS'].values:
                            tipo = 'comercial'
                            df_servico = self.comerciais
                        else:
                            continue
                            
                        linha = df_servico[df_servico['NUMOS'] == numos].iloc[0]
                        
                        if tipo == 'tecnico':
                            linha = linha.copy()
                            linha['DH_FINAL'] = datetime.fromtimestamp(arrival_ts + service_ts)
                            turno_inicio = self.equipe_linha['dthaps_ini'] if self.equipe_linha is not None else None
                            penal = calcular_penalizacao(linha, tipo, turno_inicio_dt=turno_inicio) 
                        
                        elif tipo == 'comercial':
                            prazo_regulatorio = (linha['DATA_VENC'] - linha['DATA_SOL']).total_seconds() / (3600 * 24)
                            prazo_verificado = (datetime.fromtimestamp(arrival_ts) - linha['DATA_SOL']).total_seconds() / (3600 * 24)
                            penal = calcular_penalizacao(linha, tipo, prazo_verificado, prazo_regulatorio)
                            
                        custo += penal
            
            return (custo, resposta)
        
        return (float('inf'), None) 
    
    def otimizacao_hibrida(self):
        """
        Lógica V10: "Greedy Iterative Reduction"
        """
        
        jobs_priorizados = self.vroom_input_base.get('jobs', [])
        
        if not jobs_priorizados:
            return self.avaliar_solucao(self.vroom_input_base)[1]

        num_jobs_total = len(jobs_priorizados)
        
        for num_jobs_tentativa in range(num_jobs_total, 0, -1):
            
            input_tentativa = copy.deepcopy(self.vroom_input_base)
            input_tentativa['jobs'] = jobs_priorizados[:num_jobs_tentativa]
            
            custo, resposta = self.avaliar_solucao(input_tentativa)
            
            if custo != float('inf'):
                if resposta and resposta['summary'].get('unassigned', 0) > 0:
                    print(f"    -> (VROOM rejeitou {resposta['summary']['unassigned']} desses {num_jobs_tentativa} jobs de entrada)")
                return resposta 
            else:
                pass 
                
        # (V16) Se o loop falhar (N=1), chama avaliar_solucao com 0 jobs, 
        # que agora retorna a 'resposta_vazia_manual' em vez de erro.
        vazio_input = {**self.vroom_input_base, 'jobs': []}
        return self.avaliar_solucao(vazio_input)[1]