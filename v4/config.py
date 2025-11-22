# v4/config.py
"""
Configurações ajustáveis do V4 para otimização de performance
"""

# === LIMITES DE PAYLOAD VROOM ===
# Limite absoluto de jobs por chamada ao VROOM (evita erro 500)
MAX_JOBS_ABSOLUTO = 150

# Fator multiplicador para pré-seleção de candidatos
# Formula: max_jobs = min(limite_por_equipe × n_equipes × FATOR_POOL, MAX_JOBS_ABSOLUTO)
FATOR_POOL = 3

# Máximo de equipes por sub-grupo (evita payloads muito grandes)
# Grupos maiores são divididos automaticamente
MAX_EQUIPES_POR_SUBGRUPO = 6

# === THRESHOLDS DE LOGGING ===
# Exibe warning quando pool de candidatos é maior que este valor
POOL_WARNING_THRESHOLD = 100

# === AJUSTES RECOMENDADOS POR CENÁRIO ===
"""
CENÁRIO 1: Poucos serviços, muitas equipes
- MAX_JOBS_ABSOLUTO = 100
- FATOR_POOL = 2
- MAX_EQUIPES_POR_SUBGRUPO = 4

CENÁRIO 2: Muitos serviços, poucas equipes  
- MAX_JOBS_ABSOLUTO = 200
- FATOR_POOL = 4
- MAX_EQUIPES_POR_SUBGRUPO = 8

CENÁRIO 3: Backlog muito grande (>1000 pendências)
- MAX_JOBS_ABSOLUTO = 100  # Reduzir
- FATOR_POOL = 2           # Reduzir
- MAX_EQUIPES_POR_SUBGRUPO = 4  # Reduzir

CENÁRIO 4: Performance máxima (VROOM potente)
- MAX_JOBS_ABSOLUTO = 300
- FATOR_POOL = 5
- MAX_EQUIPES_POR_SUBGRUPO = 10
"""
