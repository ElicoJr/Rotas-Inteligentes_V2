# v4/config.py
"""
Configurações ajustáveis do V4 para otimização de performance
"""

# === LIMITES DE PAYLOAD VROOM ===
# Limite absoluto de jobs por chamada ao VROOM (evita erro 500)
MAX_JOBS_ABSOLUTO = 100

# Fator multiplicador para pré-seleção de candidatos
# Formula: max_jobs_por_veiculo = limite_por_equipe × FATOR_POOL
# Com FATOR_POOL=2 e limite=15: cada veículo recebe até 30 candidatos
FATOR_POOL = 2

# Máximo de equipes por sub-grupo (evita payloads muito grandes)
# Com 3 equipes × 30 jobs/equipe = 90 jobs por sub-grupo (abaixo de 100)
MAX_EQUIPES_POR_SUBGRUPO = 3

# Mínimo de jobs para chamar VROOM (evita erros com poucos jobs)
MIN_JOBS_POR_GRUPO = 2

# === THRESHOLDS DE LOGGING ===
# Exibe warning quando pool de candidatos é maior que este valor
POOL_WARNING_THRESHOLD = 80

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
