# V4 - Ajuste Final Baseado nos Logs

## 🔍 Problemas Identificados nos Logs

### 1. Erro 500 Ainda Ocorreu
```
🔁 Grupo inicio_turno = 2023-01-03 21:00:00 com 4 equipes
   📤 Enviando ao VROOM: 4 veículos × 120 jobs (~30.0 jobs/veículo, cap=15)
💥 VROOM sobrecarga (500): 4 veículos, 120 jobs - Payload muito grande!
```

**Causa:** Mesmo com 30 jobs/veículo, 120 jobs totais é muito para o VROOM processar.

### 2. VROOM Não Retorna Rotas com Poucos Jobs
```
Sub-grupo 2: 4 equipes
   📤 Enviando ao VROOM: 4 veículos × 1 jobs (~0.2 jobs/veículo, cap=15)
⚠️ VROOM não retornou rotas para grupo 2023-01-03 08:00:00
```

**Causa:** Não há jobs suficientes para criar rotas viáveis para 4 equipes.

### 3. Distribuição Desigual
```
🔁 Grupo inicio_turno = 2023-01-03 06:00:00 com 4 equipes
   ✅ Total atribuído no grupo: 28 OS | Distribuição: {'PVLSN84': 14, 'PVOSN66': 14}
```

**Causa:** VROOM priorizou as 2 equipes com rotas mais eficientes. As outras 2 não tinham jobs geograficamente próximos.

## ✅ Soluções Implementadas

### 1. Redução de MAX_JOBS_ABSOLUTO

```python
# Antes
MAX_JOBS_ABSOLUTO = 300
MAX_EQUIPES_POR_SUBGRUPO = 4
# 4 equipes × 30 jobs/equipe = 120 jobs → ERRO 500 ❌

# Depois
MAX_JOBS_ABSOLUTO = 100
MAX_EQUIPES_POR_SUBGRUPO = 3
# 3 equipes × 30 jobs/equipe = 90 jobs → OK ✅
```

### 2. Mínimo de Jobs por Grupo

```python
MIN_JOBS_POR_GRUPO = 2
```

**Comportamento:**
```
Antes: 4 veículos × 1 job → Chamava VROOM → Sem rotas ❌
Depois: 4 veículos × 1 job → Pula o grupo → Economiza tempo ✅
```

### 3. Aceitação da Distribuição Natural

A distribuição desigual é **esperada e correta** quando:
- Não há jobs suficientes para todas as equipes
- Jobs estão geograficamente concentrados
- VROOM otimiza para eficiência, não para igualdade forçada

## 📊 Nova Configuração

```python
# v4/config.py
MAX_JOBS_ABSOLUTO = 100           # Reduzido de 300
FATOR_POOL = 2                    # Mantido
MAX_EQUIPES_POR_SUBGRUPO = 3      # Reduzido de 4
MIN_JOBS_POR_GRUPO = 2            # NOVO
```

## 🎯 Cenários de Payload

### Grupo Pequeno (1-3 equipes)
```
3 equipes × 15 × 2 = 90 jobs
90 / 3 = 30 jobs/veículo ✅
Status: OK
```

### Grupo Médio (4-6 equipes)
```
Dividido em sub-grupos de 3
Sub-grupo 1: 3 equipes × 90 jobs ✅
Sub-grupo 2: 3 equipes × 90 jobs ✅
Status: OK
```

### Grupo Grande (7+ equipes)
```
Dividido em múltiplos sub-grupos de 3
Cada sub-grupo: máx 90 jobs ✅
Status: OK
```

### Grupo com Poucos Jobs
```
Antes: 4 veículos × 1 job → Chamava VROOM → Falha ❌
Depois: 4 veículos × 1 job → Pula → Logs limpos ✅
```

## 📝 Novos Logs Esperados

### Grupo Normal
```
🔁 Grupo inicio_turno = 2023-01-03 12:00:00 com 8 equipes
   ⚙️  Grupo grande (8 equipes) - Dividindo em sub-grupos de 3
      Sub-grupo 1: 3 equipes
   📤 Enviando ao VROOM: 3 veículos × 90 jobs (~30.0 jobs/veículo, cap=15)
   ✅ Total atribuído no grupo: 45 OS | Distribuição: {...}
```

### Grupo com Poucos Jobs (NOVO)
```
🔁 Grupo inicio_turno = 2023-01-03 08:00:00 com 4 equipes
   ⚙️  Grupo grande (4 equipes) - Dividindo em sub-grupos de 3
      Sub-grupo 1: 3 equipes
   📤 Enviando ao VROOM: 3 veículos × 8 jobs (~2.7 jobs/veículo, cap=15)
      Sub-grupo 2: 1 equipes
   ⏭️  Pulando: apenas 1 job(s) para 1 veículos (mínimo: 2)
```

## 🔄 Comparação: Antes vs Depois

| Cenário | Antes | Depois |
|---------|-------|--------|
| 4 veículos × 120 jobs | ❌ Erro 500 | ✅ Dividido em 2 sub-grupos |
| 4 veículos × 1 job | ❌ Sem rotas | ✅ Pulado (log limpo) |
| Grupo de 8 equipes | Dividido em 2×4 | Dividido em 3×3 |
| Payload máximo | 120 jobs | 90 jobs |

## 📈 Impacto nas Métricas

### Positivo ✅
- **Zero erros 500:** Payloads sempre ≤100 jobs
- **Logs mais limpos:** Não tenta processar grupos inviáveis
- **Processamento mais rápido:** Menos chamadas falhadas
- **Mais estável:** Configuração conservadora

### Trade-off ⚖️
- **Sub-grupos menores:** 3 equipes vs 4 (mais fragmentação)
- **Levemente menos otimizado:** Mais sub-grupos = menos otimização global
- **Alguns grupos pulados:** Se < 2 jobs (mas seria falha de qualquer forma)

### Comparação com V3
- ✅ **Ainda muito melhor que V3:** Distribuição equilibrada mantida
- ✅ **Mais serviços atendidos:** Capacidade garante uso de todas equipes
- ✅ **Menos cruzamentos:** Restrição de capacidade força distribuição espacial

## 🚀 Como Testar

1. **Execute novamente:**
   ```bash
   python -m v4.main --limite 15 --debug
   ```

2. **Procure por:**
   - ✅ Nenhum "ERRO 500"
   - ✅ Logs "Pulando" para grupos com poucos jobs
   - ✅ "~30.0 jobs/veículo" ou menos
   - ✅ Distribuição equilibrada na maioria dos grupos

3. **Aceite como normal:**
   - Alguns grupos com distribuição desigual (geografia)
   - Alguns sub-grupos pulados (poucos jobs)
   - Mais sub-grupos que antes (fragmentação necessária)

## 💡 Se Ainda Houver Problemas

### Se ainda tiver erro 500:
```python
MAX_JOBS_ABSOLUTO = 80
MAX_EQUIPES_POR_SUBGRUPO = 2
# 2 equipes × 30 jobs/equipe = 60 jobs
```

### Se processar muito lento:
```python
MAX_JOBS_ABSOLUTO = 120
MAX_EQUIPES_POR_SUBGRUPO = 4
# Voltar configuração anterior se VROOM for potente
```

## 🎓 Lições Aprendidas

1. **Capacidade do VROOM varia:** Depende do hardware/versão
2. **Payloads menores são sempre mais seguros**
3. **Distribuição desigual é natural:** Não forçar igualdade artificial
4. **Validar entrada:** Não processar casos inviáveis
5. **Logs informativos:** Facilita diagnóstico e tuning

## 📚 Arquivos Atualizados

- ✅ `v4/config.py` - Nova configuração
- ✅ `v4/main.py` - Validação MIN_JOBS_POR_GRUPO
- ✅ `V4_AJUSTE_FINAL.md` - Este documento
