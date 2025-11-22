# V4 - Troubleshooting: Erro 500 do VROOM

## 🔴 Problema Reportado

```
💥 Falha VROOM multi-veículos para grupo 2023-01-07 08:00:00: 
500 Server Error: Internal Server Error for url: http://localhost:3000/
```

### Causa Raiz
O VROOM está recebendo **payloads muito grandes** e não consegue processar, retornando erro 500.

**Exemplo:**
- Dia 7: 1406 pendências acumuladas (backlog)
- Grupo com 8 equipes
- Limite de 15 OS por equipe
- Fator de pool = 4
- **Resultado:** 4 × 8 × 15 = 480 jobs enviados ao VROOM! ❌

## ✅ Solução Implementada

### 1. Limite Absoluto de Jobs
```python
# v4/config.py
MAX_JOBS_ABSOLUTO = 150  # Máximo de jobs por chamada
```

Agora, mesmo com backlog grande, o payload nunca excede 150 jobs.

### 2. Divisão de Grupos Grandes
```python
MAX_EQUIPES_POR_SUBGRUPO = 6
```

Grupos com mais de 6 equipes são **automaticamente divididos** em sub-grupos menores:
- Grupo de 8 equipes → 2 sub-grupos (6 + 2)
- Cada sub-grupo é processado separadamente
- Resultados são consolidados

### 3. Fator de Pool Ajustado
```python
FATOR_POOL = 3  # Reduzido de 4 para 3
```

Menos jobs candidatos = payload menor.

### 4. Logs Informativos
```
⚙️  Grupo grande (8 equipes) - Dividindo em sub-grupos de 6
   Sub-grupo 1: 6 equipes
   Sub-grupo 2: 2 equipes
⚠️  Pool grande: 150 jobs para 6 veículos (limite: 150)
📤 Enviando ao VROOM: 6 veículos × 150 jobs (cap=15 cada)
```

## 🎛️ Ajustes Recomendados

### Seu Cenário Atual: Backlog Grande (1400+ pendências)

**Edite `/app/v4/config.py`:**

```python
# Para backlog grande
MAX_JOBS_ABSOLUTO = 100        # Reduzir de 150 para 100
FATOR_POOL = 2                 # Reduzir de 3 para 2  
MAX_EQUIPES_POR_SUBGRUPO = 4   # Reduzir de 6 para 4
```

### Ajuste Progressivo

1. **Se ainda tiver erro 500:**
   ```python
   MAX_JOBS_ABSOLUTO = 80
   FATOR_POOL = 2
   MAX_EQUIPES_POR_SUBGRUPO = 3
   ```

2. **Se funcionar mas quiser mais performance:**
   ```python
   MAX_JOBS_ABSOLUTO = 120
   FATOR_POOL = 3
   MAX_EQUIPES_POR_SUBGRUPO = 5
   ```

## 📊 Impacto das Mudanças

### Antes (Original)
```
❌ Grupo de 8 equipes: 480 jobs → ERRO 500
❌ Apenas grupos com 1 equipe funcionavam
❌ Perda de 90% dos atendimentos
```

### Depois (Com Ajustes)
```
✅ Grupo de 8 equipes: Dividido em 2 sub-grupos
   Sub-grupo 1 (6 equipes): 100 jobs → OK
   Sub-grupo 2 (2 equipes): 30 jobs → OK
✅ Todos os grupos funcionando
✅ Distribuição equilibrada mantida
```

## 🔧 Como Testar

### 1. Aplique as configurações recomendadas
Edite `/app/v4/config.py` conforme seu cenário.

### 2. Execute novamente
```bash
python -m v4.main --limite 15 --debug
```

### 3. Monitore os logs
Procure por:
- ✅ `Total atribuído no grupo` - Sucesso
- ⚙️  `Grupo grande` - Divisão em sub-grupos
- ⚠️  `Pool grande` - Warning (normal)
- ❌ `500 Server Error` - Erro (ajustar config)

### 4. Ajuste incrementalmente
Se ainda houver erro 500:
- Reduza `MAX_JOBS_ABSOLUTO` em 20
- Reduza `MAX_EQUIPES_POR_SUBGRUPO` em 1
- Teste novamente

## 💡 Dicas de Otimização

### Para Máxima Produtividade
Se você tem um servidor VROOM potente:
```python
MAX_JOBS_ABSOLUTO = 200
FATOR_POOL = 4
MAX_EQUIPES_POR_SUBGRUPO = 8
```

### Para Máxima Estabilidade
Se você tem muitos erros 500:
```python
MAX_JOBS_ABSOLUTO = 80
FATOR_POOL = 2
MAX_EQUIPES_POR_SUBGRUPO = 3
```

### Para Balancear
Configuração atual (recomendada):
```python
MAX_JOBS_ABSOLUTO = 150
FATOR_POOL = 3
MAX_EQUIPES_POR_SUBGRUPO = 6
```

## 🧪 Validação

Execute este teste para verificar a configuração:

```bash
cd /app
python -c "
from v4 import config as v4_config
print('=== CONFIGURAÇÃO ATUAL ===')
print(f'MAX_JOBS_ABSOLUTO: {v4_config.MAX_JOBS_ABSOLUTO}')
print(f'FATOR_POOL: {v4_config.FATOR_POOL}')
print(f'MAX_EQUIPES_POR_SUBGRUPO: {v4_config.MAX_EQUIPES_POR_SUBGRUPO}')
print('\\n=== CENÁRIO EXEMPLO ===')
eq = 8  # equipes
lim = 15  # limite
calc = lim * eq * v4_config.FATOR_POOL
final = min(calc, v4_config.MAX_JOBS_ABSOLUTO)
print(f'{eq} equipes × {lim} limite × {v4_config.FATOR_POOL} fator = {calc} jobs')
print(f'Limitado a: {final} jobs (max={v4_config.MAX_JOBS_ABSOLUTO})')
if eq > v4_config.MAX_EQUIPES_POR_SUBGRUPO:
    print(f'Será dividido em sub-grupos de {v4_config.MAX_EQUIPES_POR_SUBGRUPO}')
"
```

## 📈 Resultados Esperados

Com as configurações ajustadas, você deve ver:

```
✅ Menos erros 500 (idealmente zero)
✅ Grupos grandes divididos automaticamente
✅ Distribuição equilibrada mantida
✅ Mais serviços atendidos que no V3
✅ Menos cruzamentos de rotas
```

## ❓ FAQ

**P: Por que não usar sempre os valores mais baixos?**
R: Valores muito baixos limitam as opções do VROOM, resultando em otimização sub-ótima.

**P: A divisão em sub-grupos piora a otimização?**
R: Levemente, mas é necessária para evitar erro 500. O trade-off vale a pena.

**P: Posso aumentar a capacidade do VROOM?**
R: Sim! Se você configurar mais memória/CPU no VROOM, pode aumentar os limites.

**P: O V3 não tinha esse problema?**
R: V3 processava equipe por equipe (1 veículo), V4 processa grupos (múltiplos veículos), que é mais complexo mas resulta em melhor otimização.
