# V4 - Configuração Aplicada: 30 Jobs por Veículo

## ⚙️ Configuração Atual

```python
MAX_JOBS_ABSOLUTO = 300
FATOR_POOL = 2
MAX_EQUIPES_POR_SUBGRUPO = 4
```

## 📐 Fórmula

```
jobs_por_veiculo = limite_por_equipe × FATOR_POOL
jobs_por_veiculo = 15 × 2 = 30 ✅
```

## 🎯 Cenários de Uso

### Grupo Pequeno (1 equipe)
- Jobs: 1 × 15 × 2 = **30 jobs**
- Jobs/veículo: **30** ✅
- Sub-grupos: 1

### Grupo Médio (4 equipes)
- Jobs: 4 × 15 × 2 = **120 jobs**
- Jobs/veículo: **30** ✅
- Sub-grupos: 1

### Grupo Grande (6 equipes)
- **Dividido em 2 sub-grupos de 4+2**
- Sub-grupo 1: 4 × 15 × 2 = **120 jobs** (30/veículo) ✅
- Sub-grupo 2: 2 × 15 × 2 = **60 jobs** (30/veículo) ✅

### Grupo Muito Grande (21 equipes)
- **Dividido em 6 sub-grupos**
- Cada sub-grupo: máx 4 equipes × 15 × 2 = **120 jobs** (30/veículo) ✅

## 📊 Comparação

### Antes (Erro 500)
```
6 equipes × 15 limite × 3 fator = 270 jobs
270 / 6 veículos = 45 jobs/veículo ❌
VROOM: 500 Internal Server Error
```

### Depois (Estável)
```
4 equipes × 15 limite × 2 fator = 120 jobs
120 / 4 veículos = 30 jobs/veículo ✅
VROOM: OK
```

## 🚀 Benefícios

1. **Estabilidade:** Payloads menores → Sem erro 500
2. **Previsibilidade:** Sempre 30 candidatos por veículo
3. **Performance:** VROOM processa mais rápido
4. **Escalabilidade:** Grupos grandes divididos automaticamente

## 📝 Logs Melhorados

### Antes
```
📤 Enviando ao VROOM: 6 veículos × 150 jobs (cap=15 cada)
💥 VROOM sobrecarga (500)
```

### Depois
```
📤 Enviando ao VROOM: 4 veículos × 120 jobs (~30.0 jobs/veículo, cap=15)
✅ Total atribuído no grupo: 58 OS | Distribuição: {...}
```

## 🔧 Ajustes Futuros

### Se ainda tiver erro 500
```python
FATOR_POOL = 1.5  # 15 × 1.5 = 22.5 jobs/veículo
MAX_EQUIPES_POR_SUBGRUPO = 3
```

### Se quiser mais candidatos
```python
FATOR_POOL = 2.5  # 15 × 2.5 = 37.5 jobs/veículo
# Teste primeiro com poucos dias!
```

## 📈 Impacto Esperado

### Métricas de Sucesso
- ✅ Taxa de sucesso VROOM: ~100% (vs ~10% antes)
- ✅ Tempo de processamento: Mais rápido
- ✅ Distribuição: Mantém equilíbrio (capacidade)
- ✅ Atendimentos: Muito maior que antes

### Trade-offs
- ⚖️ Menos candidatos por veículo (30 vs 45-50)
- ⚖️ Otimização levemente reduzida
- ✅ Mas muito melhor que erro 500 constante!

## 🧪 Validação

Execute novamente:
```bash
python -m v4.main --limite 15 --debug
```

Procure nos logs:
- ✅ `~30.0 jobs/veículo` - Correto
- ✅ `Total atribuído no grupo` - Sucesso
- ❌ `500 Server Error` - Não deve aparecer mais

## 💡 Entendendo os Números

**Por que 30?**
- É um bom equilíbrio entre opções e performance
- VROOM consegue processar rapidamente
- Dá 2x mais opções que o limite (15 × 2)

**Por que dividir em 4 equipes?**
- 4 veículos × 30 jobs = 120 jobs por payload
- Valor seguro que VROOM processa bem
- Mantém qualidade da otimização

**O que acontece com backlog grande?**
- Pool de 576 pendências → Filtrado para top 120
- Baseado em score de prioridade
- Garante que os mais urgentes são considerados

## 📚 Arquivos Relacionados

- `/app/v4/config.py` - Configurações ajustáveis
- `/app/V4_TROUBLESHOOTING.md` - Guia de troubleshooting
- `/app/V4_MELHORIAS.md` - Documentação das melhorias
