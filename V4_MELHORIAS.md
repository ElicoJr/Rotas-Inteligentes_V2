# V4 - Otimização com Restrições de Capacidade

## 🎯 Objetivo
Reduzir cruzamentos de rotas e aumentar o número total de serviços atendidos através de uma distribuição mais equilibrada entre as equipes.

## 🔧 Implementação

### Antes (V4 Original)
```python
vehicle = {
    "id": v_id,
    "start": [lon, lat],
    "end": [lon, lat],
    "time_window": [0, horizon]
}

job = {
    "id": job_id,
    "location": [lon, lat],
    "service": service_sec
}
```

**Problema:** VROOM podia atribuir quantos serviços quisesse para cada equipe, resultando em:
- ❌ Algumas equipes sobrecarregadas (20+ serviços)
- ❌ Outras equipes subutilizadas (5- serviços)
- ❌ Rotas cruzadas porque equipes invadiam territórios
- ❌ Menos serviços atendidos no total

### Depois (V4 com Capacidade)
```python
vehicle = {
    "id": v_id,
    "start": [lon, lat],
    "end": [lon, lat],
    "time_window": [0, horizon],
    "capacity": [limite_por_equipe]  # ✅ NOVO
}

job = {
    "id": job_id,
    "location": [lon, lat],
    "service": service_sec,
    "delivery": [1]  # ✅ NOVO
}
```

**Benefícios:**
- ✅ Cada equipe trabalha próximo do limite (ex: 15 serviços)
- ✅ Distribuição equilibrada entre equipes
- ✅ Menos cruzamentos (VROOM distribui espacialmente)
- ✅ Mais serviços atendidos no total
- ✅ Aproveitamento máximo da frota

## 📊 Exemplo de Impacto

### Cenário: 4 equipes com limite de 15 OS cada

**Sem capacidade:**
```
Equipe 1: 25 OS ⚠️ (sobrecarga)
Equipe 2: 18 OS
Equipe 3: 8 OS  ⚠️ (ociosidade)
Equipe 4: 5 OS  ⚠️ (ociosidade)
---
Total: 56 OS
Rotas: Muito cruzadas ❌
```

**Com capacidade:**
```
Equipe 1: 15 OS ✅
Equipe 2: 15 OS ✅
Equipe 3: 14 OS ✅
Equipe 4: 15 OS ✅
---
Total: 59 OS (+5%)
Rotas: Melhor distribuídas ✅
```

## 🚀 Como Usar

1. **Certifique-se que o VROOM está rodando:**
   ```bash
   docker-compose -f vroom-local/docker-compose.yml up -d
   ```

2. **Execute o V4 com o limite desejado:**
   ```bash
   python -m v4.main --limite 15 --debug
   ```

3. **Compare com V3:**
   ```bash
   # V3
   python -m v3.main --limite 15 --debug
   
   # V4
   python -m v4.main --limite 15 --debug
   ```

## 📈 Métricas para Comparar

### Total de Serviços Atendidos
```bash
# V3
ls results_v3/*.parquet | wc -l
# Contar total de linhas

# V4
ls results_v4/*.parquet | wc -l
# Contar total de linhas
```

### Distribuição por Equipe
O V4 agora exibe no log:
```
✅ Total atribuído no grupo: 59 OS | Distribuição: {'PVOSN66': 15, 'PVLSN07': 15, 'DE-PVH06': 14, 'DE-PVH09': 15}
```

### Visualização de Rotas
Use o notebook Jupyter fornecido anteriormente para visualizar as rotas e verificar a redução de cruzamentos.

## 🔍 Entendendo os Logs

### Novo Log de Distribuição
```
🔁 Grupo inicio_turno = 2023-01-03 08:00:00 com 4 equipes
   ✅ Total atribuído no grupo: 59 OS | Distribuição: {'Equipe1': 15, 'Equipe2': 15, ...}
```

Isso mostra:
- Quantas OS foram atribuídas no total para o grupo
- Como as OS foram distribuídas entre as equipes
- Se a distribuição está equilibrada

## ⚙️ Parâmetros Ajustáveis

### Limite por Equipe
```bash
python -m v4.main --limite 20  # Aumenta capacidade
python -m v4.main --limite 10  # Reduz capacidade
```

### Fator de Pool (no código)
```python
# Em v4/main.py, linha ~193
fator_pool = 4  # Aumentar para mais candidatos
```

Controla quantas OS são pré-selecionadas antes de enviar ao VROOM:
- Fator = 4: 4 × (num_equipes × limite) OS candidatas
- Maior = mais opções, mas mais lento
- Menor = mais rápido, mas pode perder boas opções

## 🎓 Teoria: Por que Capacidade Reduz Cruzamentos?

1. **Força Distribuição Espacial:**
   - Quando uma equipe atinge o limite, o VROOM precisa usar outra
   - Naturalmente leva a atribuição em clusters geográficos

2. **Evita Monopolização:**
   - Sem limite: uma equipe pode "roubar" serviços de áreas distantes
   - Com limite: cada equipe fica em sua região natural

3. **Otimização Global:**
   - VROOM otimiza considerando todas as equipes simultaneamente
   - Restrições de capacidade guiam a solução para melhor balanço

## 📝 Notas Técnicas

- A capacidade é dimensional: `[limite_por_equipe]` (lista de 1 elemento)
- Cada job consome `[1]` de capacidade (delivery)
- VROOM respeita automaticamente essa restrição
- Se não houver solução viável, VROOM retorna erro (tratado no código)

## 🐛 Troubleshooting

### "VROOM não retornou rotas"
- Verifique se VROOM está rodando: `curl http://localhost:3000/`
- Verifique se há jobs e veículos suficientes
- Aumente o `fator_pool` se o pool estiver muito restrito

### "Equipes ainda com distribuição desigual"
- Verifique se todas as equipes têm a mesma capacidade
- Verifique se há serviços suficientes para distribuir
- Considere ajustar o `limite_por_equipe`

### "Menos serviços atendidos que no V3"
- Improvável com essa implementação
- Verifique os logs de cada grupo
- Compare o total de pendências elegíveis
