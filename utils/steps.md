# 🗺️ Servidor OSRM + VROOM (Mapa: north-latest.osm.pbf)

Este guia descreve **todos os passos necessários** para configurar corretamente um servidor de roteirização usando:

- **OSRM** (Open Source Routing Machine)
- **VROOM** (Vehicle Routing Optimization)
- **Docker**
- **Mapa north-latest.osm.pbf**

Configuração testada e validada para:

- Roteamento urbano (Porto Velho, Ji-Paraná, Ariquemes, etc.)
- Matrizes via OSRM
- Roteirização via VROOM
- Uso em ambiente WSL + Windows 10/11

---

# 📁 Estrutura de Pastas Requerida

```
vroom-local/
│
├── osrm-data/
│     ├── norte-latest.osm.pbf
│     ├── norte-latest.osrm
│     ├── norte-latest.osrm.names
│     ├── norte-latest.osrm.geometry
│     ├── norte-latest.osrm.mldgr
│     ├── norte-latest.osrm.partition
│     ├── norte-latest.osrm.cnbg
│     └── ... (arquivos processados pelo OSRM)
│
├── conf/
│     └── config.yml   (arquivo de configuração do VROOM)
│
├── vroom-docker/
│     └── docker-compose.yml (opcional)
│
└── osrm-docker/
      └── docker-compose.yml (opcional)
```

---

# 🛠️ Pré-Requisitos

- Windows 10/11 + WSL (Ubuntu recomendado)
- Docker Desktop instalado
- Porta **5000** livre para OSRM
- Porta **3000** livre para VROOM

No WSL:

```bash
sudo apt update && sudo apt install curl -y
```

---

# 🔽 1. Baixar o mapa north-latest.osm.pbf

Baixe o arquivo:

https://download.geofabrik.de/south-america/brazil/norte-latest.osm.pbf

Coloque dentro de:

```
vroom-local/osrm-data/
```

---

# ⚙️ 2. Processar o mapa com OSRM (EXTRACT → PARTITION → CUSTOMIZE)

Entre no diretório:

```bash
cd /mnt/e/Rotas-Inteligentes/vroom-local
```

---

### 2.1 Extract

```bash
docker run -t -v ${PWD}/osrm-data:/data osrm/osrm-backend   osrm-extract -p /opt/car.lua /data/norte-latest.osm.pbf
```

---

### 2.2 Partition

```bash
docker run -t -v ${PWD}/osrm-data:/data osrm/osrm-backend   osrm-partition /data/norte-latest.osrm
```

---

### 2.3 Customize

```bash
docker run -t -v ${PWD}/osrm-data:/data osrm/osrm-backend   osrm-customize /data/norte-latest.osrm
```

---

# 🚀 3. Subir o servidor OSRM

Antes apague versões anteriores:

```bash
docker rm -f osrm
```

Inicie:

```bash
docker run -dt --name osrm   --network vroom_net   -p 5000:5000   -v ${PWD}/osrm-data:/data   osrm/osrm-backend   osrm-routed --algorithm mld /data/norte-latest.osrm
```

---

# 📡 4. Testar o servidor OSRM

### 4.1 Teste de saúde
```bash
curl http://localhost:5000
```
Resposta esperada:  
`InvalidUrl` → **Significa que o OSRM está rodando**

---

### 4.2 Teste nearest
```bash
curl "http://localhost:5000/nearest/v1/driving/-63.90,-8.73"
```

---

### 4.3 Teste route (pontos urbanos)
```bash
curl "http://localhost:5000/route/v1/driving/-63.90,-8.73;-63.88,-8.72"
```

⚠️ OBS:  
Rotas **entre cidades** (ex: Porto Velho → Ariquemes) podem retornar `NoRoute` devido ao mapa Norte não incluir a BR-364 completa.

---

# 🚚 5. Subir o servidor VROOM

Apague o anterior:

```bash
docker rm -f vroom
```

Suba o novo:

```bash
docker run -dt --name vroom   --network vroom_net   -p 3000:3000   -v ${PWD}/conf:/conf   vroomvrp/vroom-docker:v1.13.0
```

O `config.yml` dentro da pasta `conf/` deve conter:

```yaml
cliArgs:
  geometry: false
  threads: 4
  router: "osrm"
routingServers:
  osrm:
    car:
      host: "osrm"
      port: "5000"
```

---

# 🛠️ 6. Testes do VROOM

### 6.1 Teste básico

```bash
curl -X POST http://localhost:3000 -H "Content-Type: application/json" -d '{
  "vehicles":[{"id":1,"start":[-63.9009,-8.7300]}],
  "jobs":[{"id":1,"location":[-63.9048,-8.7628]}]
}'
```

Deve retornar `"code":0`

---

### 6.2 Teste com múltiplos jobs urbanos

```bash
curl -X POST http://localhost:3000 -H "Content-Type: application/json" -d '{
  "vehicles":[{"id":1,"start":[-63.9009,-8.7300]}],
  "jobs":[
    {"id":1,"location":[-63.9048,-8.7628]},
    {"id":2,"location":[-63.9055,-8.7630]}
  ]
}'
```

---

# 🟡 7. Observações importantes sobre o mapa **north-latest**

O mapa norte:

✔ Funciona **dentro das cidades**  
✔ Funciona para otimização urbana  
✔ Funciona para matrizes urbanas

⚠ NÃO funciona para:
- rotas intermunicipais longas  
- Porto Velho → Ariquemes  
- Ariquemes → Ji-Paraná  
- Rotas via BR-364  

🔎 Isso ocorre porque o extrato “north” **não cobre o estado completo**.

---

# 🎉 8. Conclusão

Seguindo este guia, você terá:

✔ OSRM funcionando com mapa Norte  
✔ VROOM conectado e otimizado  
✔ Matrizes e rotas urbanas válidas  
✔ Pipeline totalmente funcional dentro da área coberta  

Caso queira um **docker-compose.yml completo**, **API Python**, ou **script para rebuild automático** — posso gerar também.

---
