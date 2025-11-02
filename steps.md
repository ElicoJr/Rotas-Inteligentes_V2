```powershell
cd E:\Rotas-Inteligentes\vroom-local  
docker run -t -v ${PWD}\osrm-data:/data osrm/osrm-backend osrm-extract -p /opt/car.lua /data/norte-latest.osm.pbf  
docker run -t -v ${PWD}\osrm-data:/data osrm/osrm-backend osrm-partition /data/norte-latest.osrm  
docker run -t -v ${PWD}\osrm-data:/data osrm/osrm-backend osrm-customize /data/norte-latest.osrm  
```

depois
```powershell
cd E:\Rotas-Inteligentes\vroom-local\osrm-docker
docker compose up -d
```