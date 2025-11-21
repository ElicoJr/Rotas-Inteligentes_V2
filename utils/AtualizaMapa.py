import os
import requests

# Change directory to vroom-local
os.chdir('vroom-local')

# Download the OSM file using requests
response = requests.get('https://download.geofabrik.de/south-america/brazil/norte-latest.osm.pbf')
with open('osrm-data/norte-latest.osm.pbf', 'wb') as file:
    file.write(response.content)

# Change directory back
os.chdir('..')
print('Mapa atualizado com sucesso!')