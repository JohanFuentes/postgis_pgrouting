import psycopg2
import geopandas as gpd
import matplotlib.pyplot as plt
import folium
import json
from shapely.geometry import GeometryCollection, Point

conn_params = {
    "dbname": "gis",
    "user": "usuario",
    "password": "usuario",
    "host": "localhost",
    "port": "20000"  # Opcional si es el puerto por defecto
}
 
origen_latitud = "-33.45434194111309"
origen_longitud = "-70.66619434532957"
origen_latitud = input("Ingrese la latitud del punto de origen: ")
origen_longitud = input("Ingrese la longitud del punto de origen: ")

try:
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()
    query = """
        -- Encuentra el vértice más cercano al punto de origen inicial y su geometría
        WITH start AS (
        SELECT topo.source, ST_SetSRID(
            ST_GeomFromText('POINT ("""+origen_longitud+" "+origen_latitud+""")'), 4326) AS geom_origen
        FROM syd_2po_4pgr AS topo
        ORDER BY topo.geom_way <-> ST_SetSRID(
            ST_GeomFromText('POINT ("""+origen_longitud+" "+origen_latitud+""")'), 4326)
        LIMIT 1
        ),
        -- Calcula la ruta más corta desde el punto de origen a todos los locales
        rutas AS (
        SELECT l.id AS local_id, l.name AS nombre_local, l.geom AS geom_destino,
                (pgr_dijkstra(
                'SELECT id, source, target, ST_Length(ST_Transform(geom_way, 3857)) AS cost 
                    FROM syd_2po_4pgr', 
                (SELECT source FROM start), 
                topo.source, 
                directed := false)).*
        FROM localesinfo l
        JOIN syd_2po_4pgr topo ON topo.geom_way <-> ST_SetSRID(l.geom, 4326) = 
            (SELECT MIN(topo.geom_way <-> ST_SetSRID(l.geom, 4326))
            FROM syd_2po_4pgr topo)
        ),
        -- Selecciona el local más cercano
        local_mas_cercano AS (
        SELECT local_id, nombre_local, geom_destino, SUM(agg_cost) AS total_cost
        FROM rutas
        GROUP BY local_id, nombre_local, geom_destino
        ORDER BY total_cost ASC
        LIMIT 1
        ),
        -- Usa el destino del primer local como nuevo punto de origen
        nuevo_origen AS (
        SELECT geom_destino AS geom_origen, local_id AS orig_local_id
        FROM local_mas_cercano
        ),
        -- Repite el proceso para encontrar el siguiente local más cercano
        siguiente_ruta AS (
        SELECT l.id AS local_id, l.name AS nombre_local, l.geom AS geom_destino,
                (pgr_dijkstra(
                'SELECT id, source, target, ST_Length(ST_Transform(geom_way, 3857)) AS cost 
                    FROM syd_2po_4pgr', 
                (SELECT topo.source FROM syd_2po_4pgr AS topo
                    ORDER BY topo.geom_way <-> (SELECT geom_origen FROM nuevo_origen)
                    LIMIT 1), 
                topo.source, 
                directed := false)).*
        FROM localesinfo l
        JOIN syd_2po_4pgr topo ON topo.geom_way <-> ST_SetSRID(l.geom, 4326) = 
            (SELECT MIN(topo.geom_way <-> ST_SetSRID(l.geom, 4326))
            FROM syd_2po_4pgr topo)
        ),
        siguiente_local_mas_cercano AS (
        SELECT local_id, nombre_local, geom_destino, SUM(agg_cost) AS total_cost
        FROM siguiente_ruta
        GROUP BY local_id, nombre_local, geom_destino
        ORDER BY total_cost ASC
        LIMIT 1
        )
        -- Obtén la ruta unida para ambos locales más cercanos, incluyendo geometrías de origen y destino
        SELECT 'Primer Local' AS Etapa, local_mas_cercano.nombre_local, 
            ST_Collect(array[start.geom_origen, ruta_unida, local_mas_cercano.geom_destino]) AS ruta
        FROM (
            SELECT ST_Union(geom_way) AS ruta_unida, local_id
            FROM rutas
            JOIN syd_2po_4pgr ON rutas.edge = syd_2po_4pgr.id
            GROUP BY local_id
        ) AS rutas_unidas
        JOIN local_mas_cercano ON rutas_unidas.local_id = local_mas_cercano.local_id
        CROSS JOIN start

        UNION ALL

        SELECT 'Segundo Local' AS Etapa, siguiente_local_mas_cercano.nombre_local, 
            ST_Collect(array[(SELECT geom_origen FROM nuevo_origen), ruta_unida, siguiente_local_mas_cercano.geom_destino]) AS ruta
        FROM (
            SELECT ST_Union(geom_way) AS ruta_unida, local_id
            FROM siguiente_ruta
            JOIN syd_2po_4pgr ON siguiente_ruta.edge = syd_2po_4pgr.id
            GROUP BY local_id
        ) AS siguientes_rutas_unidas
        JOIN siguiente_local_mas_cercano ON siguientes_rutas_unidas.local_id = siguiente_local_mas_cercano.local_id
        CROSS JOIN nuevo_origen;
    """
    gdf = gpd.read_postgis(query, conn, geom_col='ruta')
    geojson = gdf.to_json()

    # Convierte la cadena JSON a un objeto Python
    data = json.loads(geojson)

    # Itera a través de las características y extrae las coordenadas de los puntos
    point_coordinates = []
    for feature in data['features']:
        for geometry in feature['geometry']['geometries']:
            if geometry['type'] == 'Point':
                point_coordinates.append(geometry['coordinates'])

    # point_coordinates ahora contiene todas las coordenadas de los puntos
    point_coordinates.pop(0)
    point_coordinates.pop(0)

    m = folium.Map(location=[-33.45237359573883 ,-70.66449554501418], zoom_start=16)
    colores = ['red', 'black'] # Puedes expandir esta lista para más rutas

    # Añade el nombre del local como un marcador o popup
    nombre_local = "Origen"
    ubicacion = [origen_latitud, origen_longitud]
    
    folium.Marker(
        ubicacion,
        popup=nombre_local,
        icon=folium.Icon(color="blue")
    ).add_to(m)

    i=0
    # Itera sobre las filas del GeoDataFrame
    for index, row in gdf.iterrows():
        # Añade el nombre del local como un marcador o popup
        nombre_local = row['nombre_local']
        ubicacion = [point_coordinates[i][1], point_coordinates[i][0]]
        i=i+1
        folium.Marker(
            ubicacion,
            popup=nombre_local,
            icon=folium.Icon(color=colores[index % len(colores)])
        ).add_to(m)

        # Función para filtrar las geometrías que no sean puntos
    def extraer_rutas(geometry_collection):
        if isinstance(geometry_collection, GeometryCollection):
            return GeometryCollection([geom for geom in geometry_collection.geoms if not isinstance(geom, Point)])
        else:
            # Si la entrada no es una GeometryCollection, devuelve la entrada original
            return geometry_collection

    # Aplica la función a la columna 'ruta' para obtener solo rutas (líneas)
    gdf['rutas'] = gdf['ruta'].apply(extraer_rutas)

    # Ahora, añade las rutas al mapa
    for index, row in gdf.iterrows():
        # Asegúrate de que el elemento 'rutas' es un GeometryCollection o derivado antes de añadirlo al mapa
        if isinstance(row['rutas'], GeometryCollection):
            folium.GeoJson(
                data=row['rutas'],
                style_function=lambda x, color=colores[index % len(colores)]: {"color": color}
            ).add_to(m)
    m.save('mapaDijkstra.html')
    cursor.close()
    conn.close()
except psycopg2.Error as e:
    print("Error al conectar a la base de datos:", e)


