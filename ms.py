import requests
import folium
import geopandas as gpd
import json
import logging
import streamlit as st
from io import BytesIO
from shapely.geometry import mapping, box, Point, MultiPolygon, LineString
from typing import Optional, Tuple

# Fonction pour récupérer la bounding box et le centre urbain via Nominatim
def get_city_bbox_and_center(city_name: str, user_agent: str = "Mozilla/5.0") -> Optional[Tuple[float, float, float, float, float, float]]:
    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": city_name, "format": "json", "limit": 1, "polygon_geojson": 1}
    try:
        response = requests.get(url, params=params, headers={"User-Agent": user_agent})
        if response.status_code == 200:
            results = response.json()
            if results:
                bbox = results[0]["boundingbox"]
                min_lat, max_lat, min_lon, max_lon = map(float, bbox)
                center_lat = float(results[0]["lat"])
                center_lon = float(results[0]["lon"])
                return min_lat, max_lat, min_lon, max_lon, center_lat, center_lon
    except Exception as e:
        logging.error(f"Erreur réseau pour la récupération de {city_name} : {e}")
    return None

# Ville sélectionnée
city_name = "Lille, France"

# Récupérer la bounding box dynamiquement
city_data = get_city_bbox_and_center(city_name)
if not city_data:
    st.error(f"Impossible de récupérer les données pour {city_name}")
    st.stop()

min_lat, max_lat, min_lon, max_lon, center_lat, center_lon = city_data

# URLs des données (Lille = code INSEE 59350)
parcelles_url = "https://cadastre.data.gouv.fr/bundler/cadastre-etalab/communes/59350/geojson/parcelles"
reseau_url = f"https://data.enedis.fr/api/records/1.0/download/?rows=1000&format=json&geo_simplify=true&geo_simplify_zoom=12&geofilter.bbox={min_lat},{min_lon},{max_lat},{max_lon}&fields=geo_shape&dataset=reseau-souterrain-hta"

# Interface Streamlit pour ajuster les buffers
st.sidebar.header("Réglages des Buffers")
buffer_distance_urban = st.sidebar.slider("Rayon autour du centre-ville (km)", 1, 10, 5) / 111  # Conversion km -> degrés
buffer_distance_network = st.sidebar.slider("Rayon autour du réseau HTA (m)", 15, 1000, 100) / 111000  # Conversion m -> degrés

# Télécharger les parcelles cadastrales
response_parcelles = requests.get(parcelles_url)
if response_parcelles.status_code == 200:
    gdf_parcelles = gpd.read_file(BytesIO(response_parcelles.content))
else:
    st.error("Échec du téléchargement des parcelles cadastrales")
    gdf_parcelles = None

# Télécharger les données du réseau HTA
response_reseau = requests.get(reseau_url)
if response_reseau.status_code == 200:
    data_reseau = response_reseau.json()
else:
    st.error("Échec du téléchargement du réseau électrique")
    data_reseau = None

# Vérification des données et filtrage des parcelles
if gdf_parcelles is not None and not gdf_parcelles.empty:
    #st.write("Colonnes disponibles :", list(gdf_parcelles.columns))

    # Vérifier si la colonne "contenance" est bien présente
    if "contenance" in gdf_parcelles.columns:
        gdf_parcelles["contenance"] = gdf_parcelles["contenance"].astype(float)
        gdf_parcelles = gdf_parcelles[gdf_parcelles["contenance"] > 4000]  # Filtrage sur contenance

    # Convertir les champs Timestamp en string pour éviter l'erreur JSON
    for col in ["created", "updated"]:
        if col in gdf_parcelles.columns:
            gdf_parcelles[col] = gdf_parcelles[col].astype(str)

    # Création d'un buffer autour du centre urbain
    centre_urbain = Point(center_lon, center_lat)
    buffer_zone_urban = centre_urbain.buffer(buffer_distance_urban)

    # Création du buffer autour du réseau électrique
    network_buffers = []
    if data_reseau:
        for record in data_reseau:
            if "fields" in record and "geo_shape" in record["fields"]:
                geo_data = record["fields"]["geo_shape"]
                if geo_data["type"] == "LineString":
                    coords = [(lon, lat) for lon, lat in geo_data["coordinates"]]
                    line = LineString(coords)
                    network_buffers.append(line.buffer(buffer_distance_network))

    # Fusionner tous les buffers de réseau électrique
    if network_buffers:
        buffer_zone_network = network_buffers[0]
        for buff in network_buffers[1:]:
            buffer_zone_network = buffer_zone_network.union(buff)
    else:
        buffer_zone_network = None

    # Filtrer les parcelles en fonction des buffers
    gdf_parcelles = gdf_parcelles[gdf_parcelles.intersects(buffer_zone_urban)]
    if buffer_zone_network:
        gdf_parcelles = gdf_parcelles[gdf_parcelles.intersects(buffer_zone_network)]

    if gdf_parcelles.empty:
        st.warning("Aucune parcelle trouvée après filtrage.")

    # Créer la carte Folium
    m = folium.Map(location=[center_lat, center_lon], zoom_start=12)

    # Ajouter les parcelles filtrées
    folium.GeoJson(
        gdf_parcelles,
        style_function=lambda x: {"color": "blue", "weight": 1.5, "fillOpacity": 0.2},
        tooltip=folium.GeoJsonTooltip(fields=["contenance"], aliases=["Contenance (m²):"])
    ).add_to(m)

    # Ajouter la bounding box
    folium.GeoJson(
        mapping(box(min_lon, min_lat, max_lon, max_lat)),
        style_function=lambda x: {"color": "red", "weight": 2, "fillOpacity": 0.1},
        tooltip="Bounding Box de la ville"
    ).add_to(m)

    # Ajouter le réseau HTA
    for record in data_reseau:
        if "fields" in record and "geo_shape" in record["fields"]:
            geo_data = record["fields"]["geo_shape"]
            if geo_data["type"] == "LineString":
                line_coords = [(lat, lon) for lon, lat in geo_data["coordinates"]]
                folium.PolyLine(line_coords, color="red", weight=2.5, opacity=1, tooltip="Réseau HTA").add_to(m)

    # Afficher la carte dans Streamlit
    st.components.v1.html(m._repr_html_(), height=600)

else:
    st.error("Aucune donnée de parcelles ou de réseau HTA disponible.")
