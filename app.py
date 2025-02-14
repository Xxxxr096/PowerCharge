import streamlit as st
import folium
from streamlit_folium import folium_static
import pandas as pd
import requests
import folium
import geopandas as gpd
import json
import logging
import streamlit as st
from io import BytesIO
from shapely.geometry import mapping, box, Point, MultiPolygon, LineString
from typing import Optional, Tuple

def load_data():
    # Charger les données des 20 plus grandes agglomérations
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
    buffer_distance_urban = st.sidebar.slider("Rayon autour du centre-ville (km)", 1, 40, 5) / 111  # Conversion km -> degrés
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
        # Filtrer sur la contenance si la colonne est présente
        if "contenance" in gdf_parcelles.columns:
            gdf_parcelles["contenance"] = gdf_parcelles["contenance"].astype(float)
            gdf_parcelles = gdf_parcelles[gdf_parcelles["contenance"] > 4000]

        # Conversion des champs Timestamp en string pour éviter les erreurs JSON
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

        # Afficher dynamiquement le nombre de parcelles après filtrage
        st.write(f"Nombre de parcelles après filtrage : {len(gdf_parcelles)}")
        if gdf_parcelles.empty:
            st.warning("Aucune parcelle trouvée après filtrage.")

        # Création de la carte Folium
        m = folium.Map(location=[center_lat, center_lon], zoom_start=12)

        # 1. Ajouter la bounding box avec interactivité désactivée
        folium.GeoJson(
            mapping(box(min_lon, min_lat, max_lon, max_lat)),
            style_function=lambda x: {"color": "red", "weight": 2, "fillOpacity": 0.1},
            tooltip="Bounding Box de la ville",
            interactive=False  # Empêche cette couche d'intercepter les clics
        ).add_to(m)

        # 2. Ajouter le réseau HTA
        for record in data_reseau:
            if "fields" in record and "geo_shape" in record["fields"]:
                geo_data = record["fields"]["geo_shape"]
                if geo_data["type"] == "LineString":
                    line_coords = [(lat, lon) for lon, lat in geo_data["coordinates"]]
                    folium.PolyLine(line_coords, color="red", weight=2.5, opacity=1, tooltip="Réseau HTA").add_to(m)

        # 3. Préparer la liste des champs à afficher dans le popup (on exclut la géométrie)
        parcel_fields = [col for col in gdf_parcelles.columns if col != 'geometry']

        # 4. Ajouter les parcelles avec un popup interactif (cette couche est ajoutée après, pour être au-dessus)
        folium.GeoJson(
            gdf_parcelles,
            style_function=lambda x: {"color": "blue", "weight": 1.5, "fillOpacity": 0.2},
            tooltip=folium.GeoJsonTooltip(fields=["contenance"], aliases=["Contenance (m²):"]),
            popup=folium.GeoJsonPopup(fields=parcel_fields)
        ).add_to(m)

        # Afficher la carte dans Streamlit
        st.components.v1.html(m._repr_html_(), height=600)

    else:
        st.error("Aucune donnée de parcelles ou de réseau HTA disponible.")

# def create_map(data):
#     # Création de la carte
#     m = folium.Map(location=[46.603354, 1.888334], zoom_start=6)
    
#     # Ajouter des marqueurs pour chaque ville
#     for i, row in data.iterrows():
#         folium.Marker(
#             location=[row["Lat"], row["Lon"]],
#             popup=row["Ville"],
#             icon=folium.Icon(color="blue", icon="info-sign")
#         ).add_to(m)
    
#     return m

def main():
    st.set_page_config(page_title="PowerCharge - Carte des Hubs", layout="wide")
    st.markdown("""
        <style>
            .title {
                text-align: center;
                font-size: 32px;
                font-weight: bold;
                color: #62B86D;
            }
            .sidebar .sidebar-content {
                background-color: #DC9D6E;
            }
            .stButton>button {
                background-color: #62B86D;
                color: white;
                font-size: 16px;
                border-radius: 10px;
            }
            .logo {
                text-align: center;
            }
        </style>
    """, unsafe_allow_html=True)
    
    st.markdown("<div class='logo'><img src='https://cdn.prod.website-files.com/6707919ecd8d7913f41983b7/6750674ae5a2bc3741a4e69e_powercharge-logo.webp' width='150'></div>", unsafe_allow_html=True)
    st.markdown("<div class='title'>⚡ PowerCharge - Carte Interactive des Hubs</div>", unsafe_allow_html=True)
    st.write("Bienvenue sur l'application PowerCharge. Utilisez les filtres pour afficher les zones optimales d'implantation des hubs.")
    
    # Chargement des données
    data = load_data()

    # Sélection des 5 meilleurs emplacements (exemple aléatoire)
    # top5_locations = data.sample(5).reset_index(drop=True)
    
    # st.write("### 🏆 Top 5 des meilleurs lieux d'implantation")
    # st.dataframe(top5_locations)
    
    if "validated_criteria" not in st.session_state:
        st.session_state["validated_criteria"] = {
            "prox_reseau_ht": 5.0,
            "prox_routier": 5.0,
            "prox_urbain": 20.0,
            "afficher_routes": False,
            "afficher_reseaux": False
        }
    
    if "temp_criteria" not in st.session_state:
        st.session_state["temp_criteria"] = st.session_state["validated_criteria"].copy()
    
    # st.sidebar.header("🔧 Filtres de sélection")
    # st.session_state["temp_criteria"]["prox_reseau_ht"] = st.sidebar.slider("⚡ Proximité Réseau HT (km)", 0.5, 10.0, st.session_state["temp_criteria"]["prox_reseau_ht"])
    # st.session_state["temp_criteria"]["prox_routier"] = st.sidebar.slider("🛣️ Proximité Axe Routier Principal (km)", 1.0, 10.0, st.session_state["temp_criteria"]["prox_routier"])
    # st.session_state["temp_criteria"]["prox_urbain"] = st.sidebar.slider("🏙️ Proximité Centre Urbain (km)", 5.0, 40.0, st.session_state["temp_criteria"]["prox_urbain"])
    
    # st.session_state["temp_criteria"]["afficher_routes"] = st.sidebar.checkbox("Afficher les routes", st.session_state["temp_criteria"]["afficher_routes"])
    # st.session_state["temp_criteria"]["afficher_reseaux"] = st.sidebar.checkbox("Afficher les réseaux électriques", st.session_state["temp_criteria"]["afficher_reseaux"])
    
    # if st.sidebar.button("Valider les critères"):
    #     st.session_state["validated_criteria"] = st.session_state["temp_criteria"].copy()
    
    st.write(f"**Critères appliqués :** Réseau HT {st.session_state['validated_criteria']['prox_reseau_ht']} km, Routes {st.session_state['validated_criteria']['prox_routier']} km, Urbain {st.session_state['validated_criteria']['prox_urbain']} km")
    
    # if st.session_state['validated_criteria']['afficher_routes']:
    #     st.write("🛣️ Routes affichées sur la carte.")
    # if st.session_state['validated_criteria']['afficher_reseaux']:
    #     st.write("⚡ Réseaux électriques affichés sur la carte.")
    
    # map_object = create_map(data)
    # folium_static(map_object)
    
if __name__ == "__main__":
    main()

# Commande pour lancer l'application : streamlit run app.py