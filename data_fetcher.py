import os
import json
import time
import logging
import requests
from typing import Optional, Tuple

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

# Répertoires de stockage
DATA_DIRS = {
    "parcelles": "data/parcelles",
    "reseaux": "data/reseaux",
    "axes_routiers": "data/axes_routiers",
    "proprietaires": "data/proprietaires"
}

for path in DATA_DIRS.values():
    os.makedirs(path, exist_ok=True)


class DataFetcher:
    def __init__(self, sleep_interval: float = 1.0):
        """
        Initialise la session HTTP et configure le délai entre les requêtes.
        """
        self.session = requests.Session()
        self.sleep_interval = sleep_interval

    def _save_json(self, data: dict, filepath: str) -> None:
        """Sauvegarde les données JSON dans le fichier spécifié."""
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logging.info(f"Données sauvegardées dans {filepath}")

    def _get_city_bbox_and_center(self, city_name: str, user_agent: str = "Mozilla/5.0") -> Optional[Tuple[float, float, float, float, float, float]]:
        """
        Récupère la bounding box d'une ville via l'API Nominatim ainsi que son centre (lat, lon).
        Renvoie un tuple (min_lat, max_lat, min_lon, max_lon, center_lat, center_lon) ou None en cas d'erreur.
        """
        url = "https://nominatim.openstreetmap.org/search"
        params = {"q": city_name, "format": "json", "limit": 1, "polygon_geojson": 1}
        try:
            response = self.session.get(url, params=params, headers={"User-Agent": user_agent})
            if response.status_code == 200:
                results = response.json()
                if results:
                    bbox = results[0]["boundingbox"]
                    try:
                        min_lat, max_lat, min_lon, max_lon = map(float, bbox)
                        center_lat = float(results[0]["lat"])
                        center_lon = float(results[0]["lon"])
                        return min_lat, max_lat, min_lon, max_lon, center_lat, center_lon
                    except Exception as e:
                        logging.error(f"Erreur lors du parsing de la bbox pour {city_name} : {e}")
                else:
                    logging.error(f"Aucun résultat trouvé pour {city_name}")
            else:
                logging.error(f"Erreur {response.status_code} pour la récupération de {city_name}")
        except requests.RequestException as e:
            logging.error(f"Erreur réseau pour la récupération de {city_name} : {e}")
        return None

    def fetch_parcelles(self, city_name: str, commune_code: str) -> None:
        """
        Récupère les données des parcelles pour une commune donnée, ajoute le centre urbain
        (récupéré via Nominatim) et les sauvegarde.
        """
        url = f"https://cadastre.data.gouv.fr/bundler/cadastre-etalab/communes/{commune_code}/geojson/parcelles"
        try:
            response = self.session.get(url, timeout=30)
            if response.status_code == 200:
                data = response.json()
                # Récupérer le centre urbain via Nominatim
                center_info = self._get_city_bbox_and_center(city_name)
                if center_info:
                    _, _, _, _, center_lat, center_lon = center_info
                    data["center_urban"] = {"lat": center_lat, "lon": center_lon}
                    logging.info(f"Centre urbain pour {city_name} : {data['center_urban']}")
                else:
                    logging.error(f"Impossible de récupérer le centre urbain pour {city_name}")
                filepath = os.path.join(DATA_DIRS["parcelles"], f"parcelles_{commune_code}.geojson")
                self._save_json(data, filepath)
                logging.info(f"✅ Données récupérées pour la commune {commune_code}")
            else:
                logging.error(f"❌ Erreur {response.status_code} pour la commune {commune_code}")
        except requests.RequestException as e:
            logging.error(f"⚠️ Erreur réseau pour la commune {commune_code} : {e}")
        time.sleep(self.sleep_interval)

    def fetch_parcelles_arrondissements(self, insee_global: str, city_label: str) -> None:
        """
        Récupère les parcelles pour chaque arrondissement
        défini dans self.BIG_CITY_ARRONDISSEMENTS[insee_global].
        Utilise fetch_parcelles(city_label, arr_code) pour télécharger
        'parcelles_{arr_code}.geojson' dans data/parcelles.
        """
        if insee_global not in self.BIG_CITY_ARRONDISSEMENTS:
            logging.error(f"Aucun arrondissements définis pour {insee_global}.")
            return

        for arr_code in self.BIG_CITY_ARRONDISSEMENTS[insee_global]:
            logging.info(f"Téléchargement parcelles arr. {arr_code} ({city_label})")
            self.fetch_parcelles(city_label, arr_code)
            # -> Cela télécharge data/parcelles/parcelles_{arr_code}.geojson

    def _get_city_bbox(self, city_name: str, user_agent: str = "Mozilla/5.0") -> Optional[Tuple[float, float, float, float]]:
        """
        Récupère la bounding box d'une ville via l'API Nominatim.
        Renvoie un tuple (min_lat, max_lat, min_lon, max_lon) ou None en cas d'erreur.
        """
        bbox_center = self._get_city_bbox_and_center(city_name, user_agent)
        if bbox_center:
            min_lat, max_lat, min_lon, max_lon, _, _ = bbox_center
            return min_lat, max_lat, min_lon, max_lon
        return None

    def fetch_reseau_hta(self, city_name: str, dataset: str = "reseau-hta") -> None:
        """
        Récupère le réseau HTA pour une ville donnée en utilisant d'abord Nominatim pour obtenir la bounding box,
        puis l'API d'Enedis pour récupérer les données.
        """
        bbox = self._get_city_bbox(city_name, user_agent="Mozilla/5.0")
        if bbox:
            min_lat, max_lat, min_lon, max_lon = bbox
            enedis_url = (
                f"https://data.enedis.fr/api/records/1.0/download/"
                f"?rows=1000&format=json&geo_simplify=true&geo_simplify_zoom=14"
                f"&geofilter.bbox={min_lat},{min_lon},{max_lat},{max_lon}"
                f"&fields=geo_shape&dataset={dataset}"
            )
            try:
                response = self.session.get(enedis_url)
                if response.status_code == 200:
                    data = response.json()
                    filename = f"reseau_hta_{city_name.replace(' ', '_')}.json"
                    filepath = os.path.join(DATA_DIRS["reseaux"], filename)
                    self._save_json(data, filepath)
                    logging.info(f"✅ Réseau HTA récupéré pour {city_name}")
                else:
                    logging.error(f"❌ Erreur {response.status_code} pour le réseau HTA de {city_name}")
            except requests.RequestException as e:
                logging.error(f"⚠️ Erreur réseau pour {city_name} : {e}")
        time.sleep(self.sleep_interval)

    def fetch_axes_routiers(self, city_name: str) -> None:
        """
        Récupère les axes routiers pour une ville donnée via l'API Overpass en utilisant la bounding box obtenue avec Nominatim.
        """
        logging.info(f"🚀 Récupération des axes routiers pour {city_name}...")
        bbox = self._get_city_bbox(city_name, user_agent="PowerChargeDataFetcher/1.0")
        if not bbox:
            logging.error(f"❌ Erreur lors de la récupération des coordonnées pour {city_name}")
            return

        min_lat, max_lat, min_lon, max_lon = bbox
        overpass_url = "http://overpass-api.de/api/interpreter"
        query = f"""
        [out:json][timeout:25];
        (
          way["highway"~"motorway|trunk|primary|secondary|tertiary"]({min_lat},{min_lon},{max_lat},{max_lon});
        );
        out body;
        >;
        out skel qt;
        """
        try:
            response = self.session.post(
                overpass_url,
                data=query,
                headers={"User-Agent": "PowerChargeDataFetcher/1.0"}
            )
            if response.status_code == 200:
                data = response.json()
                filename = f"axes_{city_name.replace(' ', '_').lower()}.geojson"
                filepath = os.path.join(DATA_DIRS["axes_routiers"], filename)
                self._save_json(data, filepath)
                logging.info(f"✅ Axes routiers récupérés pour {city_name}")
            else:
                logging.error(f"❌ Erreur {response.status_code} lors de la récupération des axes routiers pour {city_name}")
        except requests.RequestException as e:
            logging.error(f"⚠️ Erreur réseau pour {city_name} : {e}")
        time.sleep(self.sleep_interval)

    def fetch_proprietaires_by_city(self) -> None:
        """
        Parcourt tous les fichiers de parcelles dans DATA_DIRS["parcelles"],
        extrait la liste des identifiants de parcelles pour chaque commune,
        appelle l'API pour récupérer les propriétaires et sauvegarde le résultat
        dans un fichier par commune dans DATA_DIRS["proprietaires"].
        """
        base_url = "https://api.sogefi-sig.com/2Besqie6xBzrxMj85psAPXm7cLy7A57eoEx/majic/v2/open/parcelles/proprietaires"
        headers = {
            "Host": "api.sogefi-sig.com",
            "Connection": "keep-alive",
            "sec-ch-ua": '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
            "accept": "*/*",
            "sec-ch-ua-mobile": "?0",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
            "sec-ch-ua-platform": "Windows",
            "Origin": "https://geoservices.sogefi-sig.com",
            "Sec-Fetch-Site": "same-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Referer": "https://geoservices.sogefi-sig.com/",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7"
        }
        # Parcourir les fichiers de parcelles
        for filename in os.listdir(DATA_DIRS["parcelles"]):
            if filename.startswith("parcelles_") and filename.endswith(".geojson"):
                commune_code = filename.split("_")[1].split(".")[0]
                filepath = os.path.join(DATA_DIRS["parcelles"], filename)
                with open(filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    features = data.get("features", [])
                    parcel_ids = []
                    for feature in features:
                        # On utilise le champ "id" au niveau de la feature
                        parcel_id = feature.get("id") or feature.get("properties", {}).get("id")
                        if parcel_id:
                            parcel_ids.append(parcel_id)
                    # Si aucun identifiant n'est trouvé, on passe à la suite
                    if not parcel_ids:
                        logging.error(f"Aucun identifiant de parcelle trouvé dans {filename}")
                        continue
                    # Retirer les doublons
                    unique_ids = list(set(parcel_ids))
                    ids_param = ",".join(unique_ids)
                    params = {
                        "id_par[in]": ids_param,
                        "sogefi_annee_archivee": "_last_"
                    }
                    try:
                        result = self.session.get(url=base_url, params=params, headers=headers)
                        logging.info(f"Statut de la requête pour la commune {commune_code}: {result.status_code}")
                        result_json = result.json().get("proprietaires", [])
                        proprietaires_list = []
                        for line in result_json:
                            for parcelle in line.get("parcelles", []):
                                proprietaires_list.append(parcelle.get("id_par"))
                        # Sauvegarder le résultat dans un fichier dédié pour cette commune
                        output_filepath = os.path.join(DATA_DIRS["proprietaires"], f"proprietaires_{commune_code}.json")
                        self._save_json({"proprietaires": proprietaires_list}, output_filepath)
                        logging.info(f"Propriétaires pour la commune {commune_code}: {proprietaires_list}")
                    except requests.RequestException as e:
                        logging.error(f"Erreur lors de la récupération des propriétaires pour la commune {commune_code} : {e}")
                time.sleep(self.sleep_interval)

def main():
    # Dictionnaire associant le nom de la ville à son code INSEE
    villes_communes = {
        "Paris": "75056",
        "Marseille": "13055",
        "Lyon": "69123",
        "Nice": "06088",
        "Toulouse": "31555",
        "Bordeaux": "33063",
        "Nantes": "44109",
        "Strasbourg": "67482",
        "Lille": "59350",
        "Montpellier": "34172",
        "Rennes": "35238",
        "Reims": "51454",
        "Grenoble": "38185",
        "Toulon": "83137",
        "Le Mans": "72181",
        "Rouen": "76540",
        "Limoges": "87085",
        "Angers": "49328",
        "Le Havre": "76600",
        "Avignon": "84007"
    }

    fetcher = DataFetcher(sleep_interval=1)

    # Récupération des parcelles pour toutes les communes
    for ville, code in villes_communes.items():
        fetcher.fetch_parcelles(ville, code)

    # Récupération des réseaux HTA et axes routiers pour les 10 plus grandes villes
    top_villes = ["Paris", "Marseille", "Lyon", "Nice", "Toulouse", "Bordeaux", "Nantes", "Strasbourg", "Lille", "Montpellier"]
    for ville in top_villes:
        fetcher.fetch_reseau_hta(ville)
        fetcher.fetch_axes_routiers(ville)

    # Récupération des propriétaires de parcelles, un fichier par commune
    fetcher.fetch_proprietaires_by_city()

    logging.info("🎉 Récupération des données terminée !")


if __name__ == "__main__":
    main()
