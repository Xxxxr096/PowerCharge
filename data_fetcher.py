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
    "proprietaires": "data/proprietaires",
    "entreprises": "data/entreprises"
}

for path in DATA_DIRS.values():
    os.makedirs(path, exist_ok=True)

class DataFetcher:
    # Dictionnaire associant le code INSEE global de la grande ville (Paris, Marseille, Lyon)
    # à la liste des codes d'arrondissements.
    BIG_CITY_ARRONDISSEMENTS = {
        "75056": [  # Paris
            "75101", "75102", "75103", "75104", "75105", "75106",
            "75107", "75108", "75109", "75110", "75111", "75112",
            "75113", "75114", "75115", "75116", "75117", "75118",
            "75119", "75120"
        ],
        "13055": [  # Marseille
            "13201", "13202", "13203", "13204", "13205", "13206",
            "13207", "13208", "13209", "13210", "13211", "13212",
            "13213", "13214", "13215", "13216"
        ],
        "69123": [  # Lyon
            "69381", "69382", "69383", "69384", "69385",
            "69386", "69387", "69388", "69389"
        ]
    }

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
        Récupère les données des parcelles pour une commune (ou un arrondissement) donnée,
        ajoute le centre urbain (récupéré via Nominatim) et les sauvegarde.
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
                logging.info(f"✅ Données récupérées pour la commune/arrondissement {commune_code}")
            else:
                logging.error(f"❌ Erreur {response.status_code} pour la commune/arr. {commune_code}")
        except requests.RequestException as e:
            logging.error(f"⚠️ Erreur réseau pour la commune/arr. {commune_code} : {e}")
        time.sleep(self.sleep_interval)

    def fetch_parcelles_arrondissements(self, insee_global: str, city_label: str) -> None:
        """
        Récupère les parcelles pour chaque arrondissement
        défini dans self.BIG_CITY_ARRONDISSEMENTS[insee_global].
        Utilise fetch_parcelles(city_label, arr_code).
        """
        arr_codes = self.BIG_CITY_ARRONDISSEMENTS.get(insee_global)
        if not arr_codes:
            logging.error(f"Aucun arrondissements définis pour {insee_global}.")
            return

        for arr_code in arr_codes:
            logging.info(f"Téléchargement parcelles arr. {arr_code} pour {city_label}")
            self.fetch_parcelles(city_label, arr_code)

    def _get_city_bbox(self, city_name: str, user_agent: str = "Mozilla/5.0") -> Optional[Tuple[float, float, float, float]]:
        bbox_center = self._get_city_bbox_and_center(city_name, user_agent)
        if bbox_center:
            min_lat, max_lat, min_lon, max_lon, _, _ = bbox_center
            return min_lat, max_lat, min_lon, max_lon
        return None

    def fetch_reseau_hta(self, city_name: str) -> None:

        logging.info(f"Récupération du réseau HTA (aérien et souterrain) pour {city_name}...")

        # Récupération de la bbox via _get_city_bbox
        bbox = self._get_city_bbox(city_name, user_agent="Mozilla/5.0")
        if not bbox:
            logging.error(f"Impossible de récupérer la bbox pour {city_name}. Abandon.")
            return

        min_lat, max_lat, min_lon, max_lon = bbox

        # Liste des deux datasets à récupérer :
        datasets = [
            ("reseau-hta", "aerien"),
            ("reseau-souterrain-hta", "souterrain")
        ]

        # Pour chaque dataset, on construit l'URL et on sauvegarde la réponse
        for ds_name, ds_suffix in datasets:
            enedis_url = (
                f"https://data.enedis.fr/api/records/1.0/download/"
                f"?rows=1000&format=json&geo_simplify=true&geo_simplify_zoom=14"
                f"&geofilter.bbox={min_lat},{min_lon},{max_lat},{max_lon}"
                f"&fields=geo_shape&dataset={ds_name}"
            )

            try:
                response = self.session.get(enedis_url)
                logging.info(f"Requête Enedis pour dataset {ds_name} : {response.url}")
                if response.status_code == 200:
                    data = response.json()
                    # Nom de fichier, par ex: reseau_hta_aerien_paris.json / reseau_hta_souterrain_paris.json
                    filename = f"reseau_hta_{ds_suffix}_{city_name.replace(' ', '_')}.json"
                    filepath = os.path.join(DATA_DIRS["reseaux"], filename)
                    self._save_json(data, filepath)
                    logging.info(f"✅ Réseau HTA ({ds_suffix}) récupéré pour {city_name}")
                else:
                    logging.error(f"❌ Erreur {response.status_code} pour le réseau HTA '{ds_name}' de {city_name}")
            except requests.RequestException as e:
                logging.error(f"⚠️ Erreur réseau pour {city_name} (dataset {ds_name}) : {e}")
        time.sleep(self.sleep_interval)


    def fetch_axes_routiers(self, city_name: str) -> None:
        logging.info(f"🚀 Récupération des axes routiers pour {city_name}...")
        bbox = self._get_city_bbox(city_name, user_agent="PowerChargeDataFetcher/1.0")
        if not bbox:
            logging.error(f"❌ Erreur lors de la récupération des coordonnées pour {city_name}")
            return

        min_lat, max_lat, min_lon, max_lon = bbox
        overpass_url = "http://overpass-api.de/api/interpreter"
        query = f"""
        [out:json][timeout:250];
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


    def fetch_entreprises_btp(self, city_name: str, radius: float = 50.0, activite_principale: str = "41.20A,41.20B"):
        """
        Récupère, par pagination, toutes les entreprises correspondant aux 'activite_principale'
        dans un rayon 'radius' (km) autour du centroïde de la ville 'city_name'.

        Stocke le résultat dans un GeoJSON 'entreprises_btp_{city_name}.geojson' sous la forme
        FeatureCollection, chaque Feature représentant un établissement (avec geometry=point).
        """
        logging.info(f"Récupération entreprises BTP pour {city_name} (rayon={radius}km, codes={activite_principale})")

        # 1) Obtenir le centre (lat, lon) via _get_city_bbox_and_center
        info = self._get_city_bbox_and_center(city_name)
        if not info:
            logging.error(f"Impossible de récupérer le centroïde pour {city_name}.")
            return
        *_, center_lat, center_lon = info  # On ignore min_lat, max_lat, etc.

        base_url = "https://recherche-entreprises.api.gouv.fr/near_point"
        per_page = 25
        page = 1
        all_results = []

        while True:
            params = {
                "lat": center_lat,
                "long": center_lon,
                "radius": radius,
                "activite_principale": activite_principale,
                "per_page": per_page,
                "page": page
            }
            logging.debug(f"[{city_name}] Requête page={page}, params={params}")
            try:
                resp = self.session.get(base_url, params=params, timeout=10)
                logging.debug(f"Statut HTTP: {resp.status_code}")
                if resp.status_code != 200:
                    logging.error(f"Erreur HTTP {resp.status_code} (page={page}) pour {city_name}.")
                    break

                data = resp.json()
                results = data.get("results", [])
                total_pages = data.get("total_pages", 1)
                total_results = data.get("total_results", 0)

                logging.info(f"[{city_name}] page={page}/{total_pages}, {len(results)} résultats, total annoncé={total_results}")

                if not results:
                    logging.info(f"[{city_name}] Pas de nouveaux résultats, fin pagination.")
                    break

                all_results.extend(results)

                if page >= total_pages:
                    logging.info(f"[{city_name}] Dernière page atteinte, fin récupération.")
                    break

                page += 1
                time.sleep(self.sleep_interval)

            except Exception as e:
                logging.error(f"Erreur lors de la récupération (page={page}) pour {city_name}: {e}")
                break

        # Conversion en GeoJSON FeatureCollection
        features = []
        for entreprise in all_results:
            nom_complet = entreprise.get("nom_complet", "INCONNU")
            matching = entreprise.get("matching_etablissements", [])
            for etab in matching:
                siret = etab.get("siret", "")
                lat_str = etab.get("latitude")
                lon_str = etab.get("longitude")

                if lat_str and lon_str:
                    try:
                        lat_f = float(lat_str)
                        lon_f = float(lon_str)
                    except ValueError:
                        continue
                    feature = {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [lon_f, lat_f]
                        },
                        "properties": {
                            "nom_complet": nom_complet,
                            "siret": siret
                        }
                    }
                    features.append(feature)

        geojson_fc = {
            "type": "FeatureCollection",
            "features": features
        }

        # Nom du fichier en fonction de la ville
        filename = f"entreprises_btp_{city_name.replace(' ', '_')}.geojson"
        output_path = os.path.join(DATA_DIRS["entreprises"], filename)
        with open(output_path, "w", encoding="utf-8") as f_out:
            json.dump(geojson_fc, f_out, ensure_ascii=False, indent=4)

        logging.info(f"[{city_name}] {len(features)} établissements en GeoJSON, fichier: {output_path}")
        time.sleep(self.sleep_interval)

    def fetch_proprietaires_by_city(self) -> None:
        """
        Parcourt tous les fichiers de parcelles dans data/parcelles,
        extrait la liste des identifiants de parcelles (dont la contenance > 4000 m²),
        effectue des requêtes GET par lots (batches) pour éviter l'erreur 414,
        puis sauvegarde le résultat dans data/proprietaires/proprietaires_{commune_code}.json

        Le résultat est un dictionnaire de la forme :
        {
            "proprietaires": {
                "owner_id": ["id_parcelle1", "id_parcelle2", ...],
                ...
            }
        }
        """

        def chunker(seq, size):
            """Découpe la liste seq en morceaux de taille size."""
            return (seq[pos:pos + size] for pos in range(0, len(seq), size))

        base_url = "https://api.sogefi-sig.com/2Besqie6xBzrxMj85psAPXm7cLy7A57eoEx/majic/v2/open/parcelles/proprietaires"
        headers = {
            "Host": "api.sogefi-sig.com",
            "Connection": "keep-alive",
            "sec-ch-ua": '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
            "Accept": "*/*",
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

        logging.info("Démarrage de fetch_proprietaires_by_city (avec batching pour éviter 414).")

        # Parcourir tous les fichiers de parcelles dans data/parcelles
        for filename in os.listdir(DATA_DIRS["parcelles"]):
            if not filename.startswith("parcelles_") or not filename.endswith(".geojson"):
                continue

            try:
                commune_code = filename.split("_")[1].split(".")[0]
            except IndexError:
                logging.error(f"Nom de fichier inattendu : {filename}")
                continue

            filepath = os.path.join(DATA_DIRS["parcelles"], filename)
            logging.debug(f"Traitement du fichier: {filepath} pour la commune {commune_code}")

            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as e:
                logging.error(f"Erreur lors de la lecture du fichier {filename}: {e}")
                continue

            features = data.get("features", [])
            parcel_ids = []
            # Ne prendre que les parcelles dont la contenance > 4000
            for feat in features:
                props = feat.get("properties", {})
                try:
                    contenance = float(props.get("contenance", 0))
                except Exception as exc:
                    logging.warning(f"Impossible de convertir la contenance (fichier {filename}): {exc}")
                    continue

                if contenance > 4000:
                    pid = feat.get("id") or props.get("id")
                    if pid:
                        parcel_ids.append(pid)
                    else:
                        logging.warning(f"Aucun identifiant trouvé pour une feature dans {filename}")

            if not parcel_ids:
                logging.error(f"Aucun identifiant de parcelle (>4000) trouvé pour {filename}")
                continue

            unique_ids = list(set(parcel_ids))
            logging.info(f"Commune {commune_code}: {len(unique_ids)} parcelles (contenance>4000) identifiées.")

            # Dictionnaire associant owner_id -> set(parcelles)
            owner_map = {}

            # On découpe en lots de 50 pour limiter la longueur de l'URL
            for batch in chunker(unique_ids, 50):
                ids_param = ",".join(batch)
                short_preview = (ids_param[:80] + '...') if len(ids_param) > 80 else ids_param

                params = {
                    "id_par[in]": ids_param,
                    "sogefi_annee_archivee": "_last_"
                }
                try:
                    result = self.session.get(url=base_url, params=params, headers=headers)
                    logging.debug(f"Commune {commune_code} - batch({len(batch)}) => HTTP {result.status_code}. Ex: {short_preview}")
                    if result.status_code != 200:
                        logging.error(f"Erreur {result.status_code} pour commune {commune_code} batch (size={len(batch)}).")
                        continue

                    try:
                        parsed_json = result.json()
                    except Exception as parse_exc:
                        logging.error(f"Erreur de parsing JSON (commune {commune_code}, batch {short_preview}): {parse_exc}")
                        continue

                    # Parcours des "proprietaires"
                    for entry in parsed_json.get("proprietaires", []):
                        owner_id = entry.get("id_dnupro")
                        if owner_id:
                            if owner_id not in owner_map:
                                owner_map[owner_id] = set()
                            for parcelle_info in entry.get("parcelles", []):
                                parcelle_id = parcelle_info.get("id_par")
                                if parcelle_id:
                                    owner_map[owner_id].add(parcelle_id)

                except requests.RequestException as req_exc:
                    logging.error(f"Erreur réseau commune {commune_code}: {req_exc}")

                # Pause entre lots
                time.sleep(self.sleep_interval)

            # Conversion des sets en liste
            for o_id, parcelles_set in owner_map.items():
                owner_map[o_id] = list(parcelles_set)

            # Sauvegarde finale
            proprietaires_filepath = os.path.join(DATA_DIRS["proprietaires"], f"proprietaires_{commune_code}.json")
            try:
                with open(proprietaires_filepath, "w", encoding="utf-8") as out_f:
                    json.dump({"proprietaires": owner_map}, out_f, ensure_ascii=False, indent=4)
                logging.info(f"Commune {commune_code} - Fichier propriétaires sauvegardé : {proprietaires_filepath}")
            except Exception as e:
                logging.error(f"Erreur lors de la sauvegarde (commune {commune_code}): {e}")

            time.sleep(self.sleep_interval)

        logging.info("Fin de fetch_proprietaires_by_city.")

    def enrich_proprietaires_with_names(self) -> None:
        """
        Parcourt tous les fichiers proprietaires_{commune_code}.json dans data/proprietaires,
        puis appelle l'API Sogefi (proprietaires/filter) en une seule requête par fichier
        pour récupérer le nom (sogefi_denomination) de chaque propriétaire (id_dnupro).
        
        Structure initiale attendue (par exemple) :
          {
            "proprietaires": {
              "06088+f2582": ["06088000BH0001", "06088000HV0106"],
              "06088+f4017": ["06088000EC0305"]
            }
          }
        
        Après enrichissement :
          {
            "proprietaires": {
              "06088+f2582": {
                "parcelles": ["06088000BH0001", "06088000HV0106"],
                "owner_name": "DUPONT"
              },
              "06088+f4017": {
                "parcelles": ["06088000EC0305"],
                "owner_name": "MARTIN"
              }
            }
          }
        """
        logging.info("=== Enrichissement des propriétaires avec noms ===")

        PROPRIETAIRES_DIR = DATA_DIRS["proprietaires"]
        base_url = "https://api.sogefi-sig.com/2Besqie6xBzrxMj85psAPXm7cLy7A57eoEx/majic/v2/open/proprietaires/filter"
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

        if not os.path.exists(PROPRIETAIRES_DIR):
            logging.error(f"Répertoire {PROPRIETAIRES_DIR} inexistant.")
            return

        for filename in os.listdir(PROPRIETAIRES_DIR):
            if not (filename.startswith("proprietaires_") and filename.endswith(".json")):
                continue

            filepath = os.path.join(PROPRIETAIRES_DIR, filename)
            logging.debug(f"Traitement du fichier {filepath}")

            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as e:
                logging.error(f"Erreur de lecture JSON dans {filename}: {e}")
                continue

            proprietaires_dict = data.get("proprietaires", {})
            if not proprietaires_dict:
                logging.warning(f"Aucun propriétaire dans {filename}.")
                continue

            # 1) Uniformiser la structure pour stocker "owner_name"
            for owner_id, val in list(proprietaires_dict.items()):
                if isinstance(val, list):
                    proprietaires_dict[owner_id] = {
                        "parcelles": val
                    }

            # 2) Récupérer tous les owner_ids
            owner_ids = list(proprietaires_dict.keys())
            if not owner_ids:
                logging.info(f"{filename} : pas d'id_dnupro à enrichir.")
                continue

            # 3) Construction d'un param unique pour la requête
            ids_param = ",".join(owner_ids)
            params = {
                "id_dnupro[in]": ids_param,
                "sogefi_annee_archivee": "_last_",
                "limit": 50000  # On met un limit grand si l'API le supporte
            }

            try:
                resp = self.session.get(base_url, headers=headers, params=params, timeout=15)
                logging.debug(f"Statut HTTP: {resp.status_code}")
                if resp.status_code == 200:
                    proprietaires_array = resp.json()  # tableau d'objets
                    logging.info(f"{filename} : Réponse OK, {len(proprietaires_array)} propriétaires renvoyés.")
                    for p in proprietaires_array:
                        id_dnupro = p.get("id_dnupro")
                        personnes = p.get("personnes", [])
                        if personnes:
                            sogefi_denom = personnes[0].get("sogefi_denomination", "INCONNU")
                            if id_dnupro in proprietaires_dict:
                                proprietaires_dict[id_dnupro]["owner_name"] = sogefi_denom
                else:
                    logging.error(f"Erreur HTTP {resp.status_code} pour {filename}")
            except requests.RequestException as e:
                logging.error(f"Erreur réseau pour {filename}: {e}")

            # 4) Sauvegarde du fichier mis à jour
            data["proprietaires"] = proprietaires_dict
            try:
                with open(filepath, "w", encoding="utf-8") as out_f:
                    json.dump(data, out_f, ensure_ascii=False, indent=4)
                logging.info(f"{filename} mis à jour avec les noms de propriétaires.")
            except Exception as e:
                logging.error(f"Erreur lors de l'écriture de {filename}: {e}")

            # Pause entre les fichiers
            time.sleep(self.sleep_interval)

        logging.info("=== Fin de enrich_proprietaires_with_names ===")

    def merge_arrondissements(self, insee_global: str, contenance_min: float = 4000.0, remove_intermediate: bool = True) -> None:
        """
        Fusionne tous les fichiers parcelles_{arr_code}.geojson d'une grande ville (ex: Paris)
        en un seul 'parcelles_{insee_global}_merged.geojson',
        en ne conservant que les parcelles dont la contenance est > contenance_min (ex. 4000).
        Optionnel : supprime les fichiers intermédiaires d'arrondissements après fusion si remove_intermediate=True.
        """
        if insee_global not in self.BIG_CITY_ARRONDISSEMENTS:
            logging.error(f"Aucun arrondissements définis pour {insee_global}.")
            return

        arr_codes = self.BIG_CITY_ARRONDISSEMENTS[insee_global]

        merged_data = {
            "type": "FeatureCollection",
            "features": []
        }
        total_parcelles = 0
        total_kept = 0

        for arr_code in arr_codes:
            filename = f"parcelles_{arr_code}.geojson"
            filepath = os.path.join(DATA_DIRS["parcelles"], filename)

            if not os.path.exists(filepath):
                logging.warning(f"Fichier introuvable : {filepath}")
                continue

            # Lecture du GeoJSON
            with open(filepath, "r", encoding="utf-8") as f:
                try:
                    arr_data = json.load(f)
                except Exception as e:
                    logging.error(f"Erreur de lecture JSON pour {filepath} : {e}")
                    continue

                if arr_data.get("type") != "FeatureCollection":
                    logging.error(f"{filename} n'est pas un FeatureCollection valide.")
                    continue

                features = arr_data.get("features", [])
                logging.info(f"Arrondissement {arr_code} : {len(features)} parcelles chargées.")

                for feat in features:
                    total_parcelles += 1
                    props = feat.get("properties", {})
                    try:
                        c_val = float(props.get("contenance", 0))
                    except ValueError:
                        continue
                    if c_val > contenance_min:
                        merged_data["features"].append(feat)
                        total_kept += 1

            # Suppression du fichier intermédiaire si demandé
            if remove_intermediate:
                try:
                    os.remove(filepath)
                    logging.info(f"Fichier intermédiaire {filepath} supprimé.")
                except Exception as e:
                    logging.error(f"Impossible de supprimer {filepath}: {e}")

        # Sauvegarde du fichier final
        output_name = f"parcelles_{insee_global}_merged.geojson"
        output_path = os.path.join(DATA_DIRS["parcelles"], output_name)
        with open(output_path, "w", encoding="utf-8") as out_f:
            json.dump(merged_data, out_f, ensure_ascii=False, indent=4)
        logging.info(f"[Fusion] Fichier final : {output_path}")
        logging.info(f"[Fusion] Parcelles totales lues : {total_parcelles}, Parcelles > {contenance_min} m² retenues : {total_kept}")


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

    # 1) Récupération des parcelles pour communes "classiques" (sauf Paris, Marseille, Lyon)
    # for ville, code in villes_communes.items():
    #     if code in ["75056", "13055", "69123"]:
    #         # On saute Paris, Marseille, Lyon => on traitera leurs arrondissements
    #         logging.info(f"On ne télécharge pas directement {ville} (code {code}), on gère arrondissements.")
    #         continue
    #     fetcher.fetch_parcelles(ville, code)

    # # 2) Récupération des parcelles pour arrondissements de Paris, Marseille, Lyon
    # fetcher.fetch_parcelles_arrondissements("75056", "Paris")
    # fetcher.fetch_parcelles_arrondissements("13055", "Marseille")
    # fetcher.fetch_parcelles_arrondissements("69123", "Lyon")

    # # 3) Fusion : contenance > 4000, et on supprime les fichiers intermédiaires après fusion
    # fetcher.merge_arrondissements("75056", contenance_min=4000.0, remove_intermediate=True)
    # fetcher.merge_arrondissements("13055", contenance_min=4000.0, remove_intermediate=True)
    # fetcher.merge_arrondissements("69123", contenance_min=4000.0, remove_intermediate=True)

     # Au lieu d'un top_villes limité, on fait la boucle sur toutes les villes
    for ville in villes_communes.keys():
        # Récupération du réseau HTA (aérien + souterrain)
        #fetcher.fetch_reseau_hta(ville)
        # Récupération des axes routiers
        #fetcher.fetch_axes_routiers(ville)
        #fetcher.fetch_entreprises_btp(ville, radius=5.0, activite_principale="41.20A,41.20B")
        fetcher.enrich_proprietaires_with_names()

    # 5) Récupération des propriétaires de parcelles, un fichier par commune
    #fetcher.fetch_proprietaires_by_city()

    logging.info("🎉 Récupération et fusion des données terminée !")


if __name__ == "__main__":
    main()
