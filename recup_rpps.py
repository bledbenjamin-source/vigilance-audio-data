import pandas as pd
import requests
import json
import io
import time

# URL officielle du fichier RPPS (Data.gouv)
URL_RPPS = "https://www.data.gouv.fr/fr/datasets/r/fffda7e9-0ea2-4c35-bba0-4496f3af935d"

def run():
    print("⏳ Étape 1 : Téléchargement du fichier RPPS...")
    try:
        resp = requests.get(URL_RPPS, timeout=300)
        resp.raise_for_status()
    except Exception as e:
        print(f"❌ Erreur lors du téléchargement : {e}")
        return

    # Lecture du CSV (séparateur | par défaut sur le RPPS)
    df = pd.read_csv(io.BytesIO(resp.content), sep='|', dtype=str, low_memory=False)
    
    # Étape 2 : Filtrage pour les audioprothésistes (Code profession 26)
    print("🧹 Étape 2 : Filtrage des audioprothésistes...")
    col_profession = [c for c in df.columns if 'Code profession' in c][0]
    df_audio = df[df[col_profession] == '26'].copy()
    
    # Identification des colonnes utiles
    col_siret = [c for c in df.columns if 'SIRET' in c][0]
    
    resultats = []
    total = len(df_audio)
    print(f"🌍 Étape 3 : Géocodage de {total} centres (cela peut prendre 10 min)...")

    for i, (_, row) in enumerate(df_audio.iterrows()):
        # Construction de l'adresse
        num_voie = str(row.get('Numéro Voie (coord. structure)', '')).replace('nan', '')
        libelle_voie = str(row.get('Libellé Voie (coord. structure)', '')).replace('nan', '')
        cp = str(row.get('Code postal (coord. structure)', '')).replace('nan', '')
        ville = str(row.get('Libellé Commune (coord. structure)', '')).replace('nan', '')
        
        adresse_complete = f"{num_voie} {libelle_voie} {cp} {ville}".strip()
        
        lat, lon = None, None
        
        # On ne tente le géocodage que si l'adresse semble valide
        if len(adresse_complete) > 10:
            try:
                # PAUSE DE SÉCURITÉ : 1 seconde toutes les 10 requêtes pour l'API Gouv
                if i % 10 == 0:
                    time.sleep(0.1) 
                
                # Requête vers l'API Adresse du gouvernement
                # Timeout de 5s pour éviter que le script ne bloque 25 min si l'API tombe
                geo_url = f"https://api-adresse.data.gouv.fr/search/?q={requests.utils.quote(adresse_complete)}&limit=1"
                geo_resp = requests.get(geo_url, timeout=5)
                
                if geo_resp.status_code == 200:
                    geo_data = geo_resp.json()
                    if geo_data['features']:
                        lon, lat = geo_data['features'][0]['geometry']['coordinates']
            except Exception:
                # En cas d'erreur sur une adresse, on continue simplement la boucle
                pass

        # Ajout des données nettoyées
        resultats.append({
            "nom": str(row.get("Nom d'exercice", "")).upper(),
            "prenom": str(row.get("Prénom d'exercice", "")).capitalize(),
            "rpps": str(row.get("Identifiant PP", "")),
            "entreprise": str(row.get("Raison sociale site", "")).upper(),
            "siret": str(row.get(col_siret, "")).strip(),
            "adresse": adresse_complete,
            "lat": lat,
            "lon": lon
        })

        # Affichage de la progression dans les logs GitHub
        if i % 100 == 0:
            print(f"📈 Progression : {i}/{total} centres traités...")

    # Étape 4 : Sauvegarde finale
    print("💾 Étape 4 : Sauvegarde du fichier JSON...")
    with open('data_france.json', 'w', encoding='utf-8') as out:
        json.dump(resultats, out, ensure_ascii=False, indent=2)
    
    print("✅ Terminé avec succès !")

if __name__ == "__main__":
    run()
