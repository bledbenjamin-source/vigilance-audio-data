import pandas as pd
import requests
import json
import io
import time
import sys

# URL officielle du fichier RPPS
URL_RPPS = "https://www.data.gouv.fr/fr/datasets/r/fffda7e9-0ea2-4c35-bba0-4496f3af935d"

def run():
    # On force l'encodage pour éviter les erreurs sur GitHub
    sys.stdout.reconfigure(encoding='utf-8')

    print("⏳ Étape 1 : Téléchargement du fichier RPPS...", flush=True)
    try:
        resp = requests.get(URL_RPPS, timeout=300)
        resp.raise_for_status()
    except Exception as e:
        print(f"❌ Erreur lors du téléchargement : {e}", flush=True)
        return

    df = pd.read_csv(io.BytesIO(resp.content), sep='|', dtype=str, low_memory=False)
    
    print("🧹 Étape 2 : Filtrage des audioprothésistes...", flush=True)
    col_profession = [c for c in df.columns if 'Code profession' in c][0]
    df_audio = df[df[col_profession] == '26'].copy()
    
    col_siret = [c for c in df.columns if 'SIRET' in c][0]
    
    resultats = []
    total = len(df_audio)
    print(f"🌍 Étape 3 : Géocodage de {total} centres...", flush=True)

for i, (_, row) in enumerate(df_audio.iterrows()):
        # 1. Récupération de TOUTES les parties de l'adresse (avec .get pour éviter les erreurs si la colonne manque)
        num_voie = str(row.get('Numéro Voie (coord. structure)', '')).replace('nan', '')
        indice_voie = str(row.get('Indice de répétition (coord. structure)', '')).replace('nan', '')
        type_voie = str(row.get('Code type de voie (coord. structure)', '')).replace('nan', '')
        libelle_voie = str(row.get('Libellé Voie (coord. structure)', '')).replace('nan', '')
        cp = str(row.get('Code postal (coord. structure)', '')).replace('nan', '').split('.')[0] # Au cas où il y a un .0
        ville = str(row.get('Libellé Commune (coord. structure)', '')).replace('nan', '')
        
        # On assemble proprement (le split/join enlève les espaces en trop si une variable est vide)
        adresse_brute = f"{num_voie} {indice_voie} {type_voie} {libelle_voie} {cp} {ville}"
        adresse_complete = ' '.join(adresse_brute.split())
        
        lat, lon = None, None
        
        if len(adresse_complete) > 5:
            try:
                # Respect de la limite API (50 requêtes/sec max)
                if i % 20 == 0:
                    time.sleep(0.1) 
                
                # REQUÊTE AMÉLIORÉE : on isole le code postal dans un paramètre dédié pour aider l'API
                geo_url = f"https://api-adresse.data.gouv.fr/search/?q={requests.utils.quote(adresse_complete)}&postcode={cp}&limit=1"
                geo_resp = requests.get(geo_url, timeout=5)
                
                if geo_resp.status_code == 200:
                    geo_data = geo_resp.json()
                    if geo_data['features']:
                        feature = geo_data['features'][0]
                        # Optionnel : Tu pourrais vérifier la précision ici avec feature['properties']['score'] ou feature['properties']['type'] (housenumber vs street)
                        lon, lat = feature['geometry']['coordinates']
            except Exception:
                pass

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

        # AFFICHAGE TOUTES LES 50 LIGNES
        if i % 50 == 0:
            print(f"📈 Progression : {i}/{total} centres traités...", flush=True)

    print("💾 Étape 4 : Sauvegarde du fichier JSON...", flush=True)
    with open('data_france.json', 'w', encoding='utf-8') as out:
        json.dump(resultats, out, ensure_ascii=False, indent=2)
    
    print("✅ Terminé avec succès !", flush=True)

if __name__ == "__main__":
    run()
