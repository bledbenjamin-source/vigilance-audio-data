import pandas as pd
import requests
import json
import io
import time

URL_RPPS = "https://www.data.gouv.fr/fr/datasets/r/fffda7e9-0ea2-4c35-bba0-4496f3af935d"

def run():
    print("⏳ Téléchargement du RPPS...")
    resp = requests.get(URL_RPPS, timeout=300)
    df = pd.read_csv(io.BytesIO(resp.content), sep='|', dtype=str, low_memory=False)
    
    # Filtrage Audio
    col_profession = [c for c in df.columns if 'Code profession' in c][0]
    df_audio = df[df[col_profession] == '26'].copy()
    
    col_siret = [c for c in df.columns if 'SIRET' in c][0]
    
    resultats = []
    print(f"🌍 Géocodage de {len(df_audio)} centres...")

    for i, (_, row) in enumerate(df_audio.iterrows()):
        adresse_complete = f"{row.get('Numéro Voie (coord. structure)', '')} {row.get('Libellé Voie (coord. structure)', '')} {row.get('Code postal (coord. structure)', '')}".strip()
        
        # On ne géocode que si l'adresse est valide
        lat, lon = None, None
        if len(adresse_complete) > 10:
            try:
                # Appel API Gouv pour avoir le point GPS
                geo = requests.get(f"https://api-adresse.data.gouv.fr/search/?q={adresse_complete}&limit=1").json()
                if geo['features']:
                    lon, lat = geo['features'][0]['geometry']['coordinates']
            except: pass

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
        if i % 100 == 0: print(f"Progrès : {i}/{len(df_audio)}")

    with open('data_france.json', 'w', encoding='utf-8') as out:
        json.dump(resultats, out, ensure_ascii=False)
    print("✅ Fichier enrichi avec GPS sauvegardé.")

if __name__ == "__main__":
    run()
