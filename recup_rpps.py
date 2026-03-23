import pandas as pd
import requests
import json
import io
import sys

URL_DIRECTE = "https://www.data.gouv.fr/fr/datasets/r/fffda7e9-0ea2-4c35-bba0-4496f3af935d"

def run():
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        print("⏳ Téléchargement du fichier RPPS...")
        response = requests.get(URL_DIRECTE, headers=headers, timeout=300)
        response.raise_for_status()

        df = pd.read_csv(io.BytesIO(response.content), sep='|', dtype=str, on_bad_lines='skip', low_memory=False)
        df.columns = df.columns.str.strip()
        
        print("🔍 Filtrage Audioprothésistes (Code 26)...")
        col_code = [c for c in df.columns if 'Code profession' in c][0]
        df_audio = df[df[col_code] == '26'].copy()
        
        resultats = []
        for _, row in df_audio.iterrows():
            cp = str(row.get('Code postal (coord. structure)', '')).strip()
            resultats.append({
                "nom": str(row.get("Nom d'exercice", "")).upper(),
                "prenom": str(row.get("Prénom d'exercice", "")).capitalize(),
                "rpps": str(row.get("Identifiant PP", "")),
                "entreprise": str(row.get("Raison sociale site", "NON RENSEIGNÉ")).upper(),
                "siret": str(row.get("Identifiant technique de la structure", "INCONNU")),
                "adresse": f"{row.get('Numéro Voie (coord. structure)', '')} {row.get('Libellé Voie (coord. structure)', '')} {cp}".replace('nan', '').strip(),
                "ville": str(row.get("Libellé commune (coord. structure)", "")).upper(),
                "dept": cp[:2] if len(cp) >= 2 else "00"
            })

        with open('data_france.json', 'w', encoding='utf-8') as out:
            json.dump(resultats, out, ensure_ascii=False, indent=2)
        
        print(f"✅ Terminé : {len(resultats)} fiches exportées.")

    except Exception as e:
        print(f"❌ ERREUR : {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    run()
