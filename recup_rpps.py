import pandas as pd
import requests
import json
import zipfile
import io
import sys

URL_DIRECTE = "https://www.data.gouv.fr/fr/datasets/r/fffda7e9-0ea2-4c35-bba0-4496f3af935d"

def run():
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        
        print("⏳ Téléchargement du ZIP RPPS...")
        response = requests.get(URL_DIRECTE, headers=headers, timeout=300)
        response.headers.get('content-type') # Verification
        response.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            file_list = z.namelist()
            csv_file = [f for f in file_list if 'activite' in f][0]
            
            with z.open(csv_file) as f:
                print(f"📖 Lecture de {csv_file}...")
                # On force le type string pour les codes pour éviter que 06 devienne 6
                df = pd.read_csv(f, sep='|', dtype=str, on_bad_lines='skip', low_memory=False)

        # Nettoyage des noms de colonnes
        df.columns = df.columns.str.strip()
        
        # Filtrage par CODE PROFESSION 26
        print("🔍 Filtrage des Audioprothésistes (Code 26)...")
        # La colonne exacte est souvent 'Code profession'
        if 'Code profession' in df.columns:
            df_audio = df[df['Code profession'] == '26'].copy()
        else:
            # Sécurité au cas où le nom de colonne varie légèrement
            col_code = [c for c in df.columns if 'Code profession' in c][0]
            df_audio = df[df[col_code] == '26'].copy()
        
        resultats = []
        for _, row in df_audio.iterrows():
            cp = str(row.get('Code postal (coord. structure)', '')).strip()
            resultats.append({
                "nom": str(row.get("Nom d'exercice", "")).upper(),
                "prenom": str(row.get("Prénom d'exercice", "")).capitalize(),
                "rpps": str(row.get("Identifiant PP", "")),
                "adresse": f"{row.get('Numéro Voie (coord. structure)', '')} {row.get('Libellé Voie (coord. structure)', '')} {cp}".replace('nan', '').strip(),
                "dept": cp[:2] if len(cp) >= 2 else "00"
            })

        with open('data_france.json', 'w', encoding='utf-8') as out:
            json.dump(resultats, out, ensure_ascii=False, indent=2)
        
        print(f"🚀 Terminé ! {len(resultats)} audioprothésistes (code 26) enregistrés.")

    except Exception as e:
        print(f"❌ ERREUR : {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    run()
