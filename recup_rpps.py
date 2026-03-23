import pandas as pd
import requests
import json
import zipfile
import io
import sys

# Lien permanent vers la ressource ZIP sur Data.gouv
URL_DIRECTE = "https://www.data.gouv.fr/fr/datasets/r/fffda7e9-0ea2-4c35-bba0-4496f3af935d"

def run():
    try:
        # On renforce le User-Agent pour passer les filtres de sécurité
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        
        print("⏳ Tentative de téléchargement (méthode robuste)...")
        # On télécharge en autorisant les redirections
        response = requests.get(URL_DIRECTE, headers=headers, allow_redirects=True, timeout=300)
        response.raise_for_status()

        # On vérifie si ce qu'on a reçu commence bien par "PK" (le code d'un fichier ZIP)
        if not response.content.startswith(b'PK'):
            print("❌ Le serveur n'a pas renvoyé un ZIP mais probablement du HTML.")
            print(f"Début du contenu reçu : {response.content[:100]}")
            sys.exit(1)

        print("📦 Fichier ZIP détecté. Extraction...")
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            # Liste des fichiers à l'intérieur
            file_list = z.namelist()
            # On cherche le gros fichier CSV (souvent le plus lourd)
            csv_file = [f for f in file_list if 'activite' in f and f.endswith('.txt')][0]
            
            print(f"📖 Lecture de {csv_file}...")
            with z.open(csv_file) as f:
                # On utilise 'low_memory=False' pour éviter les avertissements sur les types
                df = pd.read_csv(f, sep='|', dtype=str, on_bad_lines='skip', low_memory=False)

        # Nettoyage des colonnes (suppression des espaces invisibles)
        df.columns = df.columns.str.strip()
        
        # Filtrage par CODE PROFESSION 26
        print("🔍 Filtrage des Audioprothésistes (Code 26)...")
        # On cherche la colonne qui contient 'Code profession'
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

        print(f"💾 Sauvegarde de {len(resultats)} fiches dans data_france.json")
        with open('data_france.json', 'w', encoding='utf-8') as out:
            json.dump(resultats, out, ensure_ascii=False, indent=2)
        
        print("✅ Terminé avec succès !")

    except Exception as e:
        print(f"❌ ERREUR : {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    run()
