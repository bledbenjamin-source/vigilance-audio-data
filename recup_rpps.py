import pandas as pd
import requests
import json
import zipfile
import io
import sys

# Nouvelle URL via les serveurs de Data.gouv (plus stable pour les scripts)
URL_DATA_GOUV = "https://www.data.gouv.fr/fr/datasets/r/00966f3f-4318-4720-9922-839075e89d53"

def run():
    try:
        # On ajoute un "User-Agent" pour faire croire qu'on est un navigateur normal
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        
        print("⏳ Téléchargement depuis Data.gouv...")
        r = requests.get(URL_DATA_GOUV, headers=headers, timeout=120)
        r.raise_for_status()

        z = zipfile.ZipFile(io.BytesIO(r.content))
        # On cherche le fichier qui contient 'Personne_activite'
        nom_fichier = [n for n in z.namelist() if 'activite' in n][0]
        
        print(f"📂 Ouverture de {nom_fichier}...")
        with z.open(nom_fichier) as f:
            # Lecture optimisée : on ne charge que ce dont on a besoin
            df = pd.read_csv(f, sep='|', dtype=str, on_bad_lines='skip', low_memory=False)

        # Nettoyage des colonnes
        df.columns = df.columns.str.strip()

        print("🔍 Filtrage Audioprothésistes...")
        # On cherche la colonne profession de manière flexible
        col_prof = [c for c in df.columns if 'Profession' in c][0]
        df_audio = df[df[col_prof].str.contains('Audio', na=False, case=False)].copy()
        
        resultats = []
        for _, row in df_audio.iterrows():
            cp = str(row.get('Code postal (coord. structure)', '')).strip()
            # On prend les 2 premiers chiffres pour le département
            dept = cp[:2] if len(cp) >= 2 else "00"
            
            resultats.append({
                "nom": str(row.get("Nom d'exercice", "NOM INCONNU")).upper(),
                "prenom": str(row.get("Prénom d'exercice", "")).capitalize(),
                "rpps": str(row.get("Identifiant PP", "0")),
                "adresse": f"{row.get('Numéro Voie (coord. structure)', '')} {row.get('Libellé Voie (coord. structure)', '')} {cp}".replace('nan', '').strip(),
                "dept": dept
            })

        print(f"💾 Sauvegarde de {len(resultats)} fiches...")
        with open('data_france.json', 'w', encoding='utf-8') as out:
            json.dump(resultats, out, ensure_ascii=False, indent=2)
        
        print("✅ Terminé avec succès !")

    except Exception as e:
        print(f"❌ ERREUR : {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    run()
