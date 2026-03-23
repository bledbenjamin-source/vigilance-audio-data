import pandas as pd
import requests
import json
import io
import sys

URL_RPPS = "https://www.data.gouv.fr/fr/datasets/r/fffda7e9-0ea2-4c35-bba0-4496f3af935d"

def run():
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        print("⏳ Téléchargement du RPPS complet...")
        resp = requests.get(URL_RPPS, headers=headers, timeout=300)
        
        # On lit le fichier avec le séparateur |
        df = pd.read_csv(io.BytesIO(resp.content), sep='|', dtype=str, on_bad_lines='skip', low_memory=False)
        df.columns = df.columns.str.strip()
        
        # Filtrage Audioprothésistes (Code 26)
        col_profession = [c for c in df.columns if 'Code profession' in c][0]
        df_audio = df[df[col_profession] == '26'].copy()
        
        # Identification des bonnes colonnes pour le SIRET
        # Dans le fichier ANS, c'est souvent "Identifiant SIRET de la structure"
        col_siret = "Identifiant SIRET de la structure"
        if col_siret not in df.columns:
            # Recherche alternative si le nom de colonne a changé
            col_siret = [c for c in df.columns if 'SIRET' in c][0]

        resultats = []
        for _, row in df_audio.iterrows():
            cp = str(row.get('Code postal (coord. structure)', '')).strip()
            # On récupère le VRAI SIRET
            siret_brut = str(row.get(col_siret, "INCONNU")).strip()
            
            resultats.append({
                "nom": str(row.get("Nom d'exercice", "")).upper(),
                "prenom": str(row.get("Prénom d'exercice", "")).capitalize(),
                "rpps": str(row.get("Identifiant PP", "")),
                "entreprise": str(row.get("Raison sociale site", "NON RENSEIGNÉ")).upper(),
                "siret": siret_brut,
                "adresse": f"{row.get('Numéro Voie (coord. structure)', '')} {row.get('Libellé Voie (coord. structure)', '')} {cp}".replace('nan', '').strip(),
                "ville": str(row.get("Libellé commune (coord. structure)", "")).upper(),
                "dept": cp[:2] if len(cp) >= 2 else "00"
            })

        with open('data_france.json', 'w', encoding='utf-8') as out:
            json.dump(resultats, out, ensure_ascii=False, indent=2)
        
        print(f"✅ Succès : {len(resultats)} fiches extraites avec le champ SIRET : {col_siret}")

    except Exception as e:
        print(f"❌ Erreur lors de l'extraction : {e}")
        sys.exit(1)

if __name__ == "__main__":
    run()
