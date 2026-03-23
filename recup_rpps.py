import pandas as pd
import requests
import json
import io
import sys

# Ton lien direct vers la ressource (qui est donc un .txt ou .csv en réalité)
URL_DIRECTE = "https://www.data.gouv.fr/fr/datasets/r/fffda7e9-0ea2-4c35-bba0-4496f3af935d"

def run():
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        
        print("⏳ Téléchargement des données RPPS...")
        response = requests.get(URL_DIRECTE, headers=headers, timeout=300)
        response.raise_for_status()

        # On transforme le texte reçu en un "fichier" que Pandas peut lire
        print("📖 Lecture du flux de données...")
        # L'encodage est souvent utf-8 ou latin-1 pour les fichiers de l'État
        df = pd.read_csv(io.BytesIO(response.content), sep='|', dtype=str, on_bad_lines='skip', low_memory=False)

        # Nettoyage des colonnes
        df.columns = df.columns.str.strip()
        
        # Filtrage par CODE PROFESSION 26
        print("🔍 Filtrage des Audioprothésistes (Code 26)...")
        # On vérifie quelle colonne contient le code profession
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

        print(f"💾 Sauvegarde de {len(resultats)} audioprothésistes...")
        with open('data_france.json', 'w', encoding='utf-8') as out:
            json.dump(resultats, out, ensure_ascii=False, indent=2)
        
        print("✅ Terminé avec succès ! Le fichier data_france.json est prêt.")

    except Exception as e:
        print(f"❌ ERREUR : {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    run()
