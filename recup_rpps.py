import pandas as pd
import requests
import json
import zipfile
import io
import os

# URL officielle du ZIP de l'Annuaire Santé (Libre Accès)
URL_RPPS = "https://annuaire.sante.fr/web/site-pro/extractions-publiques?p_p_id=abonnementportlet_WAR_annuaireportlet&p_p_lifecycle=2&p_p_state=normal&p_p_mode=view&p_p_resource_id=exportFichierExtraction&p_p_cacheability=cacheLevelPage&_abonnementportlet_WAR_annuaireportlet_nomFichier=PS_LibreAcces_Personne_activite.zip"

def run():
    print("⏳ Téléchargement du ZIP RPPS (environ 80Mo)...")
    r = requests.get(URL_RPPS)
    if r.status_code != 200:
        print("Erreur de téléchargement")
        return

    z = zipfile.ZipFile(io.BytesIO(r.content))
    nom_fichier_interne = z.namelist()[0]
    
    print(f"📂 Lecture du fichier : {nom_fichier_interne}")
    # On lit le CSV avec le séparateur | spécifique à l'ANS
    with z.open(nom_fichier_interne) as f:
        df = pd.read_csv(f, sep='|', dtype=str, encoding='utf-8', on_bad_lines='skip')

    print("🔍 Filtrage des audioprothésistes...")
    # On garde uniquement la profession Audioprothésiste
    df_audio = df[df['Profession'] == 'Audioprothésiste'].copy()
    
    resultats = []
    for _, row in df_audio.iterrows():
        cp = str(row.get('Code postal (coord. structure)', ''))
        # On prépare un dictionnaire propre pour chaque pro
        resultats.append({
            "nom": str(row.get("Nom d'exercice", "")).upper(),
            "prenom": str(row.get("Prénom d'exercice", "")).capitalize(),
            "rpps": str(row.get("Identifiant PP", "")),
            "adresse": f"{row.get('Numéro Voie (coord. structure)', '')} {row.get('Libellé Voie (coord. structure)', '')} {cp}".replace('nan', '').strip(),
            "dept": cp[:2] if len(cp) >= 2 else "00",
            "savoir_faire": str(row.get("Libellé savoir-faire", "Non renseigné"))
        })

    # On sauvegarde le résultat en JSON
    with open('data_france.json', 'w', encoding='utf-8') as f:
        json.dump(resultats, f, ensure_ascii=False, indent=2)
    
    print(f"✅ Terminé ! {len(resultats)} professionnels enregistrés dans data_france.json")

if __name__ == "__main__":
    run()
