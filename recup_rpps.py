import pandas as pd
import requests
import json
import zipfile
import io

URL_RPPS = "https://annuaire.sante.fr/web/site-pro/extractions-publiques?p_p_id=abonnementportlet_WAR_annuaireportlet&p_p_lifecycle=2&p_p_state=normal&p_p_mode=view&p_p_resource_id=exportFichierExtraction&p_p_cacheability=cacheLevelPage&_abonnementportlet_WAR_annuaireportlet_nomFichier=PS_LibreAcces_Personne_activite.zip"

def run():
    print("⏳ Téléchargement du ZIP RPPS...")
    r = requests.get(URL_RPPS)
    if r.status_code != 200:
        print("❌ Erreur de téléchargement du fichier source.")
        return

    z = zipfile.ZipFile(io.BytesIO(r.content))
    nom_fichier_interne = z.namelist()[0]
    
    # Stratégie mémoire : On ne charge QUE les colonnes utiles !
    colonnes_utiles = [
        "Profession", "Nom d'exercice", "Prénom d'exercice", "Identifiant PP",
        "Numéro Voie (coord. structure)", "Libellé Voie (coord. structure)", 
        "Code postal (coord. structure)", "Libellé savoir-faire"
    ]

    print(f"📂 Lecture optimisée du fichier : {nom_fichier_interne}")
    with z.open(nom_fichier_interne) as f:
        # On ajoute usecols pour ne pas saturer la RAM de GitHub
        df = pd.read_csv(f, sep='|', dtype=str, usecols=colonnes_utiles, encoding='utf-8', on_bad_lines='skip')

    print("🔍 Filtrage des audioprothésistes...")
    df_audio = df[df['Profession'] == 'Audioprothésiste'].copy()
    
    resultats = []
    for _, row in df_audio.iterrows():
        cp = str(row.get('Code postal (coord. structure)', '')).strip()
        resultats.append({
            "nom": str(row.get("Nom d'exercice", "")).upper(),
            "prenom": str(row.get("Prénom d'exercice", "")).capitalize(),
            "rpps": str(row.get("Identifiant PP", "")),
            "adresse": f"{row.get('Numéro Voie (coord. structure)', '')} {row.get('Libellé Voie (coord. structure)', '')} {cp}".replace('nan', '').strip(),
            "dept": cp[:2] if len(cp) >= 2 else "00",
            "savoir_faire": str(row.get("Libellé savoir-faire", "Non renseigné"))
        })

    with open('data_france.json', 'w', encoding='utf-8') as f:
        json.dump(resultats, f, ensure_ascii=False, indent=2)
    
    print(f"✅ Terminé ! {len(resultats)} professionnels enregistrés dans data_france.json")

if __name__ == "__main__":
    run()
