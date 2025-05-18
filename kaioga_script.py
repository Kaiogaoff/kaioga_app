# 📦 kaioga_script.py — version complète intégrée avec téléchargement local
import sys, subprocess
import csv, requests, re, io, time, difflib, os
import pandas as pd
from tqdm import tqdm
from ftplib import FTP
from concurrent.futures import ThreadPoolExecutor, as_completed

# 🔐 Identifiants FTP BunnyCDN
FTP_HOST = "storage.bunnycdn.com"
FTP_USER = "live-kaioga"
FTP_PASS = "76e54442-dde4-43c3-bf6afea2ff14-8fc4-4057"
CDN_BASE = "https://pull-kaioga.b-cdn.net"
XIMILAR_TOKEN = "6b1eaab219bc84644e665fd14ce4170e599dfe03"

# 📅 Saisie du nom du batch via input()
date_batch = input("📅 Nom du dossier batch (ex: 12_05_2025) : ").strip()
batch_folder = f"live_kaioga_{date_batch}"
chemin_export = f"{batch_folder}/batch_{date_batch}.csv"
chemin_erreurs = f"{batch_folder}/erreurs_{date_batch}.csv"

# 🔌 Connexion FTP
ftp = FTP(FTP_HOST)
ftp.login(user=FTP_USER, passwd=FTP_PASS)
print("\n📍 Répertoire courant après connexion :", ftp.pwd())

# 📁 Chargement des templates
lines = []
ftp.retrbinary("RETR templates/noms_pokemon_fr_en.csv", lambda b: lines.append(b.decode()))
reader = csv.DictReader("".join(lines).splitlines())
nom_fr_map = {row['english_name'].strip().lower(): row['french_name'].strip() for row in reader}

lines = []
ftp.retrbinary("RETR templates/voggt-products-import-template.csv", lambda b: lines.append(b.decode()))
reader = csv.DictReader("".join(lines).splitlines())
fieldnames = reader.fieldnames

# 🧠 Mapping flou EN > FR
def get_nom_fr_flexible(nom_en):
    mots = re.findall(r"\b\w+\b", nom_en.lower())
    best_score, best_fr = 0, nom_en.upper()
    for mot in mots:
        matches = difflib.get_close_matches(mot, nom_fr_map.keys(), n=1, cutoff=0.7)
        if matches:
            score = difflib.SequenceMatcher(None, mot, matches[0]).ratio()
            if score > best_score:
                best_score, best_fr = score, nom_fr_map[matches[0]]
    return best_fr

# 🔗 Construction des URLs BunnyCDN
def get_bunny_url(nom_fichier, dossier_public):
    return f"{CDN_BASE}/{dossier_public.strip('/')}/{nom_fichier}"

# 📂 Liste les fichiers dans le dossier batch (exclut ceux finissant par B ou b)
def lister_images(dossier_path):
    try:
        fichiers = []
        ftp.retrlines(f"NLST {dossier_path}", fichiers.append)
        return [os.path.basename(f) for f in fichiers if re.search(r'[^Bb]\.(jpe?g|png)$', f, re.IGNORECASE)]
    except Exception as e:
        print(f"❌ Erreur FTP pour {dossier_path} : {e}")
        return []

# ⚙️ API Ximilar unique
XIMILAR_URL = "https://api.ximilar.com/collectibles/v2/card_id"
headers = {
    "Content-Type": "application/json",
    "Authorization": f"Token {XIMILAR_TOKEN}",
    "User-Agent": "Mozilla/5.0 (compatible; KaiogaBot/1.0; +https://blazingtail.fr)"
}
def send_request_with_retries(payload, max_retries=3):
    for attempt in range(max_retries):
        try:
            res = requests.post(XIMILAR_URL, headers=headers, json=payload, timeout=60)
            res.raise_for_status()
            return res.json()
        except Exception as e:
            print(f"⚠️ Tentative {attempt + 1} échouée : {e}")
            time.sleep(2 ** attempt)
    raise TimeoutError("❌ Toutes les tentatives ont échoué")

# 🚀 Traitement global
def batch_traitement(fichiers):
    liens = {f: get_bunny_url(f, batch_folder) for f in fichiers}
    results, erreurs = [], []
    BATCH_SIZE = 10
    batches = [fichiers[i:i + BATCH_SIZE] for i in range(0, len(fichiers), BATCH_SIZE)]

    def process_batch(batch):
        res_lines = []
        urls = [liens[n] for n in batch]
        payload = {
            "records": [{"_url": url, "id": name} for url, name in zip(urls, batch)]
        }
        try:
            data = send_request_with_retries(payload)
            for record in data.get("records", []):
                nom_fichier = record.get("id", "inconnu.jpg")
                lien = liens.get(nom_fichier, "")
                best_match = record.get("_objects", [{}])[0].get("_identification", {}).get("best_match", {})
                if not best_match:
                    erreurs.append({"image": nom_fichier, "url": lien, "erreur": "Image non reconnue (200 vide)"})
                    continue
                nom_en = best_match.get("name", "").strip().lower()
                nom_fr = get_nom_fr_flexible(nom_en)
                full_name = best_match.get("full_name", "")
                title = full_name.replace(best_match.get("name", ""), nom_fr) if nom_en and nom_fr.lower() != nom_en else full_name
                ligne = {col: "" for col in fieldnames}
                mapping = {
                    "name": title,
                    "quantity": "1",
                    "startingPrice": "1",
                    "imagesUrls": lien,
                    "picUrl": f"{lien}||{lien}",
                    "cardLanguage": "japanese",
                    "cardGradingService": ""
                }
                for col, val in mapping.items():
                    if col in ligne:
                        ligne[col] = val
                res_lines.append(ligne)
        except Exception as e:
            for f in batch:
                ligne = {col: "" for col in fieldnames}
                ligne["name"] = f"{f} (ERREUR)"
                ligne["error"] = str(e)
                ligne["imagesUrls"] = liens.get(f, "")
                res_lines.append(ligne)
        return res_lines

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(process_batch, batch): batch for batch in batches}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Traitement images"):
            results.extend(future.result())

    return results, erreurs

# 🔁 Traitement du dossier batch unique
fichiers = lister_images(batch_folder)
results, erreurs = batch_traitement(fichiers)

# 📤 Export final CSV
df = pd.DataFrame(results)
csv_buf = io.BytesIO()
df.to_csv(csv_buf, index=False)
csv_buf.seek(0)
ftp.storbinary(f"STOR {chemin_export}", csv_buf)

# 💾 Sauvegarde locale pour téléchargement direct
local_export_path = f"static/{chemin_export.split('/')[-1]}"
os.makedirs("static", exist_ok=True)
with open(local_export_path, "wb") as f:
    f.write(csv_buf.getvalue())

# 📋 Export erreurs (s’il y en a)
if erreurs:
    df_err = pd.DataFrame(erreurs)
    err_buf = io.BytesIO()
    df_err.to_csv(err_buf, index=False)
    err_buf.seek(0)
    ftp.storbinary(f"STOR {chemin_erreurs}", err_buf)

    local_err_path = f"static/{chemin_erreurs.split('/')[-1]}"
    with open(local_err_path, "wb") as f:
        f.write(err_buf.getvalue())
    print(f"⚠️ Fichier d’erreurs généré : {chemin_erreurs}")

ftp.quit()
print(f"\n✅ Fichier exporté : {chemin_export}")
