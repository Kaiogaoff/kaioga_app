from flask import Flask, render_template, request, redirect, url_for
import subprocess
import threading
import os

app = Flask(__name__)

# Répertoire où se trouvent les batchs (à adapter si besoin)
BATCHS_DIR = "live_kaioga_batches"
LOG_FILE = "execution_log.txt"

# Fonction pour exécuter le script Python avec le nom du batch
def run_script(batch_name):
    with open(LOG_FILE, "w") as log:
        subprocess.run(["python3", "kaioga_script.py"], input=f"{batch_name}\n", text=True, stdout=log, stderr=log)

@app.route("/", methods=["GET", "POST"])
def index():
    message = None
    if request.method == "POST":
        batch_name = request.form.get("batch")
        if not batch_name:
            message = "Veuillez entrer un nom de dossier."
        else:
            threading.Thread(target=run_script, args=(batch_name,), daemon=True).start()
            return redirect(url_for("progress", batch=batch_name))
    return render_template("index.html", message=message)

@app.route("/progress")
def progress():
    batch = request.args.get("batch")
    if not os.path.exists(LOG_FILE):
        return "Aucune exécution en cours."
    with open(LOG_FILE) as f:
        log_content = f.read()
    return render_template("progress.html", batch=batch, log=log_content)

if __name__ == "__main__":
    app.run(debug=True, port=5000)
