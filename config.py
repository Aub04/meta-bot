import os

# Clé JSON Google (chemin, par défaut "credentials.json" dans le workflow)
CHEMIN_CLE_JSON = os.environ.get("CHEMIN_CLE_JSON", "credentials.json")

# Google Sheet – Fichiers
FICHIER_CLIENTS = "Suivi Programme et heure client"
FICHIER_PLANNING = "planning"
FICHIER_PROGRAMMES = "Méta-université_Programmes"

# Google Sheet – Feuilles internes
FEUILLE_CLIENTS = "Clients"
FEUILLE_PLANNING = "Planning"

# === API Telegram ===
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN', 'VOTRE_TOKEN_PAR_DEFAUT')


# === ⏱️ Autres paramètres
NB_JOURS_GENERATION = 4 # Nombre de jours de planning à générer
FUSEAU_HORAIRE = "Europe/Paris"
LANGUE = "fr_FR.UTF-8"

# === 📓 Logger / erreurs
ACTIVER_LOG = True
FICHIER_LOG = "journal_erreurs.log"
