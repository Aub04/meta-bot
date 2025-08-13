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
NB_JOURS_GENERATION = 2      # Nombre de jours de planning à générer
RETENTION_JOURS = 2          # garde J-2 (purge plus vieux)
GSHEETS_MAX_RETRIES = 5
GSHEETS_RETRY_BASE = 1.5     # exponentiel (1.5^n) + jitter

FUSEAU_HORAIRE = "Europe/Paris"
LANGUE = "fr_FR.UTF-8"

# === 📓 Logger / erreurs
ACTIVER_LOG = True
FICHIER_LOG = "journal_erreurs.log"
