import os
import time
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import pytz
import requests
import config

# ======================
# Helpers / Parameters
# ======================

def _tz():
    try:
        return pytz.timezone(config.FUSEAU_HORAIRE)
    except Exception:
        return pytz.timezone("Europe/Paris")

TELEGRAM_TOKEN = getattr(config, "TELEGRAM_TOKEN", os.getenv("TELEGRAM_TOKEN", ""))
if not TELEGRAM_TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN manquant (config.py ou variable d'env).")

TELEGRAM_TIMEOUT = getattr(config, "TELEGRAM_TIMEOUT", 10)  # seconds
TELEGRAM_MAX_RETRIES = getattr(config, "TELEGRAM_MAX_RETRIES", 3)
SEND_WINDOW_MINUTES = getattr(config, "SEND_WINDOW_MINUTES", None)  # None = pas de fenêtre

API_BASE = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

def col_idx_to_a1(idx1):
    # idx1 is 1-based index -> column letters (A, B, ... AA, AB ...)
    s = ""
    while idx1 > 0:
        idx1, rem = divmod(idx1 - 1, 26)
        s = chr(65 + rem) + s
    return s

def localize_safe(series_dt_naive, tz):
    return (series_dt_naive
            .dt.tz_localize(tz, ambiguous="infer", nonexistent="shift_forward"))

def send_telegram_message(chat_id, text):
    url = f"{API_BASE}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": False}
    return _post_with_retry(url, payload)

def send_telegram_photo(chat_id, photo_url, caption=None):
    url = f"{API_BASE}/sendPhoto"
    payload = {"chat_id": chat_id, "photo": photo_url}
    if caption:
        payload["caption"] = caption
    return _post_with_retry(url, payload)

def _post_with_retry(url, payload):
    # Retries for 429 / certain 5xx
    for attempt in range(1, TELEGRAM_MAX_RETRIES + 1):
        try:
            r = requests.post(url, data=payload, timeout=TELEGRAM_TIMEOUT)
        except requests.RequestException as e:
            if attempt >= TELEGRAM_MAX_RETRIES:
                return False, f"request_exception:{e}"
            time.sleep(2 ** attempt)
            continue

        try:
            data = r.json()
        except Exception:
            data = {"ok": False, "error_code": r.status_code, "description": "invalid_json"}

        if r.status_code == 429:
            retry_after = 1
            try:
                retry_after = int(data.get("parameters", {}).get("retry_after", 1))
            except Exception:
                pass
            time.sleep(retry_after + 1)
            continue

        if r.status_code >= 500 and attempt < TELEGRAM_MAX_RETRIES:
            time.sleep(2 ** attempt)
            continue

        if r.ok and data.get("ok", False):
            return True, "ok"

        # Other client errors: no retry
        return False, f"{data.get('error_code','?')}:{data.get('description','unknown')}"

    return False, "max_retries_exceeded"

# ======================
# Main
# ======================

def lancer_bot():
    tz = _tz()

    # Auth Sheets
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(config.CHEMIN_CLE_JSON, scopes=scope)
    client = gspread.authorize(creds)

    # Open planning
    ws_planning = client.open(config.FICHIER_PLANNING).worksheet(config.FEUILLE_PLANNING)

    # Read all records + header to compute row numbers
    rows = ws_planning.get_all_values()
    if not rows:
        print("Planning vide.")
        return
    header = rows[0]
    data_rows = rows[1:]
    if not data_rows:
        print("Aucune ligne planning.")
        return

    df = pd.DataFrame(data_rows, columns=header)

    # Ensure required columns exist
    required = ["client","programme","saison","chat_id","date","heure","type","avancement","message","format","url","envoye"]
    for c in required:
        if c not in df.columns:
            df[c] = ""

    # Normalize types
    df["programme"] = df["programme"].apply(lambda x: str(x).zfill(3))
    df["saison"] = pd.to_numeric(df["saison"], errors="coerce").fillna(1).astype(int)
    df["avancement"] = pd.to_numeric(df["avancement"], errors="coerce").fillna(1).astype(int)

    # Build datetime
    def mk_dt(row):
        s = f"{row['date']} {row['heure']}".strip()
        try:
            return pd.to_datetime(s, errors="coerce")
        except Exception:
            return pd.NaT

    df["_dt_naive"] = df.apply(mk_dt, axis=1)
    mask = df["_dt_naive"].notna()
    if mask.any():
        df.loc[mask, "_dt"] = localize_safe(df.loc[mask, "_dt_naive"].astype("datetime64[ns]"), tz)
    else:
        df["_dt"] = pd.NaT

    now_local = datetime.now(tz)

    # Filter candidates: envoye == "non" and datetime <= now (optionally within window)
    has_msg = df["message"].astype(str).str.strip() != ""
    
    elig = (
        (df["envoye"].str.lower() == "non")
        & df["_dt"].notna()
        & (df["_dt"] <= now_local)
        & has_msg             # <<--- AJOUT
    )
    
    if SEND_WINDOW_MINUTES is not None:
        window_start = now_local - timedelta(minutes=int(SEND_WINDOW_MINUTES))
        elig = elig & (df["_dt"] >= window_start)
    
    df_send = df[elig].copy()

    # Column indices (1-based) for A1 ranges
    col_map = {name: (i+1) for i, name in enumerate(header)}
    if "envoye" not in col_map:
        raise RuntimeError("Colonne 'envoye' absente de la feuille planning.")
    envoye_col_idx = col_map["envoye"]
    envoye_col_letter = col_idx_to_a1(envoye_col_idx)

    # Send loop
    updates = []  # list of (row_index_1based, value)
    for idx, row in df_send.iterrows():
        # Worksheet row number = idx in df + header row (1) + 1
        ws_row_num = int(idx) + 2

        chat_id = row["chat_id"]
        raw_text = str(row["message"]).strip()  # on teste le "message" du planning, pas le texte après append url
        fmt = str(row["format"]).strip().lower()
        url = str(row["url"]).strip()
        
        # Ne rien envoyer si message vide
        if not raw_text:
            print(f"⏭️ Skip (message vide) ligne {ws_row_num} -> chat_id={chat_id}")
            continue
        
        try:
            if fmt == "image" and url:
                success, err = send_telegram_photo(chat_id, url, caption=raw_text)
            else:
                text = raw_text
                # si tu veux garder l'ajout du lien quand ce n'est pas une image :
                if url:
                    text = f"{text}\n{url}"
                success, err = send_telegram_message(chat_id, text)
        except Exception as e:
            success = False
            err = f"exception:{e}"


        if success:
            updates.append((ws_row_num, "oui"))
            print(f"✅ Envoyé (ligne {ws_row_num}) -> chat_id={chat_id}")
        else:
            print(f"⚠️ Echec envoi (ligne {ws_row_num}) -> chat_id={chat_id} ; {err}")

    # Batch update only changed 'envoye' cells
    if updates:
        batch_body = {
            "valueInputOption": "RAW",
            "data": []
        }
        for rownum, value in updates:
            rng = f"{envoye_col_letter}{rownum}:{envoye_col_letter}{rownum}"
            batch_body["data"].append({
                "range": f"{config.FEUILLE_PLANNING}!{rng}",
                "values": [[value]]
            })
        ws_planning.spreadsheet.values_batch_update(batch_body)
        print(f"Marqués 'envoye=oui' pour {len(updates)} ligne(s).")

    print(f"🕒 Terminé à {now_local.strftime('%Y-%m-%d %H:%M:%S %Z')}")

if __name__ == "__main__":
    lancer_bot()
