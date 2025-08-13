import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials
import pandas as pd
from datetime import datetime, timedelta
import time
import random
import pytz
import config

# ======================
# Helpers
# ======================

def _tz():
    try:
        return pytz.timezone(config.FUSEAU_HORAIRE)
    except Exception:
        return pytz.timezone("Europe/Paris")

def _to_time_hms(val):
    if pd.isna(val) or str(val).strip() == "":
        return ""
    s = str(val).strip()
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            t = pd.to_datetime(s, format=fmt).time()
            return t.strftime("%H:%M:%S")
        except Exception:
            pass
    try:
        t = pd.to_datetime(s).time()
        return t.strftime("%H:%M:%S")
    except Exception:
        return ""

def _localize_safe(series_dt_naive, tz):
    return (series_dt_naive
            .dt.tz_localize(tz, ambiguous="infer", nonexistent="shift_forward"))

def _parse_jours_diffusion(s):
    if pd.isna(s):
        return set()
    raw = str(s).lower().replace(";", ",")
    parts = [x.strip() for x in raw.split(",") if x.strip()]
    mapping = {
        "monday": "lundi", "tuesday": "mardi", "wednesday": "mercredi",
        "thursday": "jeudi", "friday": "vendredi", "saturday": "samedi", "sunday": "dimanche",
    }
    out = []
    for p in parts:
        out.append(mapping.get(p, p))
    return set(out)

def _weekday_fr(dt_date):
    jours = ["lundi","mardi","mercredi","jeudi","vendredi","samedi","dimanche"]
    return jours[dt_date.weekday()]

# ---- Retriable wrapper for Google API calls ----
def _retry(fn, *args, **kwargs):
    max_attempts = getattr(config, "GSHEETS_MAX_RETRIES", 5)
    base = getattr(config, "GSHEETS_RETRY_BASE", 1.5)
    for attempt in range(1, max_attempts + 1):
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            code = getattr(e, "response", None)
            status = None
            try:
                status = e.response.status_code
            except Exception:
                pass
            # Retry on 5xx and 429; else raise
            if status in (429, 500, 502, 503, 504) or status is None:
                sleep = (base ** attempt) + random.random()
                print(f"[retry {attempt}/{max_attempts}] Google API {status or '??'} -> sleep {sleep:.1f}s")
                time.sleep(sleep)
                continue
            raise
        except Exception as e:
            # network/transient: retry
            sleep = (base ** attempt) + random.random()
            print(f"[retry {attempt}/{max_attempts}] exception {type(e).__name__}: {e} -> sleep {sleep:.1f}s")
            time.sleep(sleep)
            continue
    # Last attempt
    return fn(*args, **kwargs)

def generer_planning():
    tz = _tz()

    # Auth
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(config.CHEMIN_CLE_JSON, scopes=scope)
    client = gspread.authorize(creds)

    # Open sheets with retry
    ws_clients = _retry(client.open(config.FICHIER_CLIENTS).worksheet, config.FEUILLE_CLIENTS)
    ws_planning = _retry(client.open(config.FICHIER_PLANNING).worksheet, config.FEUILLE_PLANNING)

    # Read clients
    df_clients = pd.DataFrame(_retry(ws_clients.get_all_records))
    if df_clients.empty:
        print("Aucun client.")
        return

    # Ensure columns (legacy before slots refactor)
    cols_req = [
        "Client","Programme","Saison","chat_id",
        "Date de Démarrage","Jours de Diffusion",
        "Heure Conseil","Heure Aphorisme","Heure Réflexion",
    ]
    for c in cols_req:
        if c not in df_clients.columns:
            df_clients[c] = ""

    df_clients["Programme"] = df_clients["Programme"].apply(lambda x: str(x).zfill(3))
    df_clients["Saison"] = pd.to_numeric(df_clients["Saison"], errors="coerce").fillna(1).astype(int)
    df_clients["Date de Démarrage"] = pd.to_datetime(df_clients["Date de Démarrage"], errors="coerce").dt.date

    for hcol in ["Heure Conseil","Heure Aphorisme","Heure Réflexion"]:
        df_clients[hcol] = df_clients[hcol].apply(_to_time_hms)

    df_clients["jours_set"] = df_clients["Jours de Diffusion"].apply(_parse_jours_diffusion)

    NB_JOURS = getattr(config, "NB_JOURS_GENERATION", 2)
    today_local = datetime.now(tz).date()
    dates_fenetre = [today_local + timedelta(days=i) for i in range(NB_JOURS)]

    type_to_heure = {
        "Conseil": "Heure Conseil",
        "Aphorisme": "Heure Aphorisme",
        "Réflexion": "Heure Réflexion",
    }

    rows = []
    for _, row in df_clients.iterrows():
        nom_client = str(row["Client"]).strip()
        programme = str(row["Programme"]).zfill(3)
        saison = int(row["Saison"])
        chat_id = str(row["chat_id"]).strip()
        date_debut = row["Date de Démarrage"]
        jours_autorises = row["jours_set"]

        if not nom_client or not chat_id or pd.isna(date_debut):
            continue

        max_date = max(dates_fenetre)
        compteur = 0
        avancement_par_date = {}
        cur = date_debut
        while cur <= max_date:
            if _weekday_fr(cur) in jours_autorises or len(jours_autorises) == 0:
                compteur += 1
            avancement_par_date[cur] = compteur
            cur += timedelta(days=1)

        for d in dates_fenetre:
            if _weekday_fr(d) not in jours_autorises and len(jours_autorises) != 0:
                continue
            avancement = avancement_par_date.get(d, 0)
            for type_msg, hcol in type_to_heure.items():
                hms = row.get(hcol, "")
                if not hms:
                    continue
                rows.append({
                    "client": nom_client,
                    "programme": programme,
                    "saison": saison,
                    "chat_id": chat_id,
                    "date": d.strftime("%Y-%m-%d"),
                    "heure": hms,
                    "type": type_msg,
                    "avancement": avancement,
                    "message": "",
                    "format": "",
                    "url": "",
                    "envoye": "non",
                })

    df_new = pd.DataFrame(rows)

    # Read existing planning
    cols_planning = ["client","programme","saison","chat_id","date","heure","type","avancement","message","format","url","envoye"]
    recs = _retry(ws_planning.get_all_records)
    if recs:
        df_old = pd.DataFrame(recs)
        for c in cols_planning:
            if c not in df_old.columns:
                df_old[c] = ""
        df_old["programme"] = df_old["programme"].apply(lambda x: str(x).zfill(3))
    else:
        df_old = pd.DataFrame(columns=cols_planning)

    # Purge old rows
    RETENTION = getattr(config, "RETENTION_JOURS", 2)
    cutoff = today_local - timedelta(days=RETENTION)
    def _to_date(x):
        try:
            return pd.to_datetime(x).date()
        except Exception:
            return None
    if not df_old.empty:
        df_old["_date_obj"] = df_old["date"].apply(_to_date)
        df_old = df_old[df_old["_date_obj"].notna()]
        df_old = df_old[df_old["_date_obj"] >= cutoff].drop(columns=["_date_obj"])

    # Merge, dedupe
    df_all = pd.concat([df_old, df_new], ignore_index=True)
    df_all["programme"] = df_all["programme"].apply(lambda x: str(x).zfill(3))
    df_all["saison"] = pd.to_numeric(df_all["saison"], errors="coerce").fillna(1).astype(int)
    df_all["avancement"] = pd.to_numeric(df_all["avancement"], errors="coerce").fillna(1).astype(int)
    df_all["envoye"] = df_all["envoye"].replace({pd.NA: "non", None: "non", "": "non"})
    key_cols = ["client","programme","saison","chat_id","date","heure","type"]
    df_all.drop_duplicates(subset=key_cols, keep="last", inplace=True)

    # Build local datetime for sorting
    def _mk_dt(row):
        s = f"{row['date']} {row['heure']}".strip()
        try:
            return pd.to_datetime(s, errors="coerce")
        except Exception:
            return pd.NaT
    df_all["_dt"] = df_all.apply(_mk_dt, axis=1)
    mask = df_all["_dt"].notna()
    if mask.any():
        df_all.loc[mask, "_dt"] = _localize_safe(df_all.loc[mask, "_dt"].astype("datetime64[ns]"), tz)
    df_all.sort_values(by=["_dt","client","type"], inplace=True, kind="stable")
    df_all.drop(columns=["_dt"], inplace=True)

    # Fill messages from catalogue
    try:
        doc_prog = _retry(gspread.authorize(creds).open, config.FICHIER_PROGRAMMES)
    except Exception:
        doc_prog = None
    cache = {}
    type_mapping = {"Aphorisme": "1-Aphorisme","Conseil":"2-Conseil","Réflexion":"3-Réflexion"}
    msgs, fts, urls = [], [], []
    for _, r in df_all.iterrows():
        if not doc_prog:
            msgs.append(""); fts.append("texte"); urls.append(""); continue
        prog = str(r["programme"]).zfill(3)
        if prog not in cache:
            try:
                ws = _retry(doc_prog.worksheet, prog)
                dfp = pd.DataFrame(_retry(ws.get_all_records))
                for c in ["Saison","Jour","Type","Phrase","Format","Url"]:
                    if c not in dfp.columns: dfp[c] = ""
                dfp["Saison"] = pd.to_numeric(dfp["Saison"], errors="coerce").fillna(1).astype(int)
                dfp["Jour"] = pd.to_numeric(dfp["Jour"], errors="coerce").fillna(1).astype(int)
                cache[prog] = dfp
            except Exception:
                cache[prog] = pd.DataFrame(columns=["Saison","Jour","Type","Phrase","Format","Url"])
        dfp = cache.get(prog, pd.DataFrame())
        saison = int(r["saison"]); jour = int(r["avancement"])
        tlabel = type_mapping.get(str(r["type"]).strip(), None)
        if tlabel is None or dfp.empty:
            msgs.append(""); fts.append("texte"); urls.append(""); continue
        sel = (dfp["Saison"]==saison) & (dfp["Jour"]==jour) & (dfp["Type"]==tlabel)
        match = dfp[sel]
        if not match.empty:
            phrase = str(match.iloc[0].get("Phrase",""))
            fmt = str(match.iloc[0].get("Format","texte")).strip().lower() or "texte"
            url = str(match.iloc[0].get("Url",""))
            msgs.append(f"Saison {saison} - Jour {jour} : \n{r['type']} : {phrase}")
            fts.append(fmt); urls.append(url)
        else:
            msgs.append(""); fts.append("texte"); urls.append("")
    df_all["message"] = msgs; df_all["format"] = fts; df_all["url"] = urls

    # Write back (full, as before) with retry
    for c in df_all.columns:
        df_all[c] = df_all[c].astype(str)
    _retry(ws_planning.clear)
    _retry(ws_planning.update, [df_all.columns.tolist()] + df_all.values.tolist())

    print(f"📅 Mise à jour planning à {datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S %Z')}")

if __name__ == "__main__":
    generer_planning()
