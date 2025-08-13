import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from datetime import datetime, timedelta
import pytz
import random
import config

# === Helpers ===
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

def generer_planning():
    tz = _tz()

    # Auth
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(config.CHEMIN_CLE_JSON, scopes=scope)
    client_gsheets = gspread.authorize(creds)

    ws_clients = client_gsheets.open(config.FICHIER_CLIENTS).worksheet(config.FEUILLE_CLIENTS)
    ws_planning = client_gsheets.open(config.FICHIER_PLANNING).worksheet(config.FEUILLE_PLANNING)

    df_clients = pd.DataFrame(ws_clients.get_all_records())
    if df_clients.empty:
        print("Aucun client.")
        return

    cols_req = [
        "Client", "Programme", "Saison", "chat_id",
        "Date de Démarrage", "Jours de Diffusion",
        "Heure Conseil", "Heure Aphorisme", "Heure Réflexion"
    ]
    for c in cols_req:
        if c not in df_clients.columns:
            df_clients[c] = ""

    df_clients["Programme"] = df_clients["Programme"].apply(lambda x: str(x).zfill(3))
    df_clients["Saison"] = pd.to_numeric(df_clients["Saison"], errors="coerce").fillna(1).astype(int)

    # Parse FR dates safely
    df_clients["Date de Démarrage"] = pd.to_datetime(df_clients["Date de Démarrage"], errors="coerce", dayfirst=True).dt.date

    for hcol in ["Heure Conseil", "Heure Aphorisme", "Heure Réflexion"]:
        df_clients[hcol] = df_clients[hcol].apply(_to_time_hms)

    df_clients["jours_set"] = df_clients["Jours de Diffusion"].apply(_parse_jours_diffusion)

    NB_JOURS = getattr(config, "NB_JOURS_GENERATION", 2)
    today_local = datetime.now(tz).date()
    dates_fenetre = [today_local + timedelta(days=i) for i in range(NB_JOURS)]

    print(f"[DEBUG] today_local={today_local} NB_JOURS={NB_JOURS} dates_fenetre={dates_fenetre}")

    type_to_heure = {
        "Conseil": "Heure Conseil",
        "Aphorisme": "Heure Aphorisme",
        "Réflexion": "Heure Réflexion",
    }

    planning_rows = []
    debug_clients = []
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

        # Debug sample
        debug_clients.append((nom_client, sorted(list(jours_autorises))))

        for d in dates_fenetre:
            if _weekday_fr(d) not in jours_autorises and len(jours_autorises) != 0:
                continue
            avancement = avancement_par_date.get(d, 0)
            for type_msg, hcol in type_to_heure.items():
                heure_hms = row.get(hcol, "")
                if not heure_hms:
                    continue
                planning_rows.append({
                    "client": nom_client,
                    "programme": programme,
                    "saison": saison,
                    "chat_id": chat_id,
                    "date": d.strftime("%Y-%m-%d"),
                    "heure": heure_hms,
                    "type": type_msg,
                    "avancement": avancement,
                    "message": "",
                    "format": "",
                    "url": "",
                    "envoye": "non",
                })

    df_nouveau = pd.DataFrame(planning_rows)
    # Debug counts nouveau
    if not df_nouveau.empty:
        counts_new = df_nouveau["date"].value_counts().sort_index().to_dict()
        print(f"[DEBUG] Nouveau par date: {counts_new}")
    else:
        print("[DEBUG] df_nouveau est vide")

    # Lire planning existant
    colonnes_planning = [
        "client", "programme", "saison", "chat_id", "date", "heure",
        "type", "avancement", "message", "format", "url", "envoye",
    ]
    records = ws_planning.get_all_records()
    if records:
        df_existant = pd.DataFrame(records)
        for col in colonnes_planning:
            if col not in df_existant.columns:
                df_existant[col] = ""
        df_existant["programme"] = df_existant["programme"].apply(lambda x: str(x).zfill(3))
    else:
        df_existant = pd.DataFrame(columns=colonnes_planning)

    # Purge
    RETENTION = getattr(config, "RETENTION_JOURS", 2)
    cutoff_date = today_local - timedelta(days=RETENTION)

    def _to_date(x):
        try:
            return pd.to_datetime(x).date()
        except Exception:
            return None

    if not df_existant.empty:
        df_existant["_date_obj"] = df_existant["date"].apply(_to_date)
        df_existant = df_existant[df_existant["_date_obj"].notna()]
        df_existant = df_existant[df_existant["_date_obj"] >= cutoff_date].drop(columns=["_date_obj"])

    df_all = pd.concat([df_existant, df_nouveau], ignore_index=True)

    # Build dt safely
    df_all["_dt_str"] = (df_all["date"].astype(str) + " " + df_all["heure"].astype(str)).str.strip()
    df_all["_dt_naive"] = pd.to_datetime(df_all["_dt_str"], errors="coerce")
    mask = df_all["_dt_naive"].notna()
    df_all["_dt"] = pd.NaT
    if mask.any():
        localized = _localize_safe(df_all.loc[mask, "_dt_naive"].astype("datetime64[ns]"), tz)
        df_all.loc[mask, "_dt"] = localized

    df_all.sort_values(by=["_dt","client","type"], inplace=True, kind="stable")
    df_all.drop(columns=["_dt_str","_dt_naive"], inplace=True)

    # Debug counts after merge
    if not df_all.empty:
        counts_all = df_all["date"].value_counts().sort_index().to_dict()
        print(f"[DEBUG] Total par date (après fusion): {counts_all}")
    # Debug sample clients
    if debug_clients:
        # print up to 3 clients jours_set
        sample = debug_clients[:3]
        print(f"[DEBUG] échantillon jours_set clients: {sample}")

    # Remplissage messages (idem version clean)
    try:
        doc_prog = client_gsheets.open(config.FICHIER_PROGRAMMES)
    except Exception:
        doc_prog = None
    programmes_cache = {}

    type_mapping = {
        "Aphorisme": "1-Aphorisme",
        "Conseil": "2-Conseil",
        "Réflexion": "3-Réflexion",
    }

    messages, formats, urls = [], [], []
    for _, r in df_all.iterrows():
        if not doc_prog:
            messages.append(""); formats.append("texte"); urls.append(""); continue
        prog = str(r["programme"]).zfill(3)
        if prog not in programmes_cache:
            try:
                ws = doc_prog.worksheet(prog)
                dfp = pd.DataFrame(ws.get_all_records())
                for c in ["Saison","Jour","Type","Phrase","Format","Url"]:
                    if c not in dfp.columns:
                        dfp[c] = ""
                dfp["Saison"] = pd.to_numeric(dfp["Saison"], errors="coerce").fillna(1).astype(int)
                dfp["Jour"] = pd.to_numeric(dfp["Jour"], errors="coerce").fillna(1).astype(int)
                programmes_cache[prog] = dfp
            except Exception:
                programmes_cache[prog] = pd.DataFrame(columns=["Saison","Jour","Type","Phrase","Format","Url"])
        dfp = programmes_cache.get(prog, pd.DataFrame())
        saison = int(r["saison"]) if pd.notna(r["saison"]) else 1
        jour = int(r["avancement"]) if pd.notna(r["avancement"]) else 1
        type_excel = type_mapping.get(str(r["type"]).strip(), None)
        if type_excel is None or dfp.empty:
            messages.append(""); formats.append("texte"); urls.append(""); continue
        sel = (dfp["Saison"] == saison) & (dfp["Jour"] == jour) & (dfp["Type"] == type_excel)
        match = dfp[sel]
        if not match.empty:
            phrase = str(match.iloc[0].get("Phrase", ""))
            fmt = str(match.iloc[0].get("Format", "texte")).strip().lower() or "texte"
            url = str(match.iloc[0].get("Url", ""))
            messages.append(f"Saison {saison} - Jour {jour} : \n{r['type']} : {phrase}")
            formats.append(fmt)
            urls.append(url)
        else:
            messages.append("")
            formats.append("texte")
            urls.append("")

    df_all["message"] = messages
    df_all["format"] = formats
    df_all["url"] = urls

    # Write back
    for c in df_all.columns:
        df_all[c] = df_all[c].astype(str)

    ws_planning.clear()
    ws_planning.update([df_all.columns.tolist()] + df_all.values.tolist())

    print(f"📅 Mise à jour planning à {datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S %Z')}")

if __name__ == "__main__":
    generer_planning()
