import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
import config
import pytz

def _tz():
    try:
        return pytz.timezone(config.FUSEAU_HORAIRE)
    except Exception:
        return pytz.timezone("Europe/Paris")

def _norm_chat_id(x):
    # Normalize Canal ID / chat_id as a clean integer string
    try:
        # handle floats coming from Sheets
        if isinstance(x, float):
            return str(int(x))
        s = str(x).strip()
        if s.endswith('.0') and s.replace('.','',1).replace('-','',1).isdigit():
            return s[:-2]
        # drop spaces
        return s
    except Exception:
        return str(x)

def _norm_time_hms(val):
    # Return HH:MM:SS or empty string if invalid
    if pd.isna(val) or str(val).strip() == "":
        return ""
    s = str(val).strip().replace("h", ":").replace("H", ":").replace(" ", "")
    # Common cases
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

def _norm_date_str(val):
    # Expect input like 'YYYY-MM-DD' or variants; output 'YYYY-MM-DD'
    try:
        dt = pd.to_datetime(val, format="%Y-%m-%d", errors="coerce")
        if pd.isna(dt):
            return ""
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return ""

def generer_planning():
    # --- Authentification Google Sheets ---
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(config.CHEMIN_CLE_JSON, scopes=scope)
    client_gsheets = gspread.authorize(creds)

    # --- Dictionnaire jours FR ---
    jours_fr = {
        "monday": "lundi", "tuesday": "mardi", "wednesday": "mercredi",
        "thursday": "jeudi", "friday": "vendredi", "saturday": "samedi", "sunday": "dimanche"
    }
    def jour_fr(dt):
        return jours_fr[dt.strftime("%A").lower()]

    # --- Lecture fichier clients ---
    ws_clients = client_gsheets.open(config.FICHIER_CLIENTS).worksheet(config.FEUILLE_CLIENTS)
    df_clients = pd.DataFrame(ws_clients.get_all_records())

    # --- Préparation ---
    df_clients["Date de Démarrage"] = pd.to_datetime(df_clients["Date de Démarrage"], dayfirst=True, errors="coerce")
    df_clients["Jours de Diffusion"] = df_clients["Jours de Diffusion"].apply(
        lambda x: [j.strip().lower() for j in str(x).split(",")])
    df_clients["Programme"] = pd.to_numeric(df_clients["Programme"], errors="coerce").fillna(0).astype(int).apply(lambda x: f"{x:03}")
    df_clients["Canal ID"] = df_clients["Canal ID"].apply(_norm_chat_id)
    df_clients["Saison"] = pd.to_numeric(df_clients["Saison"], errors="coerce").fillna(1).astype(int)

    colonnes_utiles = [
        "Client", "Thème", "Canal ID", "Programme", "Saison",
        "Date de Démarrage", "Jours de Diffusion", "Heure Conseil",
        "Heure Aphorisme", "Heure Réflexion"
    ]
    df_filtree = df_clients[colonnes_utiles].copy()

    # Normaliser les heures en HH:MM:SS pour éviter les doublons à cause du format
    for hcol in ["Heure Conseil", "Heure Aphorisme", "Heure Réflexion"]:
        df_filtree[hcol] = df_filtree[hcol].apply(_norm_time_hms)

    # --- Type vers heure ---
    type_to_heure = {
        "Conseil": "Heure Conseil",
        "Aphorisme": "Heure Aphorisme",
        "Réflexion": "Heure Réflexion"
    }

    NB_JOURS = getattr(config, "NB_JOURS_GENERATION", 2)
    tz = _tz()
    aujourdhui = datetime.now(tz).replace(hour=0, minute=0, second=0, microsecond=0)
    window_start = aujourdhui
    # window includes J and J+... count
    # We'll iterate i in range(NB_JOURS) => includes today and J+1 if 2
    # Build planning
    planning = []
    for _, row in df_filtree.iterrows():
        nom_client = str(row["Client"]).strip()
        programme = str(row["Programme"]).zfill(3)
        saison = int(row["Saison"])
        chat_id = _norm_chat_id(row["Canal ID"])
        date_debut = row["Date de Démarrage"]
        jours_diff = row["Jours de Diffusion"]

        if not nom_client or not chat_id or pd.isna(date_debut):
            continue

        # Localize date_debut at midnight in tz
        if getattr(date_debut, "tzinfo", None) is None:
            date_debut = tz.localize(date_debut)
        else:
            date_debut = date_debut.astimezone(tz)
        date_debut = date_debut.replace(hour=0, minute=0, second=0, microsecond=0)

        # Compute avancement that counts only diffusion days
        # We'll walk from date_debut up to window_end
        for i in range(NB_JOURS):
            date_envoi = window_start + timedelta(days=i)
            if date_envoi < date_debut:
                continue
            # Check day allowed
            if jour_fr(date_envoi) not in jours_diff and len(jours_diff) > 0:
                continue

            # avancement = number of diffusion days from start to date_envoi inclusive
            compteur = 0
            cur = date_debut
            while cur <= date_envoi:
                if jour_fr(cur) in jours_diff or len(jours_diff) == 0:
                    compteur += 1
                cur += timedelta(days=1)

            for type_msg, col_heure in type_to_heure.items():
                heure = row[col_heure]
                if heure:
                    planning.append({
                        "client": nom_client,
                        "programme": programme,
                        "saison": saison,
                        "chat_id": chat_id,
                        "date": date_envoi.strftime("%Y-%m-%d"),
                        "heure": heure,  # already normalized
                        "type": type_msg,
                        "avancement": compteur,
                        "message": "",
                        "format": "",
                        "url": "",
                        "envoye": "non"
                    })

    df_nouveau = pd.DataFrame(planning)

    # --- Lecture planning existant ---
    ws_planning = client_gsheets.open(config.FICHIER_PLANNING).worksheet(config.FEUILLE_PLANNING)
    records = ws_planning.get_all_records()
    colonnes_planning = [
        "client", "programme", "saison", "chat_id", "date", "heure",
        "type", "avancement", "message", "format", "url", "envoye"
    ]
    if records:
        df_existant = pd.DataFrame(records)
        for col in colonnes_planning:
            if col not in df_existant.columns:
                df_existant[col] = ""
        # Normalize keys in existing too
        df_existant["programme"] = df_existant["programme"].apply(lambda x: str(x).zfill(3))
        df_existant["chat_id"] = df_existant["chat_id"].apply(_norm_chat_id)
        df_existant["date"] = df_existant["date"].apply(_norm_date_str)
        df_existant["heure"] = df_existant["heure"].apply(_norm_time_hms)
        df_existant["type"] = df_existant["type"].astype(str).str.strip()
    else:
        df_existant = pd.DataFrame(columns=colonnes_planning)

    # Normalize new keys similarly
    if not df_nouveau.empty:
        df_nouveau["programme"] = df_nouveau["programme"].apply(lambda x: str(x).zfill(3))
        df_nouveau["chat_id"] = df_nouveau["chat_id"].apply(_norm_chat_id)
        df_nouveau["date"] = df_nouveau["date"].apply(_norm_date_str)
        df_nouveau["heure"] = df_nouveau["heure"].apply(_norm_time_hms)
        df_nouveau["type"] = df_nouveau["type"].astype(str).str.strip()

    # --- Purge ancien < (today - RETENTION) ---
    RETENTION = getattr(config, "RETENTION_JOURS", 2)
    cutoff = (datetime.now(_tz()).date() - timedelta(days=RETENTION)).strftime("%Y-%m-%d")
    if not df_existant.empty:
        df_existant = df_existant[df_existant["date"] >= cutoff]

    # --- Fusion sans doublons (clé stricte sans avancement) ---
    subset_keys = ["client", "programme", "saison", "chat_id", "date", "heure", "type"]
    df_merge = pd.concat([df_existant, df_nouveau], ignore_index=True)
    # Ensure string type for subset
    for c in subset_keys:
        df_merge[c] = df_merge[c].astype(str)
    df_merge.drop_duplicates(subset=subset_keys, keep="first", inplace=True)
    df_merge = df_merge.reindex(columns=colonnes_planning)

    # --- Tri pour affichage ---
    # Build tz-aware datetime safely
    dt_naive = pd.to_datetime(df_merge["date"] + " " + df_merge["heure"], errors="coerce", format="%Y-%m-%d %H:%M:%S")
    # Create an empty aware series then fill mask to avoid dtype warning
    tz = _tz()
    aware = pd.Series(pd.NaT, index=df_merge.index, dtype=f"datetime64[ns, {tz.zone}]")
    mask = dt_naive.notna()
    if mask.any():
        aware.loc[mask] = dt_naive.loc[mask].dt.tz_localize(tz, ambiguous="infer", nonexistent="shift_forward")
    df_merge["_dt"] = aware
    df_merge = df_merge.sort_values(by="_dt").drop(columns=["_dt"])

    # --- Préchargement des programmes ---
    programmes_charges = defaultdict(pd.DataFrame)
    for prog_id in df_merge["programme"].unique():
        try:
            ws = client_gsheets.open(config.FICHIER_PROGRAMMES).worksheet(prog_id)
            programmes_charges[prog_id] = pd.DataFrame(ws.get_all_records())
        except Exception:
            programmes_charges[prog_id] = pd.DataFrame()

    # --- Remplissage des messages, format, url ---
    type_mapping = {"Conseil": "2-Conseil", "Aphorisme": "1-Aphorisme", "Réflexion": "3-Réflexion"}
    messages_remplis, formats_remplis, urls_remplis = [], [], []
    for _, row in df_merge.iterrows():
        if pd.notna(row["message"]) and str(row["message"]).strip() != "":
            messages_remplis.append(row["message"])
            formats_remplis.append(str(row.get("format", "texte")).strip().lower() or "texte")
            urls_remplis.append(str(row.get("url", "")))
            continue
        programme = row["programme"]
        saison = int(pd.to_numeric(row["saison"], errors="coerce") or 1)
        jour = int(pd.to_numeric(row["avancement"], errors="coerce") or 1)
        type_excel = type_mapping.get(str(row["type"]).strip(), None)
        df_prog = programmes_charges.get(programme, pd.DataFrame())
        if type_excel and not df_prog.empty:
            # Ensure correct dtypes for filter
            dfp = df_prog.copy()
            dfp["Saison"] = pd.to_numeric(dfp.get("Saison", 1), errors="coerce").fillna(1).astype(int)
            dfp["Jour"] = pd.to_numeric(dfp.get("Jour", 1), errors="coerce").fillna(1).astype(int)
            dfp["Type"] = dfp.get("Type", "").astype(str)
            sel = (dfp["Saison"] == saison) & (dfp["Jour"] == jour) & (dfp["Type"] == type_excel)
            match = dfp[sel]
            if not match.empty:
                phrase = str(match.iloc[0].get("Phrase", ""))
                fmt = str(match.iloc[0].get("Format", "texte")).strip().lower() or "texte"
                url = str(match.iloc[0].get("Url", ""))
                messages_remplis.append(f"Saison {saison} - Jour {jour} : \n{row['type']} : {phrase}")
                formats_remplis.append(fmt)
                urls_remplis.append(url)
                continue
        # default empty
        messages_remplis.append("")
        formats_remplis.append("texte")
        urls_remplis.append("")

    df_merge["message"] = messages_remplis
    df_merge["format"] = formats_remplis
    df_merge["url"] = urls_remplis

    # --- Sauvegarde dans Google Sheet ---
    for col in df_merge.columns:
        df_merge[col] = df_merge[col].astype(str)
    ws_planning.clear()
    ws_planning.update([df_merge.columns.values.tolist()] + df_merge.values.tolist())

    print(f"[DEBUG] Nouveau par date: {df_nouveau.groupby('date').size().to_dict() if not df_nouveau.empty else {}}")
    print(f"[DEBUG] Total par date (après fusion): {df_merge.groupby('date').size().to_dict()}")
    print(f"📅 Mise à jour planning à {datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S %Z')}")

if __name__ == "__main__":
    generer_planning()
