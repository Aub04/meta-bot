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

def _parse_jours_diffusion(x):
    # accepte 'Lundi, Mardi' ou liste déjà splittée
    if isinstance(x, list):
        vals = x
    else:
        vals = str(x).replace(";", ",").split(",")
    return [j.strip().lower() for j in vals if str(j).strip() != ""]

def _normalize_hms(val):
    """Retourne HH:MM:SS ou '' si invalide"""
    if pd.isna(val) or str(val).strip() == "":
        return ""
    s = str(val).strip()
    # Essayes formats communs
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            t = pd.to_datetime(s, format=fmt).time()
            return t.strftime("%H:%M:%S")
        except Exception:
            pass
    # fallback parsing pandas
    try:
        t = pd.to_datetime(s).time()
        return t.strftime("%H:%M:%S")
    except Exception:
        return ""

def _compute_avancement(date_debut_local_date, date_envoi_local_date, jours_diff_lc):
    """Compte uniquement les jours de diffusion entre date_debut (incluse) et date_envoi (incluse)."""
    if not jours_diff_lc:
        # diffusion quotidienne
        return (date_envoi_local_date - date_debut_local_date).days + 1
    jours = ["lundi","mardi","mercredi","jeudi","vendredi","samedi","dimanche"]
    count = 0
    cur = date_debut_local_date
    while cur <= date_envoi_local_date:
        if jours[cur.weekday()] in jours_diff_lc:
            count += 1
        cur += timedelta(days=1)
    return count

def generer_planning():
    tz = _tz()

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
    # Dates: dayfirst=True pour JJ/MM/AAAA
    df_clients["Date de Démarrage"] = pd.to_datetime(df_clients["Date de Démarrage"], errors="coerce", dayfirst=True)
    # Jours de diffusion
    df_clients["Jours de Diffusion"] = df_clients["Jours de Diffusion"].apply(_parse_jours_diffusion)
    # Programme/Saison/Canal
    df_clients["Programme"] = df_clients["Programme"].astype(int).apply(lambda x: f"{x:03}")
    df_clients["Canal ID"] = df_clients["Canal ID"].astype(str)
    df_clients["Saison"] = df_clients["Saison"].astype(int)

    colonnes_utiles = [
        "Client", "Thème", "Canal ID", "Programme", "Saison",
        "Date de Démarrage", "Jours de Diffusion", "Heure Conseil",
        "Heure Aphorisme", "Heure Réflexion"
    ]
    # sécurité si colonnes manquantes
    for c in colonnes_utiles:
        if c not in df_clients.columns:
            df_clients[c] = ""
    df_filtree = df_clients[colonnes_utiles].copy()

    # Normaliser heures -> HH:MM:SS
    for hcol in ["Heure Conseil","Heure Aphorisme","Heure Réflexion"]:
        df_filtree[hcol] = df_filtree[hcol].apply(_normalize_hms)

    # --- Type vers heure (exactement comme chez toi) ---
    type_to_heure = {
        "Conseil": "Heure Conseil",
        "Aphorisme": "Heure Aphorisme",
        "Réflexion": "Heure Réflexion"
    }

    NB_JOURS = getattr(config, "NB_JOURS_GENERATION", 2)
    # today 00:00:00 local
    now_local = datetime.now(tz)
    aujourdhui = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    window_start = aujourdhui

    # --- Génération du planning ---
    planning = []
    skips = {"client_vide":0,"canalid_vide":0,"date_invalide":0,"sans_heure":0}
    for _, row in df_filtree.iterrows():
        nom_client = row["Client"]
        programme = row["Programme"]
        saison = int(row["Saison"])
        chat_id = row["Canal ID"]
        date_debut = row["Date de Démarrage"]
        jours_diff = [j.lower() for j in row["Jours de Diffusion"]] if isinstance(row["Jours de Diffusion"], list) else []

        if not isinstance(nom_client, str) or nom_client.strip() == "":
            skips["client_vide"] += 1
            continue
        if not isinstance(chat_id, str) or chat_id.strip() == "":
            skips["canalid_vide"] += 1
            continue
        if pd.isna(date_debut):
            skips["date_invalide"] += 1
            continue

        # Local date components (ignore tz for comparison at date level)
        date_debut_local_date = date_debut.astimezone(tz).date() if getattr(date_debut, "tzinfo", None) else tz.localize(date_debut).date()

        has_any_hour = any([row["Heure Conseil"], row["Heure Aphorisme"], row["Heure Réflexion"]])
        if not has_any_hour:
            skips["sans_heure"] += 1
            continue

        for i in range(NB_JOURS):
            date_envoi = (window_start + timedelta(days=i)).date()
            # respect des jours de diffusion
            jour_nom = ["lundi","mardi","mercredi","jeudi","vendredi","samedi","dimanche"][ (window_start + timedelta(days=i)).weekday() ]
            if jours_diff and jour_nom not in jours_diff:
                continue
            if date_envoi < date_debut_local_date:
                continue
            avancement = _compute_avancement(date_debut_local_date, date_envoi, jours_diff)

            for type_msg, col_heure in type_to_heure.items():
                heure = row[col_heure]
                if not heure:
                    continue
                planning.append({
                    "client": nom_client,
                    "programme": programme,
                    "saison": saison,
                    "chat_id": chat_id,
                    "date": date_envoi.strftime("%Y-%m-%d"),
                    "heure": heure,
                    "type": type_msg,
                    "avancement": avancement,
                    "message": "",
                    "format": "",
                    "url": "",
                    "envoye": "non"
                })

    df_nouveau = pd.DataFrame(planning)
    print(f"[DEBUG] today={aujourdhui.date()} NB_JOURS={NB_JOURS} generated_rows={len(df_nouveau)}")
    print(f"[DEBUG] skips={skips}")

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
        df_existant["programme"] = df_existant["programme"].apply(lambda x: str(x).zfill(3))
    else:
        df_existant = pd.DataFrame(columns=colonnes_planning)

    # --- PURGE anciennes lignes (garder J-RETENTION) ---
    RET = getattr(config, "RETENTION_JOURS", 2)
    cutoff = aujourdhui.date() - timedelta(days=RET)
    if not df_existant.empty:
        df_existant["_date_obj"] = pd.to_datetime(df_existant["date"], errors="coerce").dt.date
        df_existant = df_existant[df_existant["_date_obj"].notna()]
        df_existant = df_existant[df_existant["_date_obj"] >= cutoff].drop(columns=["_date_obj"])

    # --- Fusion sans doublons (clé SANS avancement !) ---
    df_merge = pd.concat([df_existant, df_nouveau], ignore_index=True)
    df_merge["programme"] = df_merge["programme"].apply(lambda x: str(x).zfill(3))
    df_merge["saison"] = pd.to_numeric(df_merge["saison"], errors="coerce").fillna(1).astype(int)
    df_merge["avancement"] = pd.to_numeric(df_merge["avancement"], errors="coerce").fillna(1).astype(int)

    subset_keys = ["client", "programme", "saison", "chat_id", "date", "heure", "type"]
    df_merge.drop_duplicates(subset=subset_keys, keep="first", inplace=True)
    df_merge = df_merge.reindex(columns=colonnes_planning)

    # --- Tri chronologique robuste ---
    # Combine date + heure -> naive
    dt_naive = pd.to_datetime(df_merge["date"].astype(str) + " " + df_merge["heure"].astype(str), errors="coerce")
    # Localize safely
    mask = dt_naive.notna()
    df_merge["_dt"] = pd.NaT
    if mask.any():
        df_merge.loc[mask, "_dt"] = dt_naive[mask].dt.tz_localize(tz, ambiguous="infer", nonexistent="shift_forward")
    df_merge.sort_values(by=["_dt","client","type"], inplace=True, kind="stable")
    df_merge.drop(columns=["_dt"], inplace=True)

    # --- Préchargement des programmes ---
    programmes_charges = defaultdict(pd.DataFrame)
    for prog_id in df_merge["programme"].dropna().unique():
        try:
            ws = client_gsheets.open(config.FICHIER_PROGRAMMES).worksheet(str(prog_id))
            dfp = pd.DataFrame(ws.get_all_records())
            # types/dtypes
            if not dfp.empty:
                dfp["Saison"] = pd.to_numeric(dfp.get("Saison", 1), errors="coerce").fillna(1).astype(int)
                dfp["Jour"] = pd.to_numeric(dfp.get("Jour", 1), errors="coerce").fillna(1).astype(int)
                for c in ["Support","Type","Phrase","Format","Url"]:
                    if c not in dfp.columns: dfp[c] = ""
            programmes_charges[str(prog_id)] = dfp
        except Exception:
            programmes_charges[str(prog_id)] = pd.DataFrame(columns=["Support","Saison","Jour","Type","Phrase","Format","Url"])

    # --- Remplissage des messages, format, url ---
    type_mapping = {"Conseil": "2-Conseil", "Aphorisme": "1-Aphorisme", "Réflexion": "3-Réflexion"}
    messages_remplis, formats_remplis, urls_remplis = [], [], []
    for _, row in df_merge.iterrows():
        # si déjà rempli, conserver
        if isinstance(row.get("message", ""), str) and row["message"].strip() != "":
            messages_remplis.append(row["message"])
            formats_remplis.append(str(row.get("format", "texte")).strip().lower() or "texte")
            urls_remplis.append(str(row.get("url", "")))
            continue
        programme = str(row["programme"]).zfill(3)
        saison = int(row["saison"])
        jour = int(row["avancement"])
        type_excel = type_mapping.get(str(row["type"]), None)
        df_prog = programmes_charges.get(programme, pd.DataFrame())

        if type_excel and not df_prog.empty:
            filtre = ((df_prog["Saison"] == saison) & (df_prog["Jour"] == jour) & (df_prog["Type"] == type_excel))
            ligne = df_prog[filtre]
            if not ligne.empty:
                phrase = str(ligne.iloc[0].get("Phrase", ""))
                format_msg = str(ligne.iloc[0].get("Format", "texte")).strip().lower() or "texte"
                url_msg = str(ligne.iloc[0].get("Url", ""))
                messages_remplis.append(f"Saison {saison} - Jour {jour} : \n{row['type']} : {phrase}")
                formats_remplis.append(format_msg)
                urls_remplis.append(url_msg)
                continue
        # default si pas trouvé
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

    # Stats
    by_date_new = df_nouveau.groupby("date")["client"].count().to_dict() if not df_nouveau.empty else {}
    print(f"[DEBUG] Nouveau par date: {by_date_new}")
    by_date_total = pd.Series(df_merge["date"]).value_counts().sort_index().to_dict()
    print(f"[DEBUG] Total par date (après fusion): {by_date_total}")
    print(f"📅 Mise à jour planning à {datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S %Z')}")

if __name__ == "__main__":
    generer_planning()
