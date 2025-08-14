import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
import pytz
import config

# =========================
# Helpers
# =========================

def _tz():
    try:
        return pytz.timezone(config.FUSEAU_HORAIRE)
    except Exception:
        return pytz.timezone("Europe/Paris")

def _norm_hms(x):
    """ '10:00' / '10:00:00' / 0.4166667 (Excel float) -> 'HH:MM:SS' """
    if pd.isna(x):
        return ""
    s = str(x).strip()
    # Excel time float (fraction of day)
    try:
        if s.replace(".", "", 1).isdigit() and ":" not in s:
            val = float(s)
            total = int(round(val * 24 * 3600))
            h, m, sec = total // 3600, (total % 3600) // 60, total % 60
            return f"{h:02d}:{m:02d}:{sec:02d}"
    except Exception:
        pass
    # Anything parsable by pandas
    t = pd.to_datetime(s, errors="coerce").time()
    if t is not None:
        return f"{t.hour:02d}:{t.minute:02d}:{t.second:02d}"
    return ""

def _norm_date(s):
    dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
    return "" if pd.isna(dt) else dt.strftime("%Y-%m-%d")

def _norm_chat(s):
    s = str(s).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s

def _weekday_fr(dt_date):
    jours = ["lundi","mardi","mercredi","jeudi","vendredi","samedi","dimanche"]
    return jours[dt_date.weekday()]

def _parse_jours_diffusion(s):
    # Accept either csv string like "Lundi, Mardi, ..." or list-like
    if isinstance(s, (list, tuple)):
        parts = [str(x).strip().lower() for x in s]
    else:
        raw = str(s).replace(";", ",")
        parts = [p.strip().lower() for p in raw.split(",") if p.strip()]
    # Normalize english names if present
    mapping = {"monday":"lundi","tuesday":"mardi","wednesday":"mercredi","thursday":"jeudi",
               "friday":"vendredi","saturday":"samedi","sunday":"dimanche"}
    out = []
    for p in parts:
        out.append(mapping.get(p, p))
    return set(out)

def _normalize_key_columns(df):
    # Normalize only columns forming the de-dup key
    df["client"]    = df["client"].astype(str).str.strip()
    df["programme"] = df["programme"].astype(str).str.zfill(3)
    df["saison"]    = df["saison"].astype(str).str.strip()
    df["chat_id"]   = df["chat_id"].apply(_norm_chat)
    df["date"]      = df["date"].apply(_norm_date)
    df["heure"]     = df["heure"].apply(_norm_hms)
    df["type"]      = df["type"].astype(str).str.strip()

# =========================
# Main
# =========================

def generer_planning():
    tz = _tz()
    NB_JOURS = getattr(config, "NB_JOURS_GENERATION", 2)
    RETENTION = getattr(config, "RETENTION_JOURS", 2)

    # --- Auth ---
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(config.CHEMIN_CLE_JSON, scopes=scope)
    client = gspread.authorize(creds)

    # === Open sheets ===
    ws_clients = client.open(config.FICHIER_CLIENTS).worksheet(config.FEUILLE_CLIENTS)
    ws_planning = client.open(config.FICHIER_PLANNING).worksheet(config.FEUILLE_PLANNING)
    doc_programmes = client.open(config.FICHIER_PROGRAMMES)

    # === Read Clients ===
    df_clients = pd.DataFrame(ws_clients.get_all_records())
    if df_clients.empty:
        print("Aucun client dans la feuille Clients.")
        return

    # Ensure required columns exist
    required_clients = [
        "Client","Thème","Canal ID","Programme","Saison","Date de Démarrage",
        "Jours de Diffusion","Heure Conseil","Heure Aphorisme","Heure Réflexion"
    ]
    for c in required_clients:
        if c not in df_clients.columns:
            df_clients[c] = ""

    # Add "Date de Fin" column if missing
    if "Date de Fin" not in df_clients.columns:
        df_clients["Date de Fin"] = ""

    # Normalize core fields
    df_clients["Programme"] = df_clients["Programme"].apply(lambda x: f"{int(pd.to_numeric(x, errors='coerce')):03}" if str(x).strip() != "" else "")
    df_clients["Saison"] = pd.to_numeric(df_clients["Saison"], errors="coerce").fillna(1).astype(int)
    df_clients["Canal ID"] = df_clients["Canal ID"].apply(_norm_chat)
    df_clients["Date de Démarrage"] = pd.to_datetime(df_clients["Date de Démarrage"], dayfirst=True, errors="coerce")
    df_clients["Jours de Diffusion"] = df_clients["Jours de Diffusion"].apply(_parse_jours_diffusion)
    for hcol in ["Heure Conseil","Heure Aphorisme","Heure Réflexion"]:
        df_clients[hcol] = df_clients[hcol].apply(_norm_hms)

    # === Build window ===
    today_local = datetime.now(tz).date()
    dates_fenetre = [today_local + timedelta(days=i) for i in range(NB_JOURS)]
    print(f"[DEBUG] today={today_local} NB_JOURS={NB_JOURS} dates={dates_fenetre}")

    # === Preload Types mapping (id <-> label) ===
    types_map_id_to_label = {}
    types_map_label_to_id = {}
    try:
        ws_types = doc_programmes.worksheet("Types")
        df_types = pd.DataFrame(ws_types.get_all_records())
        if not df_types.empty:
            # expected columns: Id, Type
            for _, r in df_types.iterrows():
                tid = int(pd.to_numeric(r.get("Id", ""), errors="coerce"))
                lbl = str(r.get("Type", "")).strip()
                if lbl:
                    types_map_id_to_label[tid] = lbl
                    types_map_label_to_id[lbl.lower()] = tid
    except Exception:
        pass  # If no Types sheet, mappings stay empty

    # === Generate planning rows ===
    type_to_heure = {
        "Conseil": "Heure Conseil",
        "Aphorisme": "Heure Aphorisme",
        "Réflexion": "Heure Réflexion",
    }

    planning_rows = []
    skips = {"client_vide":0,"canalid_vide":0,"date_invalide":0,"sans_heure":0}
    for _, row in df_clients.iterrows():
        nom_client = str(row["Client"]).strip()
        programme = str(row["Programme"]).strip()
        saison = int(row["Saison"])
        chat_id = str(row["Canal ID"]).strip()
        date_debut = row["Date de Démarrage"]
        jours_autorises = row["Jours de Diffusion"]

        if not nom_client:
            skips["client_vide"] += 1; continue
        if not chat_id:
            skips["canalid_vide"] += 1; continue
        if pd.isna(date_debut):
            skips["date_invalide"] += 1; continue

        # Compute avancement only counting authorized days
        max_d = max(dates_fenetre)
        compteur = 0
        avancement_par_date = {}
        cur = date_debut.date()
        while cur <= max_d:
            if (len(jours_autorises)==0) or (_weekday_fr(cur) in jours_autorises):
                compteur += 1
            avancement_par_date[cur] = compteur
            cur += timedelta(days=1)

        # Generate rows for each day in window
        for d in dates_fenetre:
            if (len(jours_autorises)!=0) and (_weekday_fr(d) not in jours_autorises):
                continue
            avancement = int(avancement_par_date.get(d, 0))
            added_any = False
            for type_msg, col_heure in type_to_heure.items():
                heure_hms = row.get(col_heure, "")
                if not heure_hms:
                    continue
                planning_rows.append({
                    "client": nom_client,
                    "programme": programme,
                    "saison": saison,
                    "chat_id": chat_id,
                    "date": d.strftime("%Y-%m-%d"),
                    "heure": heure_hms,
                    "type": type_msg,  # textual type remains in planning
                    "avancement": avancement,
                    "message": "",
                    "format": "",
                    "url": "",
                    "envoye": "non",
                })
                added_any = True
            if not added_any:
                skips["sans_heure"] += 1

    df_nouveau = pd.DataFrame(planning_rows)
    if df_nouveau.empty:
        print(f"[DEBUG] df_nouveau est vide ; skips={skips}")
    else:
        print(f"[DEBUG] Nouveau par date: {df_nouveau['date'].value_counts().to_dict()}")
        print(f"[DEBUG] skips={skips}")

    # === Read existing planning ===
    records = ws_planning.get_all_records()
    columns_planning = ["client","programme","saison","chat_id","date","heure","type","avancement","message","format","url","envoye"]
    if records:
        df_existant = pd.DataFrame(records)
        for c in columns_planning:
            if c not in df_existant.columns:
                df_existant[c] = ""
    else:
        df_existant = pd.DataFrame(columns=columns_planning)

    # === Normalize keys BEFORE merge (avoid dupes) ===
    if not df_nouveau.empty:
        _normalize_key_columns(df_nouveau)
    if not df_existant.empty:
        _normalize_key_columns(df_existant)

    # === Purge old rows from existing (keep >= today-RETENTION) ===
    if not df_existant.empty:
        df_existant["_date_obj"] = pd.to_datetime(df_existant["date"], format="%Y-%m-%d", errors="coerce").dt.date
        cutoff = today_local - timedelta(days=RETENTION)
        df_existant = df_existant[df_existant["_date_obj"].notna() & (df_existant["_date_obj"] >= cutoff)].drop(columns=["_date_obj"])

    # === Merge & drop duplicates by strict key (without avancement) ===
    key_cols = ["client","programme","saison","chat_id","date","heure","type"]
    df_merge = pd.concat([df_existant, df_nouveau], ignore_index=True)
    df_merge.drop_duplicates(subset=key_cols, keep="first", inplace=True)
    df_merge = df_merge.reindex(columns=columns_planning)

    # === Fill messages/format/url using programme tabs ===
    # Preload programmes as needed
    programmes_cache = {}
    messages, formats, urls = [], [], []
    for _, r in df_merge.iterrows():
        if r.get("message", "").strip():
            messages.append(r["message"]); formats.append(r.get("format","texte")); urls.append(r.get("url","")); continue
        prog = str(r["programme"]).zfill(3)
        if prog not in programmes_cache:
            try:
                ws_prog = doc_programmes.worksheet(prog)
                dfp = pd.DataFrame(ws_prog.get_all_records())
                for c in ["Support","Saison","Jour","Type","Phrase","Format","Url"]:
                    if c not in dfp.columns:
                        dfp[c] = ""
                # ensure numeric types for Saison, Jour, Type
                dfp["Saison"] = pd.to_numeric(dfp["Saison"], errors="coerce").fillna(1).astype(int)
                dfp["Jour"] = pd.to_numeric(dfp["Jour"], errors="coerce").fillna(1).astype(int)
                dfp["Type"] = pd.to_numeric(dfp["Type"], errors="coerce").astype("Int64")
                programmes_cache[prog] = dfp
            except Exception:
                programmes_cache[prog] = pd.DataFrame(columns=["Support","Saison","Jour","Type","Phrase","Format","Url"])
        dfp = programmes_cache.get(prog, pd.DataFrame())
        saison = int(pd.to_numeric(r["saison"], errors="coerce") or 1)
        jour = int(pd.to_numeric(r["avancement"], errors="coerce") or 1)

        # map planning textual type -> type_id using Types sheet if available
        label = str(r["type"]).strip()
        if types_map_label_to_id:
            type_id = types_map_label_to_id.get(label.lower(), None)
        else:
            legacy = {"aphorisme":1, "conseil":2, "réflexion":3, "reflexion":3}
            type_id = legacy.get(label.lower(), None)

        if type_id is None or dfp.empty:
            messages.append(""); formats.append("texte"); urls.append(""); continue
        match = dfp[(dfp["Saison"]==saison) & (dfp["Jour"]==jour) & (dfp["Type"]==type_id)]
        if not match.empty:
            phrase = str(match.iloc[0].get("Phrase",""))
            fmt = str(match.iloc[0].get("Format","texte")).strip().lower() or "texte"
            url = str(match.iloc[0].get("Url",""))
            messages.append(f"Saison {saison} - Jour {jour} : \n{label} : {phrase}")
            formats.append(fmt); urls.append(url)
        else:
            messages.append(""); formats.append("texte"); urls.append("")

    df_merge["message"] = messages
    df_merge["format"] = formats
    df_merge["url"] = urls

    # === Sort chronologically (TZ-safe) ===
    dt_naive = pd.to_datetime(df_merge["date"].astype(str) + " " + df_merge["heure"].astype(str),
                              format="%Y-%m-%d %H:%M:%S", errors="coerce")
    mask = dt_naive.notna()
    df_sorted = df_merge.copy()
    df_sorted["_dt"] = pd.NaT
    df_sorted.loc[mask, "_dt"] = dt_naive[mask].dt.tz_localize(tz, ambiguous="infer", nonexistent="shift_forward")
    df_sorted = df_sorted.sort_values("_dt").drop(columns=["_dt"])

    # === Write planning (full rewrite) ===
    for c in df_sorted.columns:
        df_sorted[c] = df_sorted[c].astype(str)
    ws_planning.clear()
    ws_planning.update([df_sorted.columns.tolist()] + df_sorted.values.tolist())
    print(f"[DEBUG] Total par date (après fusion): {df_sorted['date'].value_counts().to_dict()}")
    print(f"📅 Mise à jour planning à {datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S %Z')}")

    # === Update Date de Fin in Clients (only when empty) ===
    # Build map: (prog,saison) -> nb_jours (max Jour)
    nb_jours_cache = {}
    def get_nb_jours(prog, saison):
        key = (prog, saison)
        if key in nb_jours_cache:
            return nb_jours_cache[key]
        dfp = programmes_cache.get(prog)
        if dfp is None:
            try:
                ws_prog = doc_programmes.worksheet(prog)
                dfp = pd.DataFrame(ws_prog.get_all_records())
                if dfp.empty:
                    nb_jours_cache[key] = None; return None
                dfp["Saison"] = pd.to_numeric(dfp["Saison"], errors="coerce").fillna(1).astype(int)
                dfp["Jour"] = pd.to_numeric(dfp["Jour"], errors="coerce").fillna(1).astype(int)
            except Exception:
                nb_jours_cache[key] = None; return None
        sel = dfp[dfp["Saison"]==saison]
        if sel.empty:
            nb_jours_cache[key] = None; return None
        nb = int(sel["Jour"].max())
        nb_jours_cache[key] = nb
        return nb

    # Determine column index for "Date de Fin"
    header = ws_clients.row_values(1)
    if "Date de Fin" not in header:
        # add column at end
        header.append("Date de Fin")
        all_vals = ws_clients.get_all_values()
        if all_vals:
            ws_clients.update("A1", [header] + all_vals[1:])
        else:
            ws_clients.update("A1", [header])
        header = ws_clients.row_values(1)
    date_fin_col_idx = header.index("Date de Fin") + 1  # 1-based

    # Build updates for rows needing a Date de Fin
    updates = []
    for i, (_, r) in enumerate(df_clients.iterrows(), start=2):  # sheet rows start at 2
        current_fin = str(r.get("Date de Fin","")).strip()
        if current_fin:
            continue
        prog = str(r["Programme"]).zfill(3)
        saison = int(r["Saison"])
        start = r["Date de Démarrage"]
        jours_aut = r["Jours de Diffusion"]
        if pd.isna(start):
            continue
        nb_jours = get_nb_jours(prog, saison)
        if not nb_jours or nb_jours <= 0:
            continue
        # simulate days
        count = 0
        cur = start.date()
        last = cur
        while count < nb_jours:
            if (len(jours_aut)==0) or (_weekday_fr(cur) in jours_aut):
                count += 1
                last = cur
            cur += timedelta(days=1)
        updates.append((i, last.strftime("%Y-%m-%d")))

    if updates:
        data_body = {
            "valueInputOption": "RAW",
            "data": [{"range": f"{config.FEUILLE_CLIENTS}!{gspread.utils.rowcol_to_a1(r, date_fin_col_idx)}",
                      "values": [[val]]} for (r, val) in updates]
        }
        ws_clients.spreadsheet.values_batch_update(data_body)
        print(f"📝 Dates de fin mises à jour pour {len(updates)} client(s).")
    else:
        print("📝 Aucune date de fin à compléter.")

if __name__ == "__main__":
    generer_planning()
