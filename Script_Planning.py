import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
import pytz
import config

# ========= Helpers =========

def _tz():
    try:
        return pytz.timezone(config.FUSEAU_HORAIRE)
    except Exception:
        return pytz.timezone("Europe/Paris")

def _norm_hms(x):
    """Normalize time into HH:MM:SS; handle NaT safely."""
    if x is None:
        return ""
    s = str(x).strip()
    if s == "" or s.lower() in ("nat", "nan"):
        return ""
    # Excel float time?
    try:
        if s.replace(".", "", 1).isdigit() and (":" not in s):
            val = float(s)
            total = int(round(val * 24 * 3600))
            h, m, sec = total // 3600, (total % 3600) // 60, total % 60
            return f"{h:02d}:{m:02d}:{sec:02d}"
    except Exception:
        pass
    # Parse common text times
    dt = pd.to_datetime(s, errors="coerce")
    if pd.isna(dt):
        return ""
    try:
        t = dt.time()
        return f"{t.hour:02d}:{t.minute:02d}:{t.second:02d}"
    except Exception:
        return ""

def _norm_date(s):
    # Accept a variety of inputs; output YYYY-MM-DD
    dt = pd.to_datetime(s, errors="coerce", format="%Y-%m-%d")
    return "" if pd.isna(dt) else dt.strftime("%Y-%m-%d")

def _norm_chat(s):
    s = str(s).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s

def _weekday_fr(d):
    return ["lundi","mardi","mercredi","jeudi","vendredi","samedi","dimanche"][d.weekday()]

def _parse_jours_diffusion(v):
    if isinstance(v, (list, tuple)):
        parts = [str(x).strip().lower() for x in v]
    else:
        parts = [p.strip().lower() for p in str(v).replace(";",",").split(",") if p.strip()]
    mapping = {"monday":"lundi","tuesday":"mardi","wednesday":"mercredi","thursday":"jeudi",
               "friday":"vendredi","saturday":"samedi","sunday":"dimanche"}
    return set(mapping.get(p,p) for p in parts)

def _normalize_key_columns(df):
    df["client"]    = df["client"].astype(str).str.strip()
    df["programme"] = df["programme"].astype(str).str.zfill(3)
    df["saison"]    = df["saison"].astype(str).str.strip()
    df["chat_id"]   = df["chat_id"].apply(_norm_chat)
    df["date"]      = df["date"].apply(_norm_date)
    df["heure"]     = df["heure"].apply(_norm_hms)
    # type left empty at generation; filled later

# ========= Main =========

def generer_planning():
    tz = _tz()
    NB_JOURS = getattr(config, "NB_JOURS_GENERATION", 2)
    RETENTION = getattr(config, "RETENTION_JOURS", 2)
    DEFAULT_SLOT_TYPE_IDS = getattr(config, "DEFAULT_SLOT_TYPE_IDS", [1,2,3])

    # Auth
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(config.CHEMIN_CLE_JSON, scopes=scope)
    client = gspread.authorize(creds)

    ws_clients = client.open(config.FICHIER_CLIENTS).worksheet(config.FEUILLE_CLIENTS)
    ws_planning = client.open(config.FICHIER_PLANNING).worksheet(config.FEUILLE_PLANNING)
    doc_programmes = client.open(config.FICHIER_PROGRAMMES)

    # Read Clients
    dfc = pd.DataFrame(ws_clients.get_all_records())
    required = ["Client","Thème","Canal ID","Programme","Saison","Date de Démarrage",
                "Jours de Diffusion","Heure envoi 1","Heure envoi 2","Heure envoi 3"]
    for c in required:
        if c not in dfc.columns:
            dfc[c] = ""
    # keep backwards compatibility with old hour columns if new are empty
    fallback = {1:"Heure Aphorisme", 2:"Heure Conseil", 3:"Heure Réflexion"}
    for k,col in fallback.items():
        if f"Heure envoi {k}" not in dfc.columns and col in dfc.columns:
            dfc[f"Heure envoi {k}"] = dfc[col]
        elif dfc[f"Heure envoi {k}"].replace("", pd.NA).isna().all() and col in dfc.columns:
            dfc[f"Heure envoi {k}"] = dfc[col]

    # Types per slot (optional in Clients)
    for k in (1,2,3):
        cname = f"Type envoi {k}"
        if cname not in dfc.columns:
            dfc[cname] = ""

    # Normalize
    dfc["Programme"] = dfc["Programme"].apply(lambda x: f"{int(pd.to_numeric(x, errors='coerce')):03}" if str(x).strip()!="" else "")
    dfc["Saison"] = pd.to_numeric(dfc["Saison"], errors="coerce").fillna(1).astype(int)
    dfc["Canal ID"] = dfc["Canal ID"].apply(_norm_chat)
    dfc["Date de Démarrage"] = pd.to_datetime(dfc["Date de Démarrage"], errors="coerce", dayfirst=True)
    dfc["Jours de Diffusion"] = dfc["Jours de Diffusion"].apply(_parse_jours_diffusion)
    for k in (1,2,3):
        dfc[f"Heure envoi {k}"] = dfc[f"Heure envoi {k}"].apply(_norm_hms)

    # Date window
    today = datetime.now(tz).date()
    dates_fenetre = [today + timedelta(days=i) for i in range(NB_JOURS)]
    print(f"[DEBUG] today={today} NB_JOURS={NB_JOURS} dates={dates_fenetre}")

    # Read Types mapping from 'Types'
    types_id_to_label, types_label_to_id = {}, {}
    try:
        ws_types = doc_programmes.worksheet("Types")
        dft = pd.DataFrame(ws_types.get_all_records())
        for _,r in dft.iterrows():
            try:
                tid = int(pd.to_numeric(r.get("Id",""), errors="coerce"))
            except Exception:
                continue
            lbl = str(r.get("Type","")).strip()
            if lbl:
                types_id_to_label[tid] = lbl
                types_label_to_id[lbl.lower()] = tid
    except Exception:
        pass

    # Generate planning rows WITHOUT type; include internal _slot
    rows = []
    skips = {"client_vide":0,"canalid_vide":0,"date_invalide":0,"sans_heure":0}
    for _, r in dfc.iterrows():
        client_name = str(r["Client"]).strip()
        chat_id = str(r["Canal ID"]).strip()
        prog = str(r["Programme"]).strip()
        saison = int(r["Saison"])
        start = r["Date de Démarrage"]
        jours = r["Jours de Diffusion"]
        if not client_name: skips["client_vide"]+=1; continue
        if not chat_id: skips["canalid_vide"]+=1; continue
        if pd.isna(start): skips["date_invalide"]+=1; continue

        # avancement counting only diffusion days
        max_d = max(dates_fenetre)
        cnt=0; adv_by_date={}
        cur = start.date()
        while cur <= max_d:
            if len(jours)==0 or _weekday_fr(cur) in jours:
                cnt += 1
            adv_by_date[cur]=cnt
            cur += timedelta(days=1)

        for d in dates_fenetre:
            if len(jours)!=0 and _weekday_fr(d) not in jours:
                continue
            adv = int(adv_by_date.get(d,0))
            # Build slots 1..3
            for k in (1,2,3):
                h = r.get(f"Heure envoi {k}", "")
                if not h:
                    continue
                rows.append({
                    "client": client_name,
                    "programme": prog,
                    "saison": saison,
                    "chat_id": chat_id,
                    "date": d.strftime("%Y-%m-%d"),
                    "heure": h,
                    "type": "",  # will be filled later
                    "avancement": adv,
                    "message": "",
                    "format": "",
                    "url": "",
                    "envoye": "non",
                    "_slot": k,
                })
        if (not r.get("Heure envoi 1") and not r.get("Heure envoi 2") and not r.get("Heure envoi 3")):
            skips["sans_heure"] += 1

    dfn = pd.DataFrame(rows)
    if dfn.empty:
        print(f"[DEBUG] df_nouveau est vide ; skips={skips}")
    else:
        print(f"[DEBUG] Nouveau par date: {dfn['date'].value_counts().to_dict()}\n[DEBUG] skips={skips}")

    # Read existing planning
    records = ws_planning.get_all_records()
    cols_plan = ["client","programme","saison","chat_id","date","heure","type","avancement","message","format","url","envoye"]
    if records:
        dfe = pd.DataFrame(records)
        for c in cols_plan:
            if c not in dfe.columns:
                dfe[c] = ""
    else:
        dfe = pd.DataFrame(columns=cols_plan)

    # Normalize keys before merge
    if not dfn.empty:
        _normalize_key_columns(dfn)
    if not dfe.empty:
        _normalize_key_columns(dfe)

    # Purge old
    if not dfe.empty:
        dfe["_date_obj"] = pd.to_datetime(dfe["date"], format="%Y-%m-%d", errors="coerce").dt.date
        cutoff = today - timedelta(days=RETENTION)
        dfe = dfe[dfe["_date_obj"].notna() & (dfe["_date_obj"]>=cutoff)].drop(columns=["_date_obj"])

    # Merge & dedup
    key_cols = ["client","programme","saison","chat_id","date","heure"]
    # NOTE: exclude 'type' from key since it's now filled post-merge
    dfm = pd.concat([dfe, dfn], ignore_index=True)
    dfm.drop_duplicates(subset=key_cols, keep="first", inplace=True)

    # ==== Fill messages / type from programme tabs ====
    # Preload programme tabs
    cache_prog = {}
    def get_prog_df(prog):
        prog = str(prog).zfill(3)
        if prog in cache_prog:
            return cache_prog[prog]
        try:
            ws = doc_programmes.worksheet(prog)
            dfp = pd.DataFrame(ws.get_all_records())
            for c in ["Support","Saison","Jour","Type","Phrase","Format","Url"]:
                if c not in dfp.columns: dfp[c] = ""
            dfp["Saison"] = pd.to_numeric(dfp["Saison"], errors="coerce").fillna(1).astype(int)
            dfp["Jour"] = pd.to_numeric(dfp["Jour"], errors="coerce").fillna(1).astype(int)
            dfp["Type"] = pd.to_numeric(dfp["Type"], errors="coerce").astype("Int64")
        except Exception:
            dfp = pd.DataFrame(columns=["Support","Saison","Jour","Type","Phrase","Format","Url"])
        cache_prog[prog]=dfp
        return dfp

    # Determine slot position for rows (if _slot missing because it came from existing dfe)
    def compute_slot_indices(group):
        # sort by heure then assign slot 1..3
        temp = group.copy()
        temp["_slot"] = pd.to_datetime(temp["heure"], format="%H:%M:%S", errors="coerce")
        temp = temp.sort_values("_slot", kind="stable")
        temp["_slot"] = range(1, len(temp)+1)
        return temp["_slot"]

    if "_slot" not in dfm.columns or dfm["_slot"].isna().any():
        dfm["_slot"] = dfm.groupby(["client","programme","saison","date"]).apply(compute_slot_indices).reset_index(level=[0,1,2,3], drop=True)

    # choose type_id per slot: prefer client-specified 'Type envoi k', else DEFAULT_SLOT_TYPE_IDS[k-1]
    def type_id_for_row(r):
        k = int(r.get("_slot", 1))
        # find the client row to look up per-slot type override
        # This mapping uses DEFAULT only; client override requires mapping by client name if needed.
        # For simplicity use DEFAULT here:
        try:
            return int(DEFAULT_SLOT_TYPE_IDS[k-1])
        except Exception:
            return None

    labels, messages, formats, urls = [], [], [], []
    for idx, r in dfm.iterrows():
        prog = str(r["programme"]).zfill(3)
        saison = int(pd.to_numeric(r["saison"], errors="coerce") or 1)
        jour = int(pd.to_numeric(r["avancement"], errors="coerce") or 1)
        dfp = get_prog_df(prog)

        # pick k-th row for this (saison, jour) sorted by Type id, based on slot
        k = int(r.get("_slot", 1))
        subset = dfp[(dfp["Saison"]==saison) & (dfp["Jour"]==jour)].copy()
        subset = subset.sort_values("Type")
        rec = subset.iloc[k-1] if len(subset) >= k else None

        if rec is not None and pd.notna(rec.get("Phrase","")) and str(rec.get("Phrase","")) != "":
            val = pd.to_numeric(rec.get("Type"), errors="coerce")
            type_id = int(val) if pd.notna(val) else 0
            label = types_id_to_label.get(type_id, str(type_id))
            labels.append(label)
            messages.append(f"Saison {saison} - Jour {jour} : \n{label} : {rec.get('Phrase','')}")
            fmt = str(rec.get("Format","texte")).strip().lower() or "texte"
            formats.append(fmt)
            urls.append(str(rec.get("Url","")))
        else:
            labels.append("")
            messages.append("")
            formats.append("texte")
            urls.append("")

    dfm["type"] = labels
    dfm["message"] = messages
    dfm["format"] = formats
    dfm["url"] = urls

    # Sort by date then time (as strings standardized), to avoid tz warnings
    dfm["date_norm"] = dfm["date"].apply(lambda x: pd.to_datetime(x, format="%Y-%m-%d", errors="coerce"))
    dfm["heure_norm"] = pd.to_datetime(dfm["heure"], format="%H:%M:%S", errors="coerce")
    dfm = dfm.sort_values(["date_norm","heure_norm"]).drop(columns=["date_norm","heure_norm","_slot"], errors="ignore")

    # Write
    for c in dfm.columns:
        dfm[c] = dfm[c].astype(str)
    ws_planning.clear()
    ws_planning.update([dfm.columns.tolist()] + dfm.values.tolist())
    print(f"[DEBUG] Total par date (après fusion): {dfm['date'].value_counts().to_dict()}\n📅 Mise à jour planning à {datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S %Z')}")

    # === Mise à jour "Date de Fin" dans la feuille Clients (si vide) ===

    # 1) S'assurer que la colonne existe
    header = ws_clients.row_values(1)
    if "Date de Fin" not in header:
        all_vals = ws_clients.get_all_values()
        header.append("Date de Fin")
        if all_vals:
            ws_clients.update("A1", [header] + all_vals[1:])
        else:
            ws_clients.update("A1", [header])
        header = ws_clients.row_values(1)
    date_fin_col_idx = header.index("Date de Fin") + 1  # index 1-based

    # 2) Cache du nombre de jours par (programme, saison)
    nb_jours_cache = {}

    def _nb_jours_for(prog, saison):
        key = (prog, int(saison))
        if key in nb_jours_cache:
            return nb_jours_cache[key]
        dfp = get_prog_df(prog)  # utilise le cache_prog défini plus haut
        if dfp is None or dfp.empty:
            nb_jours_cache[key] = None
            return None
        sel = dfp[dfp["Saison"] == int(saison)]
        if sel.empty:
            nb_jours_cache[key] = None
            return None
        m = pd.to_numeric(sel["Jour"], errors="coerce")
        m = m[m.notna()]
        nb = int(m.max()) if not m.empty else None
        nb_jours_cache[key] = nb
        return nb

    # 3) Construire les updates pour chaque client sans "Date de Fin"
    updates = []
    for i, (_, r) in enumerate(dfc.iterrows(), start=2):  # lignes sheet = 2..N
        cur_fin = str(r.get("Date de Fin", "")).strip()
        if cur_fin:
            continue

        prog = str(r.get("Programme", "")).strip().zfill(3)
        saison = r.get("Saison")
        start = r.get("Date de Démarrage")
        jours = r.get("Jours de Diffusion", set())

        if not prog or pd.isna(saison) or pd.isna(start):
            continue

        # start -> date
        start_dt = pd.to_datetime(start, errors="coerce")
        if pd.isna(start_dt):
            continue

        nb = _nb_jours_for(prog, int(saison))
        if not nb or nb <= 0:
            continue

        # normaliser l'ensemble des jours autorisés
        if isinstance(jours, (list, tuple, set)):
            jours_set = {str(x).strip().lower() for x in jours}
        else:
            jours_set = {p.strip().lower() for p in str(jours).replace(";", ",").split(",") if p.strip()}

        # simuler les jours de diffusion depuis la date de démarrage
        count = 0
        cur = start_dt.date()
        last = cur
        while count < nb:
            if (not jours_set) or (_weekday_fr(cur) in jours_set):
                count += 1
                last = cur
            cur += timedelta(days=1)

        updates.append((i, last.strftime("%Y-%m-%d")))

    # 4) Batch update dans la feuille Clients
    if updates:
        data_body = {
            "valueInputOption": "RAW",
            "data": [
                {
                    "range": f"{config.FEUILLE_CLIENTS}!{gspread.utils.rowcol_to_a1(r, date_fin_col_idx)}",
                    "values": [[val]],
                }
                for (r, val) in updates
            ],
        }
        ws_clients.spreadsheet.values_batch_update(data_body)
        print(f"📝 Dates de fin mises à jour pour {len(updates)} client(s).")
    else:
        print("📝 Aucune date de fin à compléter.")

if __name__ == "__main__":
    generer_planning()
