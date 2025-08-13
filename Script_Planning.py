import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from datetime import datetime, timedelta
import pytz
import unicodedata
import config

# ==== Configuration columns (strict headers) ====
CLIENTS_COLS = [
    "Client", "Thème", "Canal ID", "Programme", "Saison",
    "Date de Démarrage", "Jours de Diffusion",
    "Heure Conseil", "Heure Aphorisme", "Heure Réflexion",
]

PLANNING_COLS = [
    "client","programme","saison","chat_id","date","heure",
    "type","avancement","message","format","url","envoye",
]

PROGRAMME_COLS = ["Support","Saison","Jour","Type","Phrase","Format","Url"]

def _tz():
    try:
        return pytz.timezone(config.FUSEAU_HORAIRE)
    except Exception:
        return pytz.timezone("Europe/Paris")

def _to_time_hms(val):
    if pd.isna(val) or str(val).strip() == "":
        return ""
    s = str(val).strip()
    # normalize common variations
    s = s.replace("h", ":").replace("H", ":").replace(" ", "")
    s = s.replace(".", ":")
    # try several formats
    for fmt in ("%H:%M:%S","%H:%M"):
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

def _weekday_fr(dt_date):
    jours = ["lundi","mardi","mercredi","jeudi","vendredi","samedi","dimanche"]
    return jours[dt_date.weekday()]

def _parse_jours_diffusion(s):
    if pd.isna(s): return set()
    txt = str(s)
    # If Google Sheets multi-select chips come as comma-separated labels
    txt = txt.replace(";", ",")
    parts = [p.strip().lower() for p in txt.split(",") if p.strip()]
    # normalize potential English weekday names
    map_en = {
        "monday":"lundi","tuesday":"mardi","wednesday":"mercredi",
        "thursday":"jeudi","friday":"vendredi","saturday":"samedi","sunday":"dimanche"
    }
    out = []
    for p in parts:
        out.append(map_en.get(p, p))
    return set(out)

def _normalize(s):
    # lowercase, strip accents
    s = str(s)
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    return s.lower().strip()

def _type_candidates(local_type):
    # Accept both plain labels and numbered labels (1-Aphorisme, 2-Conseil, 3-Réflexion)
    base = local_type
    if base.lower() == "réflexion":
        base_alt = "Reflexion"
    else:
        base_alt = base
    mapping_num = {"Aphorisme":"1","Conseil":"2","Réflexion":"3","Reflexion":"3"}
    num = mapping_num.get(base, mapping_num.get(base_alt, ""))
    cands = [base, base_alt, base.lower(), base_alt.lower()]
    if num:
        cands += [f"{num}-{base}", f"{num} - {base}", f"{num}-{base_alt}", f"{num} - {base_alt}",
                  f"{num}-{base.lower()}", f"{num}-{base_alt.lower()}"]
    return list(dict.fromkeys(cands))  # unique, keep order

def generer_planning():
    tz = _tz()
    NB_JOURS = getattr(config, "NB_JOURS_GENERATION", 2)
    RETENTION = getattr(config, "RETENTION_JOURS", 2)

    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(config.CHEMIN_CLE_JSON, scopes=scope)
    gc = gspread.authorize(creds)

    ws_clients = gc.open(config.FICHIER_CLIENTS).worksheet(config.FEUILLE_CLIENTS)
    ws_planning = gc.open(config.FICHIER_PLANNING).worksheet(config.FEUILLE_PLANNING)

    dfc = pd.DataFrame(ws_clients.get_all_records())
    for c in CLIENTS_COLS:
        if c not in dfc.columns: dfc[c] = ""

    # normalize fields
    dfc["Programme"] = dfc["Programme"].apply(lambda x: str(x).zfill(3))
    dfc["Saison"] = pd.to_numeric(dfc["Saison"], errors="coerce").fillna(1).astype(int)
    # dayfirst per user locale JJ/MM/AAAA
    dfc["Date de Démarrage"] = pd.to_datetime(dfc["Date de Démarrage"], errors="coerce", dayfirst=True).dt.date
    for h in ["Heure Conseil","Heure Aphorisme","Heure Réflexion"]:
        dfc[h] = dfc[h].apply(_to_time_hms)
    dfc["jours_set"] = dfc["Jours de Diffusion"].apply(_parse_jours_diffusion)

    today = datetime.now(tz).date()
    dates_fenetre = [today + timedelta(days=i) for i in range(NB_JOURS)]

    type_to_hourcol = {"Conseil":"Heure Conseil","Aphorisme":"Heure Aphorisme","Réflexion":"Heure Réflexion"}

    rows = []
    skips = {"client_vide":0,"canalid_vide":0,"date_invalide":0,"sans_heure":0}
    max_date = max(dates_fenetre)

    for _, r in dfc.iterrows():
        client = str(r["Client"]).strip()
        if not client:
            skips["client_vide"] += 1; continue
        chat_id = str(r["Canal ID"]).strip()
        if not chat_id:
            skips["canalid_vide"] += 1; continue
        d0 = r["Date de Démarrage"]
        if pd.isna(d0):
            skips["date_invalide"] += 1; continue

        jours_aut = r["jours_set"]
        # compute avancement counter per date
        cnt = 0
        av_by_date = {}
        cur = d0
        while cur <= max_date:
            if (not jours_aut) or (_weekday_fr(cur) in jours_aut):
                cnt += 1
            av_by_date[cur] = cnt
            cur += timedelta(days=1)

        # produce for each date and type if hour present
        any_hour = any(r[col] for col in type_to_hourcol.values())
        if not any_hour:
            skips["sans_heure"] += 1; continue

        for d in dates_fenetre:
            if jours_aut and _weekday_fr(d) not in jours_aut:
                continue
            for typ, hcol in type_to_hourcol.items():
                hh = r[hcol]
                if not hh:
                    continue
                rows.append({
                    "client": client,
                    "programme": str(r["Programme"]).zfill(3),
                    "saison": int(r["Saison"]),
                    "chat_id": chat_id,
                    "date": d.strftime("%Y-%m-%d"),
                    "heure": hh,
                    "type": typ,
                    "avancement": int(av_by_date.get(d, 0)),
                    "message": "",
                    "format": "",
                    "url": "",
                    "envoye": "non",
                })

    df_new = pd.DataFrame(rows)
    print(f"[DEBUG] today={today} NB_JOURS={NB_JOURS} dates={dates_fenetre}")
    print(f"[DEBUG] skips={skips}")
    print(f"[DEBUG] Nouveau par date: {df_new['date'].value_counts().to_dict() if not df_new.empty else {}}")

    # Read existing planning
    recs = ws_planning.get_all_records()
    if recs:
        dfe = pd.DataFrame(recs)
        for c in PLANNING_COLS:
            if c not in dfe.columns: dfe[c] = ""
        dfe["programme"] = dfe["programme"].apply(lambda x: str(x).zfill(3))
    else:
        dfe = pd.DataFrame(columns=PLANNING_COLS)

    # purge older than today-RETENTION
    def _to_date(x):
        try: return pd.to_datetime(x).date()
        except: return None
    if not dfe.empty:
        dfe["_d"] = dfe["date"].apply(_to_date)
        cutoff = today - timedelta(days=RETENTION)
        dfe = dfe[dfe["_d"].notna() & (dfe["_d"] >= cutoff)].drop(columns=["_d"])

    # merge, dedupe
    df_all = pd.concat([dfe, df_new], ignore_index=True)
    key = ["client","programme","saison","chat_id","date","heure","type"]
    df_all.drop_duplicates(subset=key, keep="last", inplace=True)

    # Build tz-aware _dt for sort
    def mk_dt(row):
        s = f"{row['date']} {row['heure']}"
        return pd.to_datetime(s, errors="coerce")
    df_all["_dt_naive"] = df_all.apply(mk_dt, axis=1)
    tz = _tz()
    mask = df_all["_dt_naive"].notna()
    if mask.any():
        localized = df_all.loc[mask, "_dt_naive"].astype("datetime64[ns]").dt.tz_localize(tz, ambiguous="infer", nonexistent="shift_forward")
        # initialize column with NaT tz-aware to avoid dtype warnings
        df_all["_dt"] = pd.NaT
        df_all["_dt"] = df_all["_dt"].astype("datetime64[ns, {}]".format(tz.zone))
        df_all.loc[mask, "_dt"] = localized
    else:
        df_all["_dt"] = pd.NaT

    # Fill messages from programmes
    try:
        doc_prog = gc.open(config.FICHIER_PROGRAMMES)
    except Exception:
        doc_prog = None

    cache = {}
    msgs, fmts, urls = [], [], []

    for _, row in df_all.iterrows():
        if doc_prog is None:
            msgs.append(""); fmts.append("texte"); urls.append(""); continue
        prog = str(row["programme"]).zfill(3)
        if prog not in cache:
            try:
                ws = doc_prog.worksheet(prog)
                dfp = pd.DataFrame(ws.get_all_records())
                for c in PROGRAMME_COLS:
                    if c not in dfp.columns: dfp[c] = ""
                dfp["Saison"] = pd.to_numeric(dfp["Saison"], errors="coerce").fillna(1).astype(int)
                dfp["Jour"] = pd.to_numeric(dfp["Jour"], errors="coerce").fillna(1).astype(int)
                cache[prog] = dfp
            except Exception:
                cache[prog] = pd.DataFrame(columns=PROGRAMME_COLS)

        dfp = cache[prog]
        if dfp.empty:
            msgs.append(""); fmts.append("texte"); urls.append(""); continue

        saison = int(row["saison"]); jour = int(row["avancement"])
        local_type = str(row["type"]).strip()
        cands = _type_candidates(local_type)

        sub = dfp[(dfp["Saison"]==saison) & (dfp["Jour"]==jour)]
        # try to match Type with multiple candidates (case/accents tolerant)
        if not sub.empty:
            # precompute normalized type column
            col_norm = dfp["Type"].apply(_normalize)
            sub = dfp.loc[sub.index]
            sub_norm = col_norm.loc[sub.index]
            found = None
            cand_norms = [_normalize(c) for c in cands]
            for i, norm in zip(sub.index, sub_norm):
                if norm in cand_norms:
                    found = dfp.loc[i]
                    break
            if found is not None:
                phrase = str(found.get("Phrase",""))
                fmt = str(found.get("Format","texte")).strip().lower() or "texte"
                url = str(found.get("Url",""))
                msgs.append(f"Saison {saison} - Jour {jour} : \n{local_type} : {phrase}")
                fmts.append(fmt); urls.append(url); continue

        # default if not matched
        msgs.append("")
        fmts.append("texte")
        urls.append("")

    df_all["message"] = msgs
    df_all["format"] = fmts
    df_all["url"] = urls

    print("[DEBUG] Total par date (après fusion):", df_all["date"].value_counts().to_dict())

    # Write back (full write like original)
    # cast to string to avoid NaN
    for c in df_all.columns:
        df_all[c] = df_all[c].astype(str)
    # order columns as PLANNING_COLS
    out = df_all[PLANNING_COLS]
    ws_planning.clear()
    ws_planning.update([out.columns.tolist()] + out.values.tolist())

    print(f"📅 Mise à jour planning à {datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S %Z')}")

if __name__ == "__main__":
    generer_planning()
