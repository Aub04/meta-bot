import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from datetime import datetime, timedelta
import pytz
import re
import config

def _tz():
    try:
        return pytz.timezone(config.FUSEAU_HORAIRE)
    except Exception:
        return pytz.timezone("Europe/Paris")

def _normalize_headers(cols):
    # lower, strip, collapse spaces
    out = []
    for c in cols:
        s = str(c).strip().lower()
        s = re.sub(r'\s+', ' ', s)
        out.append(s)
    return out

# canonical columns expected
ALIASES = {
    "client": ["client"],
    "programme": ["programme", "program", "programme "],
    "saison": ["saison", "season"],
    "chat_id": ["chat_id", "canal id", "canalid", "canal", "channel id", "id canal"],
    "date de démarrage": ["date de démarrage", "date demarrage", "date de demarrage", "start date"],
    "jours de diffusion": ["jours de diffusion", "jours diffusion", "jours", "days"],
    "heure conseil": ["heure conseil", "heure envoi 1", "heure 1"],
    "heure aphorisme": ["heure aphorisme", "heure envoi 2", "heure 2"],
    "heure réflexion": ["heure réflexion", "heure reflexion", "heure envoi 3", "heure 3"],
}

def _find_col(name_canon, cols_norm):
    aliases = ALIASES.get(name_canon, [name_canon])
    for a in aliases:
        if a in cols_norm:
            return cols_norm.index(a)
    return None

def _to_time_hms(val):
    if pd.isna(val) or str(val).strip() == "":
        return ""
    s = str(val).strip()
    # If it's numeric (Google Sheets time), try parsing via pandas without format
    try:
        # allow "10 h 00" etc.
        s2 = s.lower().replace('h',':').replace(' ', '')
        for fmt in ("%H:%M:%S", "%H:%M"):
            try:
                t = pd.to_datetime(s2, format=fmt).time()
                return t.strftime("%H:%M:%S")
            except: pass
        t = pd.to_datetime(s).time()
        return t.strftime("%H:%M:%S")
    except Exception:
        return ""

def _weekday_fr(d):
    return ["lundi","mardi","mercredi","jeudi","vendredi","samedi","dimanche"][d.weekday()]

def generer_planning():
    tz = _tz()
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(config.CHEMIN_CLE_JSON, scopes=scope)
    client = gspread.authorize(creds)

    ws_clients = client.open(config.FICHIER_CLIENTS).worksheet(config.FEUILLE_CLIENTS)
    ws_planning = client.open(config.FICHIER_PLANNING).worksheet(config.FEUILLE_PLANNING)

    rows = ws_clients.get_all_values()
    if not rows or len(rows)<2:
        print("Aucun client.")
        return

    header = rows[0]
    data = rows[1:]
    cols_norm = _normalize_headers(header)
    df_raw = pd.DataFrame(data, columns=cols_norm)

    # map needed columns by alias
    idx_client = _find_col("client", cols_norm)
    idx_prog = _find_col("programme", cols_norm)
    idx_saison = _find_col("saison", cols_norm)
    idx_chat = _find_col("chat_id", cols_norm)
    idx_date = _find_col("date de démarrage", cols_norm)
    idx_jours = _find_col("jours de diffusion", cols_norm)
    idx_h1 = _find_col("heure conseil", cols_norm)
    idx_h2 = _find_col("heure aphorisme", cols_norm)
    idx_h3 = _find_col("heure réflexion", cols_norm)

    needed = [idx_client, idx_prog, idx_saison, idx_chat, idx_date, idx_jours]
    if any(i is None for i in needed):
        print("[DEBUG] colonnes manquantes:", {k:v for k,v in zip(["client","programme","saison","chat_id","date","jours"], needed)})
        return

    df_clients = pd.DataFrame({
        "Client": df_raw.iloc[:, idx_client],
        "Programme": df_raw.iloc[:, idx_prog],
        "Saison": df_raw.iloc[:, idx_saison],
        "chat_id": df_raw.iloc[:, idx_chat],
        "Date de Démarrage": df_raw.iloc[:, idx_date],
        "Jours de Diffusion": df_raw.iloc[:, idx_jours],
        "Heure Conseil": df_raw.iloc[:, idx_h1] if idx_h1 is not None else "",
        "Heure Aphorisme": df_raw.iloc[:, idx_h2] if idx_h2 is not None else "",
        "Heure Réflexion": df_raw.iloc[:, idx_h3] if idx_h3 is not None else "",
    })

    # normalize
    df_clients["Programme"] = df_clients["Programme"].apply(lambda x: str(x).zfill(3))
    df_clients["Saison"] = pd.to_numeric(df_clients["Saison"], errors="coerce").fillna(1).astype(int)

    # parse date (dayfirst true)
    df_clients["Date de Démarrage"] = pd.to_datetime(df_clients["Date de Démarrage"], errors="coerce", dayfirst=True).dt.date

    # parse heures
    for h in ["Heure Conseil","Heure Aphorisme","Heure Réflexion"]:
        df_clients[h] = df_clients[h].apply(_to_time_hms)

    # jours set (accept chips or comma)
    def parse_jours(s):
        if pd.isna(s): return set()
        s = str(s)
        s = s.replace(" ", " ").replace(";", ",")  # nbsp
        parts = [p.strip().lower() for p in s.split(",") if p.strip()]
        return set(parts)
    df_clients["jours_set"] = df_clients["Jours de Diffusion"].apply(parse_jours)

    NB_JOURS = getattr(config, "NB_JOURS_GENERATION", 2)
    today_local = datetime.now(tz).date()
    dates_fenetre = [today_local + timedelta(days=i) for i in range(NB_JOURS)]
    print(f"[DEBUG] today_local={today_local} NB_JOURS={NB_JOURS} dates_fenetre={dates_fenetre}")

    # build rows
    rows_out = []
    skips = {"client_vide":0, "chat_vide":0, "date_invalide":0, "sans_heure":0}
    for _, r in df_clients.iterrows():
        client_name = str(r["Client"]).strip()
        if not client_name:
            skips["client_vide"] += 1; continue
        chat = str(r["chat_id"]).strip()
        if not chat:
            skips["chat_vide"] += 1; continue
        d0 = r["Date de Démarrage"]
        if pd.isna(d0):
            skips["date_invalide"] += 1; continue
        jours = r["jours_set"]

        # compute avancement by date counting only allowed days
        max_d = max(dates_fenetre)
        cnt = 0
        av_map = {}
        cur = d0
        while cur <= max_d:
            if not jours or _weekday_fr(cur) in jours:
                cnt += 1
            av_map[cur] = cnt
            cur += timedelta(days=1)

        heures = [r["Heure Conseil"], r["Heure Aphorisme"], r["Heure Réflexion"]]
        if not any(heures):
            skips["sans_heure"] += 1; continue
        type_names = ["Conseil","Aphorisme","Réflexion"]

        for d in dates_fenetre:
            if jours and _weekday_fr(d) not in jours:
                continue
            av = av_map.get(d, 0)
            for type_name, heure in zip(type_names, heures):
                if not heure: continue
                rows_out.append({
                    "client": client_name,
                    "programme": str(r["Programme"]).zfill(3),
                    "saison": int(r["Saison"]),
                    "chat_id": chat,
                    "date": d.strftime("%Y-%m-%d"),
                    "heure": heure,
                    "type": type_name,
                    "avancement": av,
                    "message": "",
                    "format": "",
                    "url": "",
                    "envoye": "non",
                })

    df_nouveau = pd.DataFrame(rows_out)
    if df_nouveau.empty:
        print("[DEBUG] df_nouveau est vide")
    else:
        print("[DEBUG] Nouveau par date:", dict(df_nouveau["date"].value_counts().sort_index()))

    # read existing planning
    cols_planning = ["client","programme","saison","chat_id","date","heure","type","avancement","message","format","url","envoye"]
    recs = ws_planning.get_all_records()
    if recs:
        df_exist = pd.DataFrame(recs)
        for c in cols_planning:
            if c not in df_exist.columns: df_exist[c] = ""
        df_exist["programme"] = df_exist["programme"].apply(lambda x: str(x).zfill(3))
    else:
        df_exist = pd.DataFrame(columns=cols_planning)

    # purge by RETENTION_JOURS
    RET = getattr(config, "RETENTION_JOURS", 2)
    cutoff = today_local - timedelta(days=RET)
    def to_date(x):
        try: return pd.to_datetime(x).date()
        except: return None
    if not df_exist.empty:
        df_exist["_d"] = df_exist["date"].apply(to_date)
        df_exist = df_exist[df_exist["_d"].notna()]
        df_exist = df_exist[df_exist["_d"] >= cutoff].drop(columns=["_d"])

    # merge
    df_all = pd.concat([df_exist, df_nouveau], ignore_index=True)
    df_all["programme"] = df_all["programme"].apply(lambda x: str(x).zfill(3))
    df_all["saison"] = pd.to_numeric(df_all["saison"], errors="coerce").fillna(1).astype(int)
    df_all["avancement"] = pd.to_numeric(df_all["avancement"], errors="coerce").fillna(1).astype(int)
    df_all["envoye"] = df_all["envoye"].replace({pd.NA:"non", None:"non", "": "non"})
    key = ["client","programme","saison","chat_id","date","heure","type"]
    df_all.drop_duplicates(subset=key, keep="last", inplace=True)

    # tz-aware _dt WITHOUT dtype warning
    df_all["_dt"] = pd.Series(pd.NaT, index=df_all.index, dtype=f"datetime64[ns, {tz.zone}]")
    mask = df_all["date"].notna() & df_all["heure"].notna()
    s = (df_all.loc[mask, "date"] + " " + df_all.loc[mask, "heure"])
    dt_naive = pd.to_datetime(s, errors="coerce")
    localized = dt_naive.dt.tz_localize(tz, ambiguous="infer", nonexistent="shift_forward")
    df_all.loc[mask, "_dt"] = localized

    df_all.sort_values(by=["_dt","client","type"], inplace=True, kind="stable")
    df_all.drop(columns=["_dt"], inplace=True)

    # fill messages from programmes (same as before, omitted for brevity in this debug)
    # write back
    for c in df_all.columns:
        df_all[c] = df_all[c].astype(str)
    ws_planning.clear()
    ws_planning.update([df_all.columns.tolist()] + df_all.values.tolist())

    print(f"[DEBUG] Total par date (après fusion): {dict(df_all['date'].value_counts().sort_index())}")
    print(f"📅 Mise à jour planning à {datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S %Z')}")

if __name__ == "__main__":
    generer_planning()
