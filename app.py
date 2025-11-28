import io
from collections import defaultdict

import pandas as pd
import requests
import streamlit as st

# -------------------------------------------------------------------
# Constants
# -------------------------------------------------------------------
VER_BASE = "https://vtr.valasztas.hu/ogy2022/data/04022333/ver"
SZAVOSSZ_BASE = "https://vtr.valasztas.hu/ogy2022/data/04161400/szavossz"


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

def slugify(name: str) -> str:
    if name is None:
        return "unknown"
    s = name.strip().lower()
    for ch in [" ", "-", "/", "(", ")", "’", "'", "„", "”"]:
        s = s.replace(ch, "_")
    for ch in [",", ".", ":", ";"]:
        s = s.replace(ch, "")
    while "__" in s:
        s = s.replace("__", "_")
    s = s.strip("_")
    return s or "unknown"

# -------------------------------------------------------------------
# Party / list name normalisation to English
# -------------------------------------------------------------------

PARTY_NAME_MAP = {
    "DK-JOBBIK-MOMENTUM-MSZP-LMP-PÁRBESZÉD": "United for Hungary",
    "FIDESZ-KDNP": "Fidesz",
    "ORÖ": "Ruthenians",
    "BOLGÁR ORSZÁGOS ÖNKORMÁNYZAT": "Bulgarians",
    "OHÖ": "Croatians",
    "MAGYARORSZÁGI GÖRÖGÖK ORSZÁGOS": "Greeks",
    "UOÖ": "Ukrainians",
    "OÖÖ": "Armenians",
    "MNOÖ": "Germans",
    "OLÖ": "Poles",
    "ORSZÁGOS SZLOVÁK ÖNK": "Slovaks",
    "MI HAZÁNK": "Our Homeland",
    "MKKP": "Two-Tailed Dog",
    "MEMO": "Solution",
    "NORMÁLIS PÁRT": "Normal Life",
}

# crude keyword fallback for other minority lists
MINORITY_KEYWORDS = {
    "NÉMET": "Germans",
    "SZLOVÁK": "Slovaks",
    "BOLGÁR": "Bulgarians",
    "GÖRÖG": "Greeks",
    "UKRÁN": "Ukrainians",
    "HORVÁT": "Croatians",
    "SZERB": "Serbs",
    "ROMA": "Roma",
    "SZLOVÉN": "Slovenes",
    "LENGYEL": "Poles",
    "ÖRMÉNY": "Armenians",
    "UKRÁN": "Ukrainians",
}
COLUMN_RENAME_MAP = {
    "szk_nev": "polling_station_name",
    "evk": "constituency_code",
    "evk_nev": "constituency_name",
    "cim": "polling_station_address",

    "akadaly": "accessible_for_disabled",          # 1 = accessible
    "szamlKijelolt": "designated_counting_station",# 1 = counting station
    "atjKijelolt": "designated_transfer_station",  # 1 = handles transferred voters
    "telepSzintu": "municipality_level_station",   # 1 = single station for whole municipality

    "letszam_indulo": "electorate_initial",        # starting register size
    "letszam_honos": "electorate_resident",        # resident voters
    "letszam_atjel": "electorate_transferred_in",  # voters voting here from elsewhere
    "letszam_atjelInnen": "electorate_transferred_out", # voters from here voting elsewhere
    "letszam_osszesen": "electorate_total",        # total eligible at station

    "vp_osszes_egyeni": "eligible_voters_individual",
    "szavazott_osszesen_egyeni": "turnout_individual",
    "szavazott_osszesen_szaz_egyeni": "turnout_rate_pct_individual",
    "szl_ervenyes_egyeni": "valid_votes_individual",
    "szl_ervenytelen_egyeni": "invalid_votes_individual",

    "vp_osszes_lista": "eligible_voters_list",
    "szavazott_osszesen_lista": "turnout_list",
    "szavazott_osszesen_szaz_lista": "turnout_rate_pct_list",
    "szl_ervenyes_lista": "valid_votes_list",
    "szl_ervenytelen_lista": "invalid_votes_list",
}

def canonical_party_name(raw: str) -> str:
    """Map Hungarian party/list names to short English labels where possible."""
    if not raw:
        return "Unknown"
    key = raw.strip().upper()

    # direct mapping first
    if key in PARTY_NAME_MAP:
        return PARTY_NAME_MAP[key]

    # keyword-based minority guess
    for kw, eng in MINORITY_KEYWORDS.items():
        if kw in key:
            return eng

    # fallback: title-case original
    return raw.title()

def fetch_json_with_log(url: str, log: list):
    """Fetch JSON, return dict or None, and append a detailed record to `log`."""
    try:
        resp = requests.get(url, headers={"User-Agent": "ogy2022-streamlit-scraper"})
        status = resp.status_code
        if status == 200:
            try:
                data = resp.json()
                log.append({"url": url, "status": status, "ok": True, "error": ""})
                return data
            except Exception as e:
                log.append(
                    {
                        "url": url,
                        "status": status,
                        "ok": False,
                        "error": f"JSON decode error: {e}",
                    }
                )
                return None
        else:
            log.append(
                {"url": url, "status": status, "ok": False, "error": "HTTP error"}
            )
            return None
    except Exception as e:
        log.append(
            {"url": url, "status": None, "ok": False, "error": f"Request error: {e}"}
        )
        return None


def load_global_meta(log):
    """Download global metadata JSONs with logging."""
    telep_raw = fetch_json_with_log(f"{VER_BASE}/Telepulesek.json", log)
    egyeni_raw = fetch_json_with_log(f"{VER_BASE}/EgyeniJeloltek.json", log)
    listak_raw = fetch_json_with_log(f"{VER_BASE}/ListakEsJeloltek.json", log)
    jlcs_raw = fetch_json_with_log(f"{VER_BASE}/Jlcs.json", log)
    szervezetek_raw = fetch_json_with_log(f"{VER_BASE}/Szervezetek.json", log)

    telep = telep_raw.get("list", []) if telep_raw else []
    egyeni = egyeni_raw.get("list", []) if egyeni_raw else []
    listak = listak_raw.get("list", []) if listak_raw else []
    jlcs = jlcs_raw.get("list", []) if jlcs_raw else []
    szervezetek = szervezetek_raw.get("list", []) if szervezetek_raw else []

    return telep, egyeni, listak, jlcs, szervezetek


def build_constituency_id_mapping(egyeni_list):
    cand_by_const = defaultdict(set)
    cand_meta_rows = []

    for c in egyeni_list:
        maz = c["maz"]
        evk = c["evk"]
        ej_id = c["ej_id"]
        jlcs_nev = c.get("jlcs_nev", "UNKNOWN")
        name = c.get("neve", "")

        cand_by_const[(maz, evk)].add(ej_id)

        # use canonical English party/list name for columns
        canon_name = canonical_party_name(jlcs_nev)
        party_slug = slugify(canon_name)
        colname = f"candidate_{party_slug}_name"

        cand_meta_rows.append(
            {
                "maz": maz,
                "evk": evk,
                "party_slug": party_slug,
                "col": colname,
                "candidate_name": name,
            }
        )

    candidate_sets = {}
    constituency_id_by_const = {}
    next_id = 1

    for (maz, evk), ej_set in sorted(cand_by_const.items()):
        sig = tuple(sorted(ej_set))
        if sig not in candidate_sets:
            candidate_sets[sig] = next_id
            next_id += 1
        constituency_id_by_const[(maz, evk)] = candidate_sets[sig]

    const_rows = []
    for (maz, evk), cid in constituency_id_by_const.items():
        const_rows.append({"maz": maz, "evk": evk, "constituency_id": cid})
    df_const_ids = pd.DataFrame(const_rows)

    df_const_cands = pd.DataFrame(cand_meta_rows)
    if not df_const_cands.empty:
        df_const_cands_wide = df_const_cands.pivot_table(
            index=["maz", "evk"],
            columns="col",
            values="candidate_name",
            aggfunc="first",
        ).reset_index()
    else:
        df_const_cands_wide = pd.DataFrame(columns=["maz", "evk"])

    df_const = df_const_ids.merge(df_const_cands_wide, on=["maz", "evk"], how="left")
    return df_const


def build_df_from_all_pairs(
    telep,
    egyeni_list,
    listak_list,
    progress_placeholder,
    bar,
    log,
    test_mode=False,
    test_limit=50,
):
    cand_meta = {
        c["ej_id"]: {
            "jlcs_nev": c.get("jlcs_nev", "UNKNOWN"),
        }
        for c in egyeni_list
    }

    list_meta = {
        l["tl_id"]: {
            "jlcs_nev": l.get("jlcs_nev", "UNKNOWN"),
            "lista_tip": l.get("lista_tip", "X"),
        }
        for l in listak_list
    }

    df_constituencies = build_constituency_id_mapping(egyeni_list)

    pairs = sorted({(row["maz"], row["taz"]) for row in telep})
    if test_mode:
        pairs = pairs[:test_limit]

    szk_rows = []
    base_rows = []
    cand_party_rows = []
    list_rows = []

    sample_szk_raw = None
    sample_jkv_raw = None

    total = len(pairs) if pairs else 1

    for idx, (maz, taz) in enumerate(pairs, start=1):
        progress_placeholder.text(
            f"Processing maz={maz}, taz={taz} ({idx}/{total})"
            + (" [TEST MODE]" if test_mode else "")
        )

        # Szavazokorok
        szk_url = f"{VER_BASE}/{maz}/Szavazokorok-{maz}-{taz}.json"
        szk_data = fetch_json_with_log(szk_url, log)
        if szk_data is None:
            bar.progress(idx / total)
            continue

        if sample_szk_raw is None:
            sample_szk_raw = szk_data

        szk_data_inner = szk_data.get("data", szk_data)
        szk_stations = szk_data_inner.get("szavazokorok", [])

        evk_map = {}

        for sz in szk_stations:
            sorsz = sz["sorszam"]
            evk_value = sz.get("evk", "")
            evk_map[sorsz] = evk_value

            row = {
                "maz": maz,
                "taz": taz,
                "sorsz": sorsz,
                "szk_nev": sz.get("szk_nev", ""),
                "evk": evk_value,
                "evk_nev": sz.get("evk_nev", ""),
                "cim": sz.get("cim", ""),
                "akadaly": sz.get("akadaly", 0),
                "szamlKijelolt": sz.get("szamlKijelolt", 0),
                "atjKijelolt": sz.get("atjKijelolt", 0),
                "telepSzintu": sz.get("telepSzintu", 0),
            }
            letszam = sz.get("letszam", {})
            for k, v in letszam.items():
                row[f"letszam_{k}"] = v
            szk_rows.append(row)

        # SzavkorJkv
        jkv_url = f"{SZAVOSSZ_BASE}/{maz}/SzavkorJkv-{maz}-{taz}.json"
        jkv_data = fetch_json_with_log(jkv_url, log)
        if jkv_data is None:
            bar.progress(idx / total)
            continue

        if sample_jkv_raw is None:
            sample_jkv_raw = jkv_data

        for rec in jkv_data.get("list", []):
            sorsz = rec["sorsz"]
            evk_value = evk_map.get(sorsz, "")

            key = {
                "maz": rec["maz"],
                "taz": rec["taz"],
                "sorsz": sorsz,
                "evk": evk_value,
            }
            ej = rec["egyeni_jkv"]
            li = rec["listas_jkv"]

            base_row = {
                **key,
                "vp_osszes_egyeni": ej.get("vp_osszes", 0),
                "szavazott_osszesen_egyeni": ej.get("szavazott_osszesen", 0),
                "szavazott_osszesen_szaz_egyeni": ej.get("szavazott_osszesen_szaz", 0.0),
                "szl_ervenyes_egyeni": ej.get("szl_ervenyes", 0),
                "szl_ervenytelen_egyeni": ej.get("szl_ervenytelen", 0),
                "vp_osszes_lista": li.get("vp_osszes", 0),
                "szavazott_osszesen_lista": li.get("szavazott_osszesen", 0),
                "szavazott_osszesen_szaz_lista": li.get("szavazott_osszesen_szaz", 0.0),
                "szl_ervenyes_lista": li.get("szl_ervenyes", 0),
                "szl_ervenytelen_lista": li.get("szl_ervenytelen", 0),
            }
            base_rows.append(base_row)

            for t in ej.get("tetelek", []):
                ej_id = t["ej_id"]
                votes = t.get("szavazat", 0)
                meta = cand_meta.get(ej_id, {})
                jlcs_nev = meta.get("jlcs_nev", "UNKNOWN")
            
                canon_name = canonical_party_name(jlcs_nev)
                party_slug = slugify(canon_name)
                party_col = f"votes_individual_party_{party_slug}"
            
                cand_party_rows.append(
                    {
                        **key,
                        "party_col": party_col,
                        "votes": votes,
                    }
                )

            for t in li.get("tetelek", []):
                tl_id = t["tl_id"]
                votes = t.get("szavazat", 0)
                meta = list_meta.get(tl_id, {})
                jlcs_nev = meta.get("jlcs_nev", "UNKNOWN")
                lista_tip = meta.get("lista_tip", "X")
                type_map = {"K": "comp", "O": "party", "N": "minority"}
                list_type = type_map.get(lista_tip, lista_tip.lower())
            
                canon_name = canonical_party_name(jlcs_nev)
                list_slug = slugify(canon_name)
                list_col = f"votes_list_{list_type}_{list_slug}"
            
                list_rows.append(
                    {
                        **key,
                        "list_col": list_col,
                        "votes": votes,
                    }
                )

        bar.progress(idx / total)

    # DataFrame construction
    df_szk = pd.DataFrame(szk_rows)
    df_base = pd.DataFrame(base_rows)

    df_cand_long = pd.DataFrame(cand_party_rows)
    if not df_cand_long.empty:
        df_cand_wide = df_cand_long.pivot_table(
            index=["maz", "taz", "sorsz", "evk"],
            columns="party_col",
            values="votes",
            aggfunc="sum",
        ).reset_index()
    else:
        df_cand_wide = pd.DataFrame()

    df_list_long = pd.DataFrame(list_rows)
    if not df_list_long.empty:
        df_list_wide = df_list_long.pivot_table(
            index=["maz", "taz", "sorsz", "evk"],
            columns="list_col",
            values="votes",
            aggfunc="sum",
        ).reset_index()
    else:
        df_list_wide = pd.DataFrame()

    if df_szk.empty:
        return pd.DataFrame(), pd.DataFrame(), sample_szk_raw, sample_jkv_raw

    df_results = df_szk.merge(
        df_base, on=["maz", "taz", "sorsz", "evk"], how="left"
    )

    if not df_cand_wide.empty:
        df_results = df_results.merge(
            df_cand_wide, on=["maz", "taz", "sorsz", "evk"], how="left"
        )

    if not df_list_wide.empty:
        df_results = df_results.merge(
            df_list_wide, on=["maz", "taz", "sorsz", "evk"], how="left"
        )

    info_cols = [
        "maz",
        "taz",
        "sorsz",
        "szk_nev",
        "evk",
        "evk_nev",
        "cim",
        "akadaly",
        "szamlKijelolt",
        "atjKijelolt",
        "telepSzintu",
    ]
    info_cols += [c for c in df_results.columns if c.startswith("letszam_")]
    info_cols += [
        "vp_osszes_egyeni",
        "szavazott_osszesen_egyeni",
        "szavazott_osszesen_szaz_egyeni",
        "szl_ervenyes_egyeni",
        "szl_ervenytelen_egyeni",
        "vp_osszes_lista",
        "szavazott_osszesen_lista",
        "szavazott_osszesen_szaz_lista",
        "szl_ervenyes_lista",
        "szl_ervenytelen_lista",
    ]
    info_cols = [c for c in info_cols if c in df_results.columns]

    df_info = df_results[info_cols].copy()

    df_results = df_results.merge(
        df_constituencies, on=["maz", "evk"], how="left"
    )
    df_info = df_info.merge(
        df_constituencies, on=["maz", "evk"], how="left"
    )

    df_info_rename_map = {
        "szk_nev": "polling_station_name",
        "evk": "constituency_code",
        "evk_nev": "constituency_name",
        "cim": "polling_station_address",
        "akadaly": "accessible_for_disabled",
        "szamlKijelolt": "designated_counting_station",
        "atjKijelolt": "designated_transfer_station",
        "telepSzintu": "municipality_level_station",
        "letszam_indulo": "electorate_initial",
        "letszam_honos": "electorate_resident",
        "letszam_atjel": "electorate_transferred_in",
        "letszam_atjelInnen": "electorate_transferred_out",
        "letszam_osszesen": "electorate_total",
        "vp_osszes_egyeni": "eligible_voters_individual",
        "szavazott_osszesen_egyeni": "turnout_individual",
        "szavazott_osszesen_szaz_egyeni": "turnout_rate_pct_individual",
        "szl_ervenyes_egyeni": "valid_votes_individual",
        "szl_ervenytelen_egyeni": "invalid_votes_individual",
        "vp_osszes_lista": "eligible_voters_list",
        "szavazott_osszesen_lista": "turnout_list",
        "szavazott_osszesen_szaz_lista": "turnout_rate_pct_list",
        "szl_ervenyes_lista": "valid_votes_list",
        "szl_ervenytelen_lista": "invalid_votes_list",
    }
    # df_info = df_info.rename(columns=df_info_rename_map)
    df_info = df_info.rename(columns=COLUMN_RENAME_MAP)
    df_results = df_results.rename(columns=COLUMN_RENAME_MAP
                                   
    return df_results, df_info, sample_szk_raw, sample_jkv_raw


# -------------------------------------------------------------------
# Streamlit UI
# -------------------------------------------------------------------

st.title("Hungary 2022 Parliamentary Election – Polling Station Scraper")

st.write(
    "This app scrapes polling-station level data from vtr.valasztas.hu for OGY 2022 "
    "and builds two CSVs:\n"
    "- polling_station_results.csv: station info, results by party (individual), "
    "list results, candidate names, constituency_id\n"
    "- polling_station_info.csv: station info, electorate, turnout, constituency_id"
)

test_mode = st.checkbox("Test mode (only first 50 municipalities)")

if st.button("Scrape and build CSVs"):
    progress_text = st.empty()
    progress_bar = st.progress(0.0)

    # HTTP log
    http_log = []

    # 1) global meta
    progress_text.text("Downloading global metadata (Telepulesek, candidates, lists)...")
    telep, egyeni_list, listak_list, jlcs, szervezetek = load_global_meta(http_log)
    progress_bar.progress(0.05)

    # 2) main scrape
    df_results, df_info, sample_szk_raw, sample_jkv_raw = build_df_from_all_pairs(
        telep,
        egyeni_list,
        listak_list,
        progress_text,
        progress_bar,
        log=http_log,
        test_mode=test_mode,
        test_limit=50,
    )
    progress_bar.progress(1.0)
    progress_text.text("Scraping completed.")

    # ---- HTTP LOG PREVIEW (this is what you need to debug URL/network issues) ----
    st.subheader("HTTP request log")
    if http_log:
        st.dataframe(pd.DataFrame(http_log))
    else:
        st.write("No HTTP requests recorded – something is wrong before fetching.")

    # ---- RAW JSON PREVIEW ----
    st.subheader("Raw JSON preview")

    if telep:
        with st.expander("Raw JSON: Telepulesek.json (first entry)"):
            st.json(telep[0])

    if sample_szk_raw is not None:
        with st.expander("Raw JSON: first Szavazokorok-maz-taz.json processed"):
            st.json(sample_szk_raw)

    if sample_jkv_raw is not None:
        with st.expander("Raw JSON: first SzavkorJkv-maz-taz.json processed"):
            st.json(sample_jkv_raw)

    # ---- DATAFRAME PREVIEWS / DOWNLOADS ----
    if df_results.empty:
        st.error("df_results is empty. Check the HTTP log above for failing URLs/status codes.")
    else:
        st.subheader("Preview: polling_station_results (first 10 rows)")
        st.dataframe(df_results.head(10))

        st.subheader("Preview: polling_station_info (first 10 rows)")
        st.dataframe(df_info.head(10))

        buf_results = io.StringIO()
        df_results.to_csv(buf_results, index=False)
        csv_results = buf_results.getvalue()

        buf_info = io.StringIO()
        df_info.to_csv(buf_info, index=False)
        csv_info = buf_info.getvalue()

        st.download_button(
            label="Download polling_station_results.csv",
            data=csv_results,
            file_name="polling_station_results.csv",
            mime="text/csv",
        )

        st.download_button(
            label="Download polling_station_info.csv",
            data=csv_info,
            file_name="polling_station_info.csv",
            mime="text/csv",
        )
