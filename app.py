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
    """
    Simple slug for column names: lowercase, replace spaces and dashes,
    remove some punctuation.
    """
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


@st.cache_data(show_spinner=False)
def fetch_json(url: str):
    resp = requests.get(url, headers={"User-Agent": "ogy2022-streamlit-scraper"})
    resp.raise_for_status()
    return resp.json()


@st.cache_data(show_spinner=False)
def load_global_meta():
    """Download and cache global metadata JSONs."""
    telep = fetch_json(f"{VER_BASE}/Telepulesek.json")["list"]
    egyeni = fetch_json(f"{VER_BASE}/EgyeniJeloltek.json")["list"]
    listak = fetch_json(f"{VER_BASE}/ListakEsJeloltek.json")["list"]
    # not strictly needed for CSVs but useful for raw preview/context
    jlcs = fetch_json(f"{VER_BASE}/Jlcs.json")["list"]
    szervezetek = fetch_json(f"{VER_BASE}/Szervezetek.json")["list"]
    return telep, egyeni, listak, jlcs, szervezetek


def build_constituency_id_mapping(egyeni_list):
    """
    Build:
      - mapping (maz, evk) -> constituency_id (same for all areas with same candidate set)
      - candidate name per party per constituency for later columns.
    """
    cand_by_const = defaultdict(set)
    cand_meta_rows = []  # for candidate names per party

    for c in egyeni_list:
        maz = c["maz"]
        evk = c["evk"]
        ej_id = c["ej_id"]
        jlcs_nev = c.get("jlcs_nev", "UNKNOWN")
        name = c.get("neve", "")
        cand_by_const[(maz, evk)].add(ej_id)

        party_slug = slugify(jlcs_nev)
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

    # map each unique candidate set to an integer constituency_id
    candidate_sets = {}
    constituency_id_by_const = {}
    next_id = 1

    for (maz, evk), ej_set in sorted(cand_by_const.items()):
        sig = tuple(sorted(ej_set))
        if sig not in candidate_sets:
            candidate_sets[sig] = next_id
            next_id += 1
        constituency_id_by_const[(maz, evk)] = candidate_sets[sig]

    # build constituency dataframe
    const_rows = []
    for (maz, evk), cid in constituency_id_by_const.items():
        const_rows.append({"maz": maz, "evk": evk, "constituency_id": cid})
    df_const_ids = pd.DataFrame(const_rows)

    # build candidate-name-wide table
    df_const_cands = pd.DataFrame(cand_meta_rows)
    if not df_const_cands.empty:
        df_const_cands_wide = df_const_cands.pivot_table(
            index=["maz", "evk"],
            columns="col",
            values="candidate_name",
            aggfunc="first",
        )
        df_const_cands_wide = df_const_cands_wide.reset_index()
    else:
        df_const_cands_wide = pd.DataFrame(columns=["maz", "evk"])

    # merge constituency_id into candidate-name table
    df_const = df_const_ids.merge(df_const_cands_wide, on=["maz", "evk"], how="left")

    return df_const  # columns: maz, evk, constituency_id, candidate_*_name...


def build_df_from_all_pairs(
    telep,
    egyeni_list,
    listak_list,
    progress_placeholder,
    bar,
    test_mode=False,
    test_limit=50,
):
    """
    Core scraping + dataframe construction:
      - iterates over all (maz, taz) pairs in Telepulesek
      - fetches Szavazokorok-{maz}-{taz}.json and SzavkorJkv-{maz}-{taz}.json
      - builds df_results and df_info (pure column merges)
      - also returns first raw szk and jkv jsons for preview
      - if test_mode=True, only uses first `test_limit` (maz, taz) pairs
    """
    # candidate metadata dict (by ej_id)
    cand_meta = {
        c["ej_id"]: {
            "jlcs_nev": c.get("jlcs_nev", "UNKNOWN"),
        }
        for c in egyeni_list
    }

    # list metadata dict (by tl_id)
    list_meta = {
        l["tl_id"]: {
            "jlcs_nev": l.get("jlcs_nev", "UNKNOWN"),
            "lista_tip": l.get("lista_tip", "X"),
        }
        for l in listak_list
    }

    # mapping (maz, evk) -> constituency_id + candidate name columns
    df_constituencies = build_constituency_id_mapping(egyeni_list)

    # unique (maz, taz) pairs
    pairs = sorted({(row["maz"], row["taz"]) for row in telep})
    if test_mode:
        pairs = pairs[:test_limit]

    szk_rows = []        # polling station info
    base_rows = []       # aggregate turnout / vp data
    cand_party_rows = [] # individual ballot by party (per polling station)
    list_rows = []       # list ballot by list (per polling station)

    sample_szk_raw = None
    sample_jkv_raw = None

    total = len(pairs) if pairs else 1

    for idx, (maz, taz) in enumerate(pairs, start=1):
        progress_placeholder.text(
            f"Processing maz={maz}, taz={taz} ({idx}/{total})"
            + (" [TEST MODE]" if test_mode else "")
        )

        # Szavazokorok: polling stations + electorate
        szk_url = f"{VER_BASE}/Szavazokorok-{maz}-{taz}.json"
        try:
            szk_data = fetch_json(szk_url)
        except Exception:
            bar.progress(idx / total)
            continue

        if sample_szk_raw is None:
            sample_szk_raw = szk_data

        szk_data_inner = szk_data.get("data", szk_data)
        szk_stations = szk_data_inner.get("szavazokorok", [])

        # build quick lookup of evk for this (maz, taz, sorsz)
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

        # SzavkorJkv: results per station
        jkv_url = f"{SZAVOSSZ_BASE}/{maz}/SzavkorJkv-{maz}-{taz}.json"
        try:
            jkv_data = fetch_json(jkv_url)
        except Exception:
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

            # base turnout / validity
            base_row = {
                **key,
                "vp_osszes_egyeni": ej.get("vp_osszes", 0),
                "szavazott_osszesen_egyени": ej.get("szavazott_osszesen", 0),
                "szavazott_osszesen_szaz_egyени": ej.get("szavazott_osszesen_szaz", 0.0),
                "szl_ervenyes_egyени": ej.get("szl_ervenyes", 0),
                "szl_ervenytelen_egyени": ej.get("szl_ervenytelen", 0),
                "vp_osszes_lista": li.get("vp_osszes", 0),
                "szavazott_osszesen_lista": li.get("szavazott_osszesen", 0),
                "szavazott_osszesen_szaz_lista": li.get("szavazott_osszesen_szaz", 0.0),
                "szl_ervenyes_lista": li.get("szl_ervenyes", 0),
                "szl_ervenytelen_lista": li.get("szl_ervenytelen", 0),
            }
            # normalize possible stray unicode
            base_row = {
                (k.replace("egyени", "egyeni") if isinstance(k, str) else k): v
                for k, v in base_row.items()
            }
            base_rows.append(base_row)

            # individual ballot: aggregate by party (jlcs_nev)
            for t in ej.get("tetelek", []):
                ej_id = t["ej_id"]
                votes = t.get("szavazat", 0)
                meta = cand_meta.get(ej_id, {})
                jlcs_nev = meta.get("jlcs_nev", "UNKNOWN")
                party_slug = slugify(jlcs_nev)
                party_col = f"votes_individual_party_{party_slug}"
                cand_party_rows.append(
                    {
                        **key,
                        "party_col": party_col,
                        "votes": votes,
                    }
                )

            # list ballot: per list (party / minority list)
            for t in li.get("tetelek", []):
                tl_id = t["tl_id"]
                votes = t.get("szavazat", 0)
                meta = list_meta.get(tl_id, {})
                jlcs_nev = meta.get("jlcs_nev", "UNKNOWN")
                lista_tip = meta.get("lista_tip", "X")
                type_map = {"K": "comp", "O": "party", "N": "minority"}
                list_type = type_map.get(lista_tip, lista_tip.lower())
                list_slug = slugify(jlcs_nev)
                list_col = f"votes_list_{list_type}_{list_slug}"
                list_rows.append(
                    {
                        **key,
                        "list_col": list_col,
                        "votes": votes,
                    }
                )

        bar.progress(idx / total)

    # ---------------------------
    # Build dataframes (column merges only)
    # ---------------------------
    df_szk = pd.DataFrame(szk_rows)    # station info + electorate
    df_base = pd.DataFrame(base_rows)  # turnout etc.

    # individual ballot by party
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

    # list ballot by list
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

    # merge base turnout
    df_results = df_szk.merge(
        df_base, on=["maz", "taz", "sorsz", "evk"], how="left"
    )

    # merge individual by-party votes
    if not df_cand_wide.empty:
        df_results = df_results.merge(
            df_cand_wide, on=["maz", "taz", "sorsz", "evk"], how="left"
        )

    # merge list votes
    if not df_list_wide.empty:
        df_results = df_results.merge(
            df_list_wide, on=["maz", "taz", "sorsz", "evk"], how="left"
        )

    # ---------------------------
    # df_info = station info + electorate + turnout, no vote-by-party/list
    # ---------------------------
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
        "szavazott_osszesen_egyени",
        "szavazott_osszesen_szaz_egyени",
        "szl_ervenyes_egyени",
        "szl_ervenytelen_egyени",
        "vp_osszes_lista",
        "szavazott_osszesen_lista",
        "szavazott_osszesen_szaz_lista",
        "szl_ervenyes_lista",
        "szl_ervenytelen_lista",
    ]
    info_cols = [c.replace("egyени", "egyeni") for c in info_cols]
    info_cols = [c for c in info_cols if c in df_results.columns]

    df_info = df_results[info_cols].copy()

    # ---------------------------
    # Add constituency_id and candidate name columns (per party)
    # ---------------------------
    df_results = df_results.merge(
        df_constituencies, on=["maz", "evk"], how="left"
    )
    df_info = df_info.merge(
        df_constituencies, on=["maz", "evk"], how="left"
    )

    # ---------------------------
    # English renaming for df_info basic columns
    # ---------------------------
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
        "szavazott_osszesen_egyени": "turnout_individual",
        "szavazott_osszesen_szaz_egyени": "turnout_rate_pct_individual",
        "szl_ervenyes_egyени": "valid_votes_individual",
        "szl_ervenytelen_egyени": "invalid_votes_individual",
        "vp_osszes_lista": "eligible_voters_list",
        "szavazott_osszesen_lista": "turnout_list",
        "szavazott_osszesen_szaz_lista": "turnout_rate_pct_list",
        "szl_ervenyes_lista": "valid_votes_list",
        "szl_ervenytelen_lista": "invalid_votes_list",
    }
    df_info_rename_map = {
        k.replace("egyени", "egyeni"): v for k, v in df_info_rename_map.items()
    }

    df_info = df_info.rename(columns=df_info_rename_map)

    return df_results, df_info, sample_szk_raw, sample_jkv_raw


# -------------------------------------------------------------------
# Streamlit UI
# -------------------------------------------------------------------

st.title("Hungary 2022 Parliamentary Election – Polling Station Scraper")

st.write(
    "This app scrapes polling-station level data (all counties/municipalities) "
    "from vtr.valasztas.hu for OGY 2022 and builds two CSVs:\n"
    "- polling_station_results.csv: station info, results by party (individual), "
    "list results, candidate names, constituency_id\n"
    "- polling_station_info.csv: station info, electorate, turnout, constituency_id"
)

test_mode = st.checkbox("Test mode (only first 50 municipalities)")

if st.button("Scrape and build CSVs"):
    progress_text = st.empty()
    progress_bar = st.progress(0.0)

    # 1) load global metadata
    progress_text.text("Downloading global metadata (Telepulesek, candidates, lists)...")
    telep, egyeni_list, listak_list, jlcs, szervezetek = load_global_meta()
    progress_bar.progress(0.05)

    # 2) scrape all (maz, taz) and build dataframes + sample raw JSONs
    df_results, df_info, sample_szk_raw, sample_jkv_raw = build_df_from_all_pairs(
        telep,
        egyeni_list,
        listak_list,
        progress_text,
        progress_bar,
        test_mode=test_mode,
        test_limit=50,
    )
    progress_bar.progress(1.0)
    progress_text.text("Done building dataframes.")

    if df_results.empty:
        st.error("No data could be built (df_results is empty). Check network access or URL paths.")
    else:
        # ---- RAW JSON PREVIEW ----
        st.subheader("Raw JSON preview")

        # global Telepulesek preview
        if telep:
            with st.expander("Raw JSON: Telepulesek.json (first entry)"):
                st.json(telep[0])

        # first Szavazokorok and SzavkorJkv actually processed in the scraping loop
        if sample_szk_raw is not None:
            with st.expander("Raw JSON: first Szavazokorok-maz-taz.json processed"):
                st.json(sample_szk_raw)

        if sample_jkv_raw is not None:
            with st.expander("Raw JSON: first SzavkorJkv-maz-taz.json processed"):
                st.json(sample_jkv_raw)

        # ---- DATAFRAME PREVIEWS ----
        st.subheader("Preview: polling_station_results (first 10 rows)")
        st.dataframe(df_results.head(10))

        st.subheader("Preview: polling_station_info (first 10 rows)")
        st.dataframe(df_info.head(10))

        # 3) prepare CSVs for download
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
