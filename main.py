import streamlit as st
import pandas as pd
import json
from sqlalchemy import create_engine, text
from datetime import datetime
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
from geopy.distance import geodesic
from collections import Counter
import pydeck as pdk
import requests
import numpy as np
from ortools.constraint_solver import routing_enums_pb2, pywrapcp

def project_to_meters(lons, lats):
    lat0 = np.mean(lats)
    m_lat = 111_320
    m_lon = 111_320 * np.cos(np.deg2rad(lat0))
    x = (lons - np.mean(lons)) * m_lon
    y = (lats - lat0) * m_lat
    return np.vstack([x, y]).T

def farthest_point_seeds_indices(coords, k):
    n = coords.shape[0]
    seeds = [np.random.randint(n)]
    for _ in range(1, k):
        d = np.min([np.linalg.norm(coords - coords[s], axis=1) for s in seeds], axis=0)
        seeds.append(int(np.argmax(d)))
    return seeds

def capacity_balance(coords, labels, caps):
    k = len(caps)
    sizes = np.array([np.sum(labels==i) for i in range(k)])
    cents = np.vstack([coords[labels==i].mean(axis=0) if sizes[i]>0 else np.zeros(2) for i in range(k)])
    pool = []
    for i in range(k):
        over = sizes[i] - caps[i]
        if over>0:
            idxs = np.where(labels==i)[0]
            dists = np.linalg.norm(coords[idxs]-cents[i], axis=1)
            remove = idxs[np.argsort(dists)[-over:]]
            labels[remove] = -1
            pool.extend(remove.tolist())
    if pool:
        pool = np.array(pool)
        dmat = np.linalg.norm(coords[pool,None,:] - cents[None,:,:], axis=2)
        for i in range(k):
            need = caps[i] - np.sum(labels==i)
            if need<=0: continue
            order = np.argsort(dmat[:,i])[:need]
            labels[pool[order]] = i
            mask = np.ones(len(pool),bool)
            mask[order]=False
            pool=pool[mask]
            dmat=dmat[mask]
            if pool.size==0: break
    return labels
## ─── LOGIN ───────────────────────────────────────
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

def do_login():
    st.markdown("## 🔐 Log in om toegang te krijgen")
    username = st.text_input("Gebruikersnaam", key="login_user")
    password = st.text_input("Wachtwoord", type="password", key="login_pass")
    if st.button("Inloggen"):
        creds = st.secrets["credentials"]
        if username == creds["username"] and password == creds["password"]:
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("❌ Ongeldige gebruikersnaam of wachtwoord")

if not st.session_state.authenticated:
    do_login()
    st.stop()

# ─── GEBRUIKER KEUZE ─────────────────────────────
if st.session_state.authenticated and st.session_state.get("gebruiker") is None:
    with st.sidebar:
        st.header("👤 Kies je gebruiker")
        temp = st.selectbox("Gebruiker", ["Delft", "Den Haag"], key="temp_gebruiker")
        if st.button("Bevestig gebruiker"):
            st.session_state.gebruiker = temp
            st.success(f"✅ Ingeset als gebruiker: {temp}")
            st.rerun()
    st.stop()

# ─── DATABASE ────────────────────────────────────
@st.cache_resource
def get_engine():
    config = st.secrets["postgres"]
    db_url = (
        f"postgresql+psycopg2://{config['user']}:{config['password']}"
        f"@{config['host']}:{config['port']}/{config['dbname']}"
    )
    return create_engine(db_url)

# ─── GECACHEDE QUERIES ──────────────────────────
@st.cache_data(ttl=300)
def get_df_sidebar():
    df = run_query("SELECT * FROM apb_containers")
    df["fill_level"] = pd.to_numeric(df["fill_level"], errors="coerce")
    df["extra_meegegeven"] = df["extra_meegegeven"].astype(bool)
    return df

@st.cache_data(ttl=300)
def get_df_routes():
    return run_query("""
        SELECT r.route_omschrijving, r.omschrijving AS container_name,
               r.datum, c.container_location, c.content_type
        FROM apb_routes r
        JOIN apb_containers c ON r.omschrijving = c.container_name
        WHERE r.datum >= current_date AND c.container_location IS NOT NULL
    """)

@st.cache_data(ttl=300)
def get_df_containers():
    # Alleen containers ingelezen vandaag of later
    return run_query("""
        SELECT container_name, container_location, content_type, fill_level, address, city
        FROM apb_containers
        WHERE datum_ingelezen >= current_date
    """)

def run_query(query, params=None):
    with get_engine().connect() as conn:
        return pd.read_sql(text(query), conn, params=params)

def execute_query(query, params=None):
    with get_engine().begin() as conn:
        conn.execute(text(query), params or {})

# ─── PAGINA INSTELLINGEN ────────────────────────
st.set_page_config(page_title="Afvalcontainerbeheer", layout="wide")
st.title("♻️ Afvalcontainerbeheer Dashboard")

# ─── SESSIESTATE INITIALISATIE ───────────────────
def init_session_state():
    defaults = {
        "op_route": False,
        "selected_types": [],
        "refresh_needed": False,
        "extra_meegegeven_tijdelijk": [],
        "geselecteerde_routes": [],
        "gebruiker": st.session_state.get("gebruiker")
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

init_session_state()

## ─── SIDEBAR ─────────────────────────────────────
with st.sidebar:
    st.header("🔧 Instellingen")
    rol = st.selectbox("👤 Kies je rol:", ["Gebruiker", "Upload"])
    st.markdown(f"**Ingelogd als:** {st.session_state.gebruiker}")

    # Cache vernieuwen als nodig
    try:
        if st.session_state.refresh_needed:
            st.cache_data.clear()
            st.session_state.refresh_needed = False

        df_sidebar = get_df_sidebar()
    except Exception as e:
        st.error(f"❌ Fout bij laden van containerdata: {e}")
        df_sidebar = pd.DataFrame()

    if rol == "Gebruiker":
        st.markdown("### 🔎 Filters")
        # Content type filter as checkboxes in an expander
        types = sorted(df_sidebar["content_type"].dropna().unique())
        # Default: geen types geselecteerd bij opstarten
        if "selected_types" not in st.session_state:
            st.session_state.selected_types = []
        with st.expander("Content types", expanded=True):
            selected_types = []
            for t in types:
                checked = st.checkbox(
                    label=t,
                    value=(t in st.session_state.selected_types),
                    key=f"cb_type_{t}"
                )
                if checked:
                    selected_types.append(t)
            st.session_state.selected_types = selected_types

        st.markdown("### 🚚 Routeselectie")
        try:
            df_routes_full = get_df_routes()
            if not df_routes_full.empty:
                # Groepeer en tel het aantal containers per routeomschrijving
                route_counts = df_routes_full["route_omschrijving"].value_counts().to_dict()

                # Maak een lijst met labels zoals "Route A (12)"
                beschikbare_routes = sorted(route_counts.items())  # lijst van (route, count)
                label_to_route = {f"{route} ({count})": route for route, count in beschikbare_routes}

                # Toon checkboxen met labels
                with st.expander("Selecteer routes", expanded=True):
                    geselecteerde = []
                    for label, route in label_to_route.items():
                        checked = st.checkbox(
                            label=label,
                            value=(route in st.session_state.geselecteerde_routes),
                            key=f"cb_route_{route}"
                        )
                        if checked:
                            geselecteerde.append(route)
                    st.session_state.geselecteerde_routes = geselecteerde

                    if checked:
                            geselecteerde.append(route)
                    st.session_state.geselecteerde_routes = geselecteerde
            else:
                st.info("📬 Geen routes van vandaag of later beschikbaar. Upload eerst data.")
        except Exception as e:
            st.error(f"❌ Fout bij ophalen van routes: {e}")
            pass


    elif rol == "Upload":

        st.markdown("### 📤 Upload bestanden")

        file1 = st.file_uploader("🟢 Bestand van Abel", type=["xlsx"], key="upload_abel")

        file2 = st.file_uploader("🔵 Bestand van Pieterbas", type=["xlsx"], key="upload_pb")

        process = st.button("🗄️ Verwerk en laad data")

        if process and file1 and file2:

            try:

                # 1) Leeg de cache

                st.cache_data.clear()

                # 2) Lees en verwerk de uploads

                df1 = pd.read_excel(file1)

                df1.columns = df1.columns.str.strip().str.lower().str.replace(" ", "_")

                df1.rename(columns={"fill_level_(%)": "fill_level"}, inplace=True)

                df2 = pd.read_excel(file2)

                df1['operational_state'] = df1['operational_state'].astype(str).str.strip().str.lower()

                df1 = df1[
                    (df1['operational_state'].isin(['in use', 'issue detected'])) &
                    (df1['status'].str.strip().str.lower() == 'in use') &
                    (df1['on_hold'].str.strip().str.lower() == 'no')
                    ].copy()

                df1["content_type"] = df1["content_type"].apply(

                    lambda x: "Glas" if "glass" in str(x).lower() else x

                )

                df1["combinatietelling"] = df1.groupby(

                    ["location_code", "content_type"]

                )["content_type"].transform("count")

                df1["gemiddeldevulgraad"] = df1.groupby(

                    ["location_code", "content_type"]

                )["fill_level"].transform("mean")

                df1["oproute"] = df1["container_name"].isin(df2["Omschrijving"]).map(

                    {True: "Ja", False: "Nee"}

                )

                df1["extra_meegegeven"] = False

                cols = [

                    "container_name", "address", "city", "location_code", "content_type",

                    "fill_level", "container_location", "combinatietelling",

                    "gemiddeldevulgraad", "oproute", "extra_meegegeven"

                ]

                df1 = df1[cols]

                df1["datum_ingelezen"] = datetime.now().date()

                engine = get_engine()

                with engine.begin() as conn:

                    conn.execute(text("TRUNCATE TABLE apb_containers RESTART IDENTITY"))

                df1.to_sql("apb_containers", engine, if_exists="append", index=False)

                df2 = df2.rename(columns={

                    "Route Omschrijving": "route_omschrijving",

                    "Omschrijving": "omschrijving",

                    "Datum": "datum"

                })[["route_omschrijving", "omschrijving", "datum"]].drop_duplicates()

                with engine.begin() as conn:

                    conn.execute(text("TRUNCATE TABLE apb_routes RESTART IDENTITY"))

                df2.to_sql("apb_routes", engine, if_exists="append", index=False)

                # Markeer voor herladen in hoofd-app

                st.session_state.refresh_needed = True

                st.success("✅ Gegevens succesvol geüpload en cache vernieuwd.")

            except Exception as e:

                st.error(f"❌ Fout bij verwerken van bestanden: {e}")



# ─── TABS ─────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Dashboard", "🗺️ Kaartweergave", "📋 Route-status", "🚀 Optimalisatie"
])


# ─── TAB 1: DASHBOARD ────────────────────────────
with tab1:
    df = df_sidebar.copy()

    if st.session_state.refresh_needed:
        df = run_query("""
            SELECT *
            FROM apb_containers
            WHERE datum_ingelezen::date = CURRENT_DATE
        """)
        st.session_state.refresh_needed = False

    df["fill_level"] = pd.to_numeric(df["fill_level"], errors="coerce")
    df["extra_meegegeven"] = df["extra_meegegeven"].astype(bool)

    # KPI's
    try:
        df_logboek = run_query("SELECT gebruiker FROM apb_logboek_afvalcontainers WHERE datum >= current_date")
        counts = df_logboek["gebruiker"].value_counts().to_dict()
        delft_count = counts.get("Delft", 0)
        denhaag_count = counts.get("Den Haag", 0)
    except:
        delft_count = denhaag_count = 0

    k1, k2, k3 = st.columns(3)
    k1.metric("\U0001F4E6 Totaal containers", len(df))
    k2.metric("\U0001F4CA Vulgraad ≥ 80%", (df["fill_level"] >= 80).sum())
    k3.metric("🧝 Extra meegegeven (Delft / Den Haag)", f"{delft_count} / {denhaag_count}")

    # Zoekfilters
    st.subheader("✍️ Bewerkbare containers")
    with st.expander("🔍 Zoekfilters"):
        col1, col2 = st.columns(2)
        with col1:
            zoek_naam = st.text_input("🔤 Zoek op container_name").strip().lower()
        with col2:
            zoek_straat = st.text_input("📍 Zoek op address").strip().lower()

    # Start met alle containers die nog niet extra zijn meegegeven
    bewerkbaar = df[~df["extra_meegegeven"]].copy()

    # Zonder zoekopdracht -> filter op oproute en content_type uit sidebar
    if not zoek_naam and not zoek_straat:
        bewerkbaar = bewerkbaar[bewerkbaar["oproute"] == "Nee"]
        bewerkbaar = bewerkbaar[bewerkbaar["content_type"].isin(st.session_state.selected_types)]

    # Met zoekopdracht -> zoek door alles, ook op route
    if zoek_naam:
        bewerkbaar = bewerkbaar[bewerkbaar["container_name"].str.lower().str.contains(zoek_naam)]
    if zoek_straat:
        bewerkbaar = bewerkbaar[bewerkbaar["address"].str.lower().str.contains(zoek_straat)]

    # Sorteer altijd op content_type > gemiddeldevulgraad
    bewerkbaar = bewerkbaar.sort_values(["content_type", "gemiddeldevulgraad"], ascending=[True, False])

    zichtbaar = [
        "container_name", "address", "city", "location_code", "content_type",
        "fill_level", "combinatietelling", "gemiddeldevulgraad", "oproute", "extra_meegegeven"
    ]

    # Paginering
    if "page_bewerkbaar" not in st.session_state:
        st.session_state.page_bewerkbaar = 0

    containers_per_page = 25
    total_rows = len(bewerkbaar)
    total_pages = max(1, (total_rows - 1) // containers_per_page + 1)

    if st.session_state.page_bewerkbaar >= total_pages:
        st.session_state.page_bewerkbaar = total_pages - 1

    col1, col2, col3 = st.columns([1, 2, 8])
    with col1:
        if st.button("⬅️"):
            if st.session_state.page_bewerkbaar > 0:
                st.session_state.page_bewerkbaar -= 1
    with col2:
        if st.button("➡️"):
            if st.session_state.page_bewerkbaar < total_pages - 1:
                st.session_state.page_bewerkbaar += 1
    with col3:
        st.markdown(f"**Pagina {st.session_state.page_bewerkbaar + 1} van {total_pages}**")

    start_idx = st.session_state.page_bewerkbaar * containers_per_page
    end_idx = start_idx + containers_per_page
    paged = bewerkbaar.iloc[start_idx:end_idx]

    # AgGrid
    from st_aggrid import GridOptionsBuilder, AgGrid, GridUpdateMode

    gb = GridOptionsBuilder.from_dataframe(paged[zichtbaar])
    gb.configure_default_column(filter=True)
    gb.configure_column("extra_meegegeven", editable=True)
    grid = AgGrid(
        paged[zichtbaar],
        gridOptions=gb.build(),
        update_mode=GridUpdateMode.VALUE_CHANGED,
        height=500
    )

    updated = grid["data"].copy()
    updated["extra_meegegeven"] = updated["extra_meegegeven"].astype(bool)
    st.session_state.extra_meegegeven_tijdelijk = updated[updated["extra_meegegeven"]]["container_name"].tolist()

    # Wijzigingen toepassen en loggen
    if st.button("✅ Wijzigingen toepassen en loggen"):
        gewijzigde = updated[updated["extra_meegegeven"]]
        if not gewijzigde.empty:
            try:
                df_log = run_query("SELECT container_name, datum FROM apb_logboek_afvalcontainers")
                df_log["datum"] = pd.to_datetime(df_log["datum"], errors="coerce")
            except:
                df_log = pd.DataFrame(columns=["container_name", "datum"])

            vandaag = datetime.now().date()
            count = 0
            for _, row in gewijzigde.iterrows():
                if ((df_log["container_name"] == row["container_name"]) &
                    (df_log["datum"].dt.date == vandaag)).any():
                    continue
                nm = row["container_name"].strip()
                execute_query(
                    "UPDATE apb_containers SET extra_meegegeven = TRUE WHERE TRIM(container_name) = :naam",
                    {"naam": nm}
                )
                execute_query("""
                    INSERT INTO apb_logboek_afvalcontainers
                    (container_name, address, city, location_code, content_type, fill_level, datum, gebruiker)
                    VALUES (:a, :b, :c, :d, :e, :f, :g, :h)
                """, {
                    "a": row["container_name"],
                    "b": row["address"],
                    "c": row["city"],
                    "d": row["location_code"],
                    "e": row["content_type"],
                    "f": row["fill_level"],
                    "g": datetime.now(),
                    "h": st.session_state.gebruiker
                })
                count += 1

            if count:
                st.success(f"✔️ {count} containers gelogd en bijgewerkt.")
                st.session_state.refresh_needed = True
                st.rerun()
            else:
                st.warning("⚠️ Geen nieuwe logs toegevoegd.")

    st.subheader("🔒 Reeds gemarkeerde containers")
    reeds = df[df["extra_meegegeven"]]
    st.dataframe(reeds[zichtbaar], use_container_width=True)



# ─── TAB 2: KAART ─────────────────────────────────
with tab2:
    st.subheader("🗺️ Containerkaart")

    @st.cache_data(ttl=300)
    def load_routes_for_map():
        df = run_query("""
            SELECT r.route_omschrijving, r.omschrijving AS container_name,
                   c.container_location, c.content_type, c.fill_level, c.address, c.city
            FROM apb_routes r
            JOIN apb_containers c ON r.omschrijving = c.container_name
            WHERE c.container_location IS NOT NULL
        """)
        df[["r_lat", "r_lon"]] = df["container_location"].str.split(",", expand=True)
        df["r_lat"] = pd.to_numeric(df["r_lat"], errors="coerce")
        df["r_lon"] = pd.to_numeric(df["r_lon"], errors="coerce")
        df = df.dropna(subset=["r_lat", "r_lon"])
        return df

    @st.cache_data(ttl=300)
    def load_all_containers():
        df = run_query("""
            SELECT container_name, container_location, content_type, fill_level, address, city
            FROM apb_containers
        """)
        df[["lat", "lon"]] = df["container_location"].str.split(",", expand=True).astype(float)
        return df

    df_routes = load_routes_for_map()
    df_containers = load_all_containers()
    sel_routes = st.session_state.geselecteerde_routes
    sel_names = st.session_state.extra_meegegeven_tijdelijk
    df_hand = df_containers[df_containers["container_name"].isin(sel_names)].copy()

    def find_nearest_route(r):
        if pd.isna(r["lat"]) or pd.isna(r["lon"]):
            return None
        radius = 0.15
        while True:
            matches = [
                rp["route_omschrijving"] for _, rp in df_routes.iterrows()
                if rp["content_type"] == r["content_type"]
                and geodesic((r["lat"], r["lon"]), (rp["r_lat"], rp["r_lon"])).km <= radius
            ]
            if matches:
                return Counter(matches).most_common(1)[0][0]
            radius += 0.1
            if radius > 5:
                return None

    if not df_hand.empty:
        df_hand["dichtstbijzijnde_route"] = df_hand.apply(find_nearest_route, axis=1)
    else:
        df_hand["dichtstbijzijnde_route"] = None

    kleuren = [
        [255, 0, 0], [0, 100, 255], [0, 255, 0], [255, 165, 0], [160, 32, 240],
        [0, 206, 209], [255, 105, 180], [255, 255, 0], [139, 69, 19], [0, 128, 128]
    ]
    kleur_map = {route: kleuren[i % len(kleuren)] + [175] for i, route in enumerate(sel_routes)}

    layers = []
    for route in sel_routes:
        df_r = df_routes[df_routes["route_omschrijving"] == route].copy()
        df_r["tooltip_label"] = df_r.apply(
            lambda row: f"""
                <b>🧺 {row['container_name']}</b><br>
                Type: {row['content_type']}<br>
                Vulgraad: {row['fill_level']}%<br>
                Route: {row['route_omschrijving'] or "—"}<br>
                Locatie: {row['address']}, {row['city']}
            """, axis=1
        )
        layers.append(pdk.Layer(
            "ScatterplotLayer",
            data=df_r,
            get_position='[r_lon, r_lat]',
            get_fill_color=kleur_map[route],
            stroked=True,
            get_line_color=[0, 0, 0],            # zwarte outline
            line_width_min_pixels=2,             # dun lijntje
            radiusMinPixels=4,
            radiusMaxPixels=6,
            pickable=True
        ))

    if not df_hand.empty:
        df_hand["tooltip_label"] = df_hand.apply(
            lambda row: f"""
                <b>🖤 {row['container_name']}</b><br>
                Type: {row['content_type']}<br>
                Vulgraad: {row['fill_level']}%<br>
                Route: {row['dichtstbijzijnde_route'] or "—"}<br>
                Locatie: {row['address']}, {row['city']}
            """, axis=1
        )
        layers.append(pdk.Layer(
            "ScatterplotLayer",
            data=df_hand.dropna(subset=["lat", "lon"]),
            get_position='[lon, lat]',
            get_fill_color=[0, 0, 0, 220],
            stroked=True,
            radiusMinPixels=5,
            radiusMaxPixels=10,
            pickable=True
        ))

    tooltip = {
        "html": "{tooltip_label}",
        "style": {"backgroundColor": "steelblue", "color": "white"}
    }

    if not df_containers.empty:
        midpoint = [df_containers["lat"].mean(), df_containers["lon"].mean()]
    else:
        midpoint = [52.0, 4.3]

    st.pydeck_chart(pdk.Deck(
        map_style="mapbox://styles/mapbox/streets-v12",
        initial_view_state=pdk.ViewState(
            latitude=midpoint[0], longitude=midpoint[1],
            zoom=11, pitch=0
        ),
        layers=layers,
        tooltip=tooltip
    ))

    if not df_hand.empty:
        st.markdown("### 📋 Handmatig geselecteerde containers")
        st.dataframe(df_hand[[
            "container_name", "address", "city", "content_type",
            "fill_level", "dichtstbijzijnde_route"
        ]], use_container_width=True)
    else:
        st.info("📋 Nog geen containers geselecteerd. Alleen routes worden getoond.")


# ─── TAB 3: ROUTE STATUS ─────────────────────────
with tab3:
    st.subheader("🚣️ Route status")
    df_routes = run_query("SELECT * FROM public.apb_routes")
    routes = sorted(df_routes["route_omschrijving"].dropna().unique())
    route = st.selectbox("Kies een route", routes)
    status_opties = ["Actueel", "Gedeeltelijk niet gereden door:", "Volledig niet gereden door:"]
    gekozen = st.selectbox("Status", status_opties)
    reden = st.text_input("Reden") if "niet gereden" in gekozen else ""

    if st.button("✅ Bevestig status"):
        vandaag = datetime.now().strftime("%Y-%m-%d")
        if gekozen == "Actueel":
            df_log = run_query("SELECT * FROM public.apb_logboek_route")
            for i, row in df_log[::-1].iterrows():
                if row["route"] == route and row["datum"][:10] == vandaag:
                    execute_query(
                        "DELETE FROM public.apb_logboek_route WHERE id = :id",
                        {"id": row["id"]}
                    )
                    st.success(f"🗑️ Verwijderd: {route} ({vandaag})")
                    break
            else:
                st.info("ℹ️ Geen afwijking voor vandaag gevonden.")
        else:
            if not reden:
                st.warning("⚠️ Reden verplicht.")
            else:
                execute_query(
                    """INSERT INTO apb_logboek_route (route, status, reden, datum)
                       VALUES (:a, :b, :c, :d)""",
                    {
                        "a": route,
                        "b": gekozen.replace(":", ""),
                        "c": reden,
                        "d": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                )
                st.success("📝 Afwijking succesvol gelogd.")
                st.session_state.refresh_needed = True

# ─── TAB 4: ROUTE OPTIMALISATIE (kleurpunten + knop) ─────────────────────────
with tab4:
    st.subheader("🚀 Route-optimalisatie via OR-Tools CVRP (relaxed)")
    sel_routes = st.session_state.geselecteerde_routes
    if len(sel_routes)<2:
        st.info("Selecteer in de sidebar minimaal 2 routes om te optimaliseren.")
        st.stop()
    df_r = load_routes_for_map()
    df_sel = df_r[df_r["route_omschrijving"].isin(sel_routes)].copy()
    common = df_sel["content_type"].value_counts()[lambda x:x>=2].index.tolist()
    if not common:
        st.warning("Onder de geselecteerde routes is géén content_type met ≥2 containers.")
        st.stop()
    optim_type = st.selectbox("Kies content_type voor weergave", common)
    df_opt = df_sel[(df_sel["content_type"]==optim_type)&df_sel["r_lat"].notna()&df_sel["r_lon"].notna()].copy().reset_index(drop=True)
    coords = df_opt[["r_lon","r_lat"]].values.tolist()
    coords_m = project_to_meters(df_opt["r_lon"].values, df_opt["r_lat"].values)
    N = len(coords); k=len(sel_routes)
    OSRM_URL = st.secrets.get("osrm",{}).get("table_url","http://router.project-osrm.org")
    coord_str = ";".join(f"{lon},{lat}" for lon,lat in coords)
    try:
        resp=requests.get(f"{OSRM_URL}/table/v1/driving/{coord_str}",params={"annotations":"distance"})
        resp.raise_for_status()
        matrix = np.array(resp.json()["distances"],dtype=int)
    except:
        st.warning("⚠️ OSRM table failed, falling back to geodesic distances.")
        matrix=np.zeros((N,N),dtype=int)
        for i in range(N):
            for j in range(N):
                if i!=j:
                    matrix[i,j]=int(geodesic((coords[i][1],coords[i][0]),(coords[j][1],coords[j][0])).meters)
    demands=[1]*N
    base,extra=divmod(N,k)
    vehicle_caps=[base+1 if i<extra else base for i in range(k)]
    manager=pywrapcp.RoutingIndexManager(N,k,0)
    routing=pywrapcp.RoutingModel(manager)
    def dist_cb(i,j): return int(matrix[manager.IndexToNode(i)][manager.IndexToNode(j)])
    tidx=routing.RegisterTransitCallback(dist_cb)
    routing.SetArcCostEvaluatorOfAllVehicles(tidx)
    def dem_cb(i): return demands[manager.IndexToNode(i)]
    didx=routing.RegisterUnaryTransitCallback(dem_cb)
    routing.AddDimensionWithVehicleCapacity(didx,0,vehicle_caps,True,"Capacity")
    search_params=pywrapcp.DefaultRoutingSearchParameters()
    search_params.first_solution_strategy=routing_enums_pb2.FirstSolutionStrategy.CHEAPEST_INSERTION
    search_params.local_search_metaheuristic=routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    search_params.time_limit.seconds=120
    solution=routing.SolveWithParameters(search_params)
    if not solution:
        st.warning("⚠️ OR-Tools kon binnen 120 s geen oplossing vinden; gebruik fallback-clustering.")
    if solution:
        labels=np.empty(N,dtype=int)
        for v in range(k):
            idx=routing.Start(v)
            while not routing.IsEnd(idx):
                n=manager.IndexToNode(idx)
                labels[n]=v
                idx=solution.Value(routing.NextVar(idx))
    else:
        seeds_idx=farthest_point_seeds_indices(coords_m,k)
        seed_coords=coords_m[seeds_idx]
        dmat=np.linalg.norm(coords_m[:,None,:]-seed_coords[None,:,:],axis=2)
        init_labels=np.argmin(dmat,axis=1)
        labels=capacity_balance(coords_m,init_labels.copy(),vehicle_caps)
    df_opt["new_route"]=[sel_routes[l] for l in labels]
    st.subheader("📊 Aantal containers per nieuwe route")
    cnt=df_opt.groupby("new_route").size().reset_index(name="aantal").sort_values("new_route")
    st.dataframe(cnt,use_container_width=True)
    kleuren=[[255,0,0],[0,100,255],[0,255,0],[255,165,0],[160,32,240],[0,206,209],[255,105,180],[255,255,0],[139,69,19],[0,128,128]]
    kleur_map={r:kleuren[i%len(kleuren)]+[200] for i,r in enumerate(sel_routes)}
    layers=[]
    for route in sel_routes:
        part=df_opt[df_opt["new_route"]==route].copy()
        part["tooltip"] = part.apply(lambda r: (
            f"<b>🧺 {r['container_name']}</b><br>"
            f"Type: {r['content_type']}<br>"
            f"Vulgraad: {r['fill_level']}%<br>"
            f"Route: {r['new_route']}<br>"
            f"Locatie: {r['address']}, {r['city']}"
        ),axis=1)
        layers.append(pdk.Layer("ScatterplotLayer",data=part,get_position='[r_lon, r_lat]',get_fill_color=kleur_map[route],stroked=True,get_line_color=[0,0,0],line_width_min_pixels=1,radiusMinPixels=6,radiusMaxPixels=10,pickable=True))
    mid_lat,mid_lon=df_opt["r_lat"].mean(),df_opt["r_lon"].mean()
    st.pydeck_chart(pdk.Deck(map_style="mapbox://styles/mapbox/streets-v12",initial_view_state=pdk.ViewState(latitude=mid_lat,longitude=mid_lon,zoom=12,pitch=0),layers=layers,tooltip={"html":"{tooltip}","style":{"backgroundColor":"steelblue","color":"white"}}))
