import streamlit as st
import pandas as pd
import json
from sqlalchemy import create_engine, text
from datetime import datetime
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import itertools
import folium
from streamlit_folium import st_folium
from geopy.distance import geodesic
from collections import Counter
import pydeck as pdk

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
        # … bestaande filter- en route-logica …
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

                df1 = df1[
                    (df1['operational_state']=='In use') &
                    (df1['status']=='In use') &
                    (df1['on_hold']=='No')
                ].copy()
                df1["content_type"] = df1["content_type"].apply(
                    lambda x: "Glas" if "glass" in str(x).lower() else x
                )
                df1["combinatietelling"] = df1.groupby(
                    ["location_code","content_type"]
                )["content_type"].transform("count")
                df1["gemiddeldevulgraad"] = df1.groupby(
                    ["location_code","content_type"]
                )["fill_level"].transform("mean")
                df1["oproute"] = df1["container_name"].isin(df2["Omschrijving"]).map(
                    {True: "Ja", False: "Nee"}
                )
                df1["extra_meegegeven"] = False

                cols = [
                    "container_name","address","city","location_code","content_type",
                    "fill_level","container_location","combinatietelling",
                    "gemiddeldevulgraad","oproute","extra_meegegeven"
                ]
                df1 = df1[cols]
                df1["datum_ingelezen"] = datetime.now().date()

                engine = get_engine()
                with engine.begin() as conn:
                    conn.execute(text("TRUNCATE TABLE apb_containers RESTART IDENTITY"))
                df1.to_sql("apb_containers", engine, if_exists="append", index=False)

                df2 = df2.rename(columns={
                    "Route Omschrijving":"route_omschrijving",
                    "Omschrijving":"omschrijving",
                    "Datum":"datum"
                })[["route_omschrijving","omschrijving","datum"]].drop_duplicates()
                with engine.begin() as conn:
                    conn.execute(text("TRUNCATE TABLE apb_routes RESTART IDENTITY"))
                df2.to_sql("apb_routes", engine, if_exists="append", index=False)

                # Markeer voor herladen in hoofd-app
                st.session_state.refresh_needed = True
                st.success("✅ Gegevens succesvol geüpload en cache vernieuwd.")
            except Exception as e:
                st.error(f"❌ Fout bij verwerken van bestanden: {e}")


# ─── TABS ─────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["📊 Dashboard", "🗺️ Kaartweergave", "📋 Route-status"])

# ─── TAB 1: DASHBOARD ────────────────────────────
with tab1:
    df = df_sidebar.copy()
    if st.session_state.refresh_needed:
        # Altijd actuele data ophalen met datumfilter
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
    k1.metric("📦 Totaal containers", len(df))
    k2.metric("📊 Vulgraad ≥ 80%", (df["fill_level"] >= 80).sum())
    k3.metric("🧍 Extra meegegeven (Delft / Den Haag)", f"{delft_count} / {denhaag_count}")

    # Filters
    # Filter zó dat je enkel containers met oproute == 'Nee' ziet
    df = df[df["content_type"].isin(st.session_state.selected_types)]
    df = df[df["oproute"] == "Nee"]

    zichtbaar = [
        "container_name", "address", "city", "location_code", "content_type",
        "fill_level", "combinatietelling", "gemiddeldevulgraad", "oproute", "extra_meegegeven"
    ]

    # Bewerkbare containers
    bewerkbaar = df[~df["extra_meegegeven"]].copy()
    bewerkbaar = bewerkbaar[
        (bewerkbaar["gemiddeldevulgraad"] > 45) |
        (bewerkbaar["fill_level"] > 80)
    ].sort_values("gemiddeldevulgraad", ascending=False)

    st.subheader("✏️ Bewerkbare containers")
    gb = GridOptionsBuilder.from_dataframe(bewerkbaar[zichtbaar])
    gb.configure_default_column(filter=True)
    gb.configure_column("extra_meegegeven", editable=True)
    grid = AgGrid(
        bewerkbaar[zichtbaar],
        gridOptions=gb.build(),
        update_mode=GridUpdateMode.VALUE_CHANGED,
        height=500
    )
    updated = grid["data"].copy()
    updated["extra_meegegeven"] = updated["extra_meegegeven"].astype(bool)
    st.session_state.extra_meegegeven_tijdelijk = updated[updated["extra_meegegeven"]]["container_name"].tolist()

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
        df = df.dropna(subset=["r_lat", "r_lon"])  # ← belangrijk
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
            radiusMinPixels=4,
            radiusMaxPixels=6,
            pickable=True,
            get_line_color=[0, 0, 220],
            line_width_min_pixels=0
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
            get_fill_color='[0, 0, 0, 220]',
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
        layers=layers, tooltip=tooltip
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