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

# ─── LOGIN ───────────────────────────────────────
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
    return run_query("""
        SELECT container_name, container_location, content_type, fill_level, address, city
        FROM apb_containers
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

# ─── SESSIESTATE INITIALISATIE ─────────────────────────────
def init_session_state():
    defaults = {
        "op_route": False,
        "selected_type": None,
        "refresh_needed": False,
        "extra_meegegeven_tijdelijk": [],
        "geselecteerde_routes": [],
        "gebruiker": "Onbekend"
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

init_session_state()

# ─── SIDEBAR ─────────────────────────────────────
with st.sidebar:
    st.header("🔧 Instellingen")
    rol = st.selectbox("👤 Kies je rol:", ["Gebruiker", "Upload"])

    try:
        if st.session_state.refresh_needed:
            st.cache_data.clear()
            st.session_state.refresh_needed = False

        df_sidebar = get_df_sidebar()
    except Exception as e:
        st.error(f"❌ Fout bij laden van containerdata: {e}")
        df_sidebar = pd.DataFrame()

    if rol == "Gebruiker":
        gebruiker = st.selectbox("🔑 Kies je gebruiker:", ["Delft", "Den Haag"])
        st.markdown("### 🔎 Filters")
        types = sorted(df_sidebar["content_type"].dropna().unique())
        if st.session_state.selected_type not in types:
            st.session_state.selected_type = types[0] if types else None
        st.session_state.selected_type = st.selectbox("Content type", types, index=types.index(st.session_state.selected_type))
        st.session_state.op_route = st.toggle("📍 Alleen op route", value=st.session_state.op_route)

        st.markdown("### 🚚 Routeselectie")
        try:
            df_routes_full = get_df_routes()

            if not df_routes_full.empty:
                def _parse(loc):
                    try: return tuple(map(float, loc.split(",")))
                    except: return (None, None)

                df_routes_full[["r_lat", "r_lon"]] = df_routes_full["container_location"].apply(lambda loc: pd.Series(_parse(loc)))

                if "routes_cache" not in st.session_state:
                    st.session_state["routes_cache"] = df_routes_full

                beschikbare_routes = sorted(df_routes_full["route_omschrijving"].dropna().unique())
                st.session_state.geselecteerde_routes = st.multiselect(
                    label="📍 Selecteer één of meerdere routes:",
                    options=beschikbare_routes,
                    default=st.session_state.get("geselecteerde_routes", []),
                    placeholder="Klik om routes te selecteren (blijft geselecteerd)",
                )
            else:
                st.info("📬 Geen routes van vandaag of later beschikbaar. Upload eerst data.")
        except Exception as e:
            st.error(f"❌ Fout bij ophalen van routes: {e}")


    elif rol == "Upload":

        st.markdown("### 📤 Upload bestanden")

        file1 = st.file_uploader("🟢 Bestand van Abel", type=["xlsx"], key="upload_abel")

        file2 = st.file_uploader("🔵 Bestand van Pieterbas", type=["xlsx"], key="upload_pb")

        if file1 and file2:

            try:

                df1 = pd.read_excel(file1)

                df1.columns = df1.columns.str.strip().str.lower().str.replace(" ", "_")

                df1.rename(columns={"fill_level_(%)": "fill_level"}, inplace=True)

                df2 = pd.read_excel(file2)

                df1 = df1[(df1['operational_state'] == 'In use') & (df1['status'] == 'In use') & (
                            df1['on_hold'] == 'No')].copy()

                df1["content_type"] = df1["content_type"].apply(lambda x: "Glas" if "glass" in str(x).lower() else x)

                df1['combinatietelling'] = df1.groupby(['location_code', 'content_type'])['content_type'].transform(
                    'count')

                df1['gemiddeldevulgraad'] = df1.groupby(['location_code', 'content_type'])['fill_level'].transform(
                    'mean')

                df1['oproute'] = df1['container_name'].isin(df2['Omschrijving'].values).map({True: 'Ja', False: 'Nee'})

                df1['extra_meegegeven'] = False

                # Beperk tot alleen de gewenste kolommen

                kolommen_bewaren = [

                    "container_name", "address", "city", "location_code", "content_type",

                    "fill_level", "container_location", "combinatietelling",

                    "gemiddeldevulgraad", "oproute", "extra_meegegeven"

                ]

                df1 = df1[kolommen_bewaren]

                # Verwijder alle bestaande rijen, maar behoud structuur

                execute_query("DELETE FROM apb_containers")

                # Voeg daarna nieuwe records toe

                df1.to_sql("apb_containers", get_engine(), if_exists="append", index=False)

                df2 = df2.rename(columns={

                    "Route Omschrijving": "route_omschrijving",

                    "Omschrijving": "omschrijving",

                    "Datum": "datum"

                })

                # Verwijder alle bestaande rijen, behoud structuur

                execute_query("DELETE FROM apb_routes")

                # Voeg nieuwe records toe

                df2[["route_omschrijving", "omschrijving", "datum"]].drop_duplicates().to_sql(

                    "apb_routes", get_engine(), if_exists="append", index=False

                )

                df_routes_full = run_query("""

                        SELECT r.route_omschrijving, r.omschrijving AS container_name,

                               c.container_location, c.content_type

                        FROM apb_routes r

                        JOIN apb_containers c ON r.omschrijving = c.container_name

                        WHERE c.container_location IS NOT NULL

                    """)


                def _parse(loc):

                    try:
                        return tuple(map(float, loc.split(",")))

                    except:
                        return (None, None)


                df_routes_full[["r_lat", "r_lon"]] = df_routes_full["container_location"].apply(
                    lambda loc: pd.Series(_parse(loc)))

                st.session_state["routes_cache"] = df_routes_full

                st.success("✅ Gegevens succesvol opgeslagen in de database.")

            except Exception as e:

                st.error(f"❌ Fout bij verwerken van bestanden: {e}")


# ─── TABS ─────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["📊 Dashboard", "🗺️ Kaartweergave", "📋 Route-status"])

# ─── TAB 1: DASHBOARD ────────────────────────────
with tab1:
    df = df_sidebar.copy()
    if "refresh_needed" in st.session_state and st.session_state.refresh_needed:
        df = run_query("SELECT * FROM apb_containers")
        st.session_state.refresh_needed = False

    df["fill_level"] = pd.to_numeric(df["fill_level"], errors="coerce")
    df["extra_meegegeven"] = df["extra_meegegeven"].astype(bool)

    df_all = df.copy()
    try:
        df_logboek = run_query("SELECT gebruiker FROM apb_logboek_afvalcontainers where datum >= current_date")
        log_counts = df_logboek["gebruiker"].value_counts()
        delft_count = log_counts.get("Delft", 0)
        denhaag_count = log_counts.get("Den Haag", 0)
    except:
        delft_count = denhaag_count = 0

    kpi1, kpi2, kpi3 = st.columns(3)
    kpi1.metric("📦 Totaal containers", len(df_all))
    kpi2.metric("📊 Vulgraad ≥ 80%", (df_all["fill_level"] >= 80).sum())
    kpi3.metric("🧍 Extra meegegeven (Delft / Den Haag)", f"{delft_count} / {denhaag_count}")

    df = df[df["content_type"] == st.session_state.selected_type]
    df = df[df["oproute"] == ("Ja" if st.session_state.op_route else "Nee")]

    zichtbaar = [
        "container_name", "address", "city", "location_code", "content_type",
        "fill_level", "combinatietelling", "gemiddeldevulgraad", "oproute", "extra_meegegeven"
    ]

    # Selecteer alleen containers die nog niet extra zijn meegegeven
    bewerkbaar = df[df["extra_meegegeven"] == False].copy()

    # Filter op vulgraad-criteria
    bewerkbaar = bewerkbaar[
        (bewerkbaar["gemiddeldevulgraad"] > 65) |
        (bewerkbaar["fill_level"] > 80)
        ]

    # Sorteer bijv. nog op vulgraad
    bewerkbaar = bewerkbaar.sort_values(by="gemiddeldevulgraad", ascending=False)
    st.subheader("✏️ Bewerkbare containers")
    gb = GridOptionsBuilder.from_dataframe(bewerkbaar[zichtbaar])
    gb.configure_default_column(filter=True)
    gb.configure_column("extra_meegegeven", editable=True)

    grid_response = AgGrid(
        bewerkbaar[zichtbaar],
        gridOptions=gb.build(),
        update_mode=GridUpdateMode.VALUE_CHANGED,
        height=500
    )
    updated_df = grid_response["data"].copy()
    updated_df["extra_meegegeven"] = updated_df["extra_meegegeven"].astype(bool)

    tijdelijke_selectie = updated_df[updated_df["extra_meegegeven"] == True]["container_name"].tolist()
    st.session_state["extra_meegegeven_tijdelijk"] = tijdelijke_selectie

    if st.button("✅ Wijzigingen toepassen en loggen"):
        gewijzigde_rijen = updated_df[updated_df["extra_meegegeven"] == True]
        if not gewijzigde_rijen.empty:
            try:
                df_log = run_query("SELECT container_name, datum FROM apb_logboek_afvalcontainers")
                df_log["datum"] = pd.to_datetime(df_log["datum"], errors="coerce")
            except Exception:
                df_log = pd.DataFrame(columns=["container_name", "datum"])
            vandaag = datetime.now().date()
            log_count = 0
            for _, row in gewijzigde_rijen.iterrows():
                if ((df_log["container_name"] == row["container_name"]) &
                    (df_log["datum"].dt.date == vandaag)).any():
                    continue
                naam = row["container_name"].strip()
                execute_query(
                    "UPDATE apb_containers SET extra_meegegeven = TRUE WHERE TRIM(container_name) = :naam",
                    {"naam": naam}
                )
                execute_query(
                    """INSERT INTO apb_logboek_afvalcontainers
                    (container_name, address, city, location_code, content_type, fill_level, datum, gebruiker)
                    VALUES (:a, :b, :c, :d, :e, :f, :g, :h)""",
                    {
                        "a": row["container_name"], "b": row["address"], "c": row["city"],
                        "d": row["location_code"], "e": row["content_type"],
                        "f": row["fill_level"], "g": datetime.now(), "h": st.session_state.get("gebruiker", "Onbekend")
                    }
                )

                log_count += 1
            if log_count > 0:
                st.success(f"✔️ {log_count} containers gelogd en bijgewerkt.")
                st.session_state.refresh_needed = True
                st.rerun()
            else:
                st.warning("⚠️ Geen nieuwe logs toegevoegd.")

    st.subheader("🔒 Reeds gemarkeerde containers")
    reeds = df[df["extra_meegegeven"] == True]
    st.dataframe(reeds[zichtbaar], use_container_width=True)


# ─── TAB 2: KAART ─────────────────────────────────
with tab2:
    st.subheader("🗺️ Containerkaart")

    # ───── Data laden ───────────────────────────────
    @st.cache_data(ttl=300)
    def load_routes_for_map():
        df = run_query("""
            SELECT r.route_omschrijving, r.omschrijving AS container_name,
                   c.container_location, c.content_type, c.fill_level, c.address, c.city
            FROM apb_routes r
            JOIN apb_containers c ON r.omschrijving = c.container_name
            WHERE c.container_location IS NOT NULL
        """)
        df[["r_lat", "r_lon"]] = df["container_location"].str.split(",", expand=True).astype(float)
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

    geselecteerde_routes = st.session_state.get("geselecteerde_routes", [])
    geselecteerde_namen = st.session_state.get("extra_meegegeven_tijdelijk", [])

    # ───── Handmatig geselecteerd ───────────────────
    df_hand = df_containers[df_containers["container_name"].isin(geselecteerde_namen)].copy()

    # ───── Dichtstbijzijnde route ───────────────────
    def find_nearest_route(r):
        if pd.isna(r["lat"]) or pd.isna(r["lon"]): return None
        radius = 0.15
        while True:
            matches = [
                rp["route_omschrijving"] for _, rp in df_routes.iterrows()
                if rp["content_type"] == r["content_type"]
                and not pd.isna(rp["r_lat"])
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

    # ───── Kleur per route ──────────────────────────
    kleuren = [
        [255, 0, 0], [0, 100, 255], [0, 255, 0], [255, 165, 0], [160, 32, 240],
        [0, 206, 209], [255, 105, 180], [255, 255, 0], [139, 69, 19], [0, 128, 128]
    ]
    kleur_map = {route: kleuren[i % len(kleuren)] + [175] for i, route in enumerate(geselecteerde_routes)}

    # ───── Layers ───────────────────────────────────
    layers = []

    for route in geselecteerde_routes:
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

    # ───── Tooltip ──────────────────────────────────
    tooltip = {
        "html": "{tooltip_label}",
        "style": {
            "backgroundColor": "steelblue",
            "color": "white"
        }
    }

    # ───── View ─────────────────────────────────────
    if not df_containers.empty:
        midpoint = [df_containers["lat"].mean(), df_containers["lon"].mean()]
    else:
        midpoint = [52.0, 4.3]

    st.pydeck_chart(pdk.Deck(
        map_style="mapbox://styles/mapbox/streets-v12",
        initial_view_state=pdk.ViewState(
            latitude=midpoint[0],
            longitude=midpoint[1],
            zoom=11,
            pitch=0,
        ),
        layers=layers,
        tooltip=tooltip
    ))

    # ───── Extra info ───────────────────────────────
    if not df_hand.empty:
        st.markdown("### 📋 Handmatig geselecteerde containers")
        st.dataframe(df_hand[[
            "container_name", "address", "city", "content_type", "fill_level", "dichtstbijzijnde_route"
        ]], use_container_width=True)
    else:
        st.info("📋 Nog geen containers geselecteerd. Alleen routes worden getoond.")



# ─── TAB 3: ROUTE STATUS ─────────────────────────
with tab3:
    st.subheader("🚣️ Route status")
    df = run_query("SELECT * FROM public.apb_routes")
    routes = sorted(df["route_omschrijving"].dropna().unique())
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
                    execute_query("DELETE FROM public.apb_logboek_route WHERE id = :id", {"id": row["id"]})
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