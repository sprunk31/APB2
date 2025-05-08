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


with tab2:
    st.subheader("🗺️ Containerkaart (pydeck)")

    # 🧭 Laad route- en containerdata (gecached)
    @st.cache_data(ttl=300)
    def load_routes_for_map():
        return run_query("""
            SELECT r.route_omschrijving, r.omschrijving AS container_name,
                   c.container_location, c.content_type
            FROM apb_routes r
            JOIN apb_containers c ON r.omschrijving = c.container_name
            WHERE c.container_location IS NOT NULL
        """)

    @st.cache_data(ttl=300)
    def load_all_containers():
        return run_query("""
            SELECT container_name, container_location, content_type, fill_level, address, city
            FROM apb_containers
        """)

    def parse_location_to_latlon(df, col="container_location"):
        df[["lat", "lon"]] = df[col].str.split(",", expand=True).astype(float)
        return df

    df_routes = load_routes_for_map()
    df_routes = parse_location_to_latlon(df_routes)

    df_containers = load_all_containers()
    df_containers = parse_location_to_latlon(df_containers)

    geselecteerde_routes = st.session_state.get("geselecteerde_routes", [])
    geselecteerde_namen = st.session_state.get("extra_meegegeven_tijdelijk", [])

    # 🔎 Handmatig geselecteerde containers
    df_hand = df_containers[df_containers["container_name"].isin(geselecteerde_namen)].copy()

    # 🗺️ Pydeck-lagen
    layers = []

    if geselecteerde_routes:
        for route in geselecteerde_routes:
            df_route = df_routes[df_routes["route_omschrijving"] == route]
            color = [0, 100, 255]  # blauw
            layers.append(pdk.Layer(
                "ScatterplotLayer",
                data=df_route,
                get_position='[lon, lat]',
                get_color=color,
                get_radius=50,
                pickable=True,
                tooltip=True
            ))

    if not df_hand.empty:
        layers.append(pdk.Layer(
            "ScatterplotLayer",
            data=df_hand,
            get_position='[lon, lat]',
            get_color='[200, 30, 0, 160]',  # rood
            get_radius=80,
            pickable=True,
            tooltip=True
        ))

    # 🔍 Tooltip
    tooltip = {
        "html": """
        <b>🧺 {container_name}</b><br>
        Type: {content_type}<br>
        Vulgraad: {fill_level}%<br>
        Locatie: {address}, {city}
        """,
        "style": {"backgroundColor": "steelblue", "color": "white"}
    }

    # 🌍 Weergave
    if not df_containers.empty:
        midpoint = [df_containers["lat"].mean(), df_containers["lon"].mean()]
    else:
        midpoint = [52.0, 4.3]

    st.pydeck_chart(pdk.Deck(
        map_style="mapbox://styles/mapbox/light-v9",
        initial_view_state=pdk.ViewState(
            latitude=midpoint[0],
            longitude=midpoint[1],
            zoom=11,
            pitch=0,
        ),
        layers=layers,
        tooltip=tooltip
    ))

    # 📋 Extra info
    if not df_hand.empty:
        st.markdown("### 📋 Handmatig geselecteerde containers")
        st.dataframe(df_hand[["container_name", "address", "city", "content_type", "fill_level"]], use_container_width=True)
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