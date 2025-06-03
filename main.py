import streamlit as st
import pandas as pd
import json
from sqlalchemy import create_engine, text
from datetime import datetime
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
from geopy.distance import geodesic
from collections import Counter
import pydeck as pdk

## ─── LOGIN ───────────────────────────────────────
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

def do_login():
    st.markdown("## 🔐 Log in om toegang te krijgen")
    username = st.text_input("Gebruikersnaam", key="login_user_input")
    password = st.text_input("Wachtwoord", type="password", key="login_pass_input")
    if st.button("Inloggen"):
        users = st.secrets["credentials"]["users"]
        if username in users and password == users[username]:
            st.session_state.authenticated = True
            st.session_state.login_user = username       # Sla de ingelogde gebruiker op
            st.rerun()
        else:
            st.error("❌ Ongeldige gebruikersnaam of wachtwoord")

if not st.session_state.authenticated:
    do_login()
    st.stop()

# ─── GEBRUIKER KEUZE ─────────────────────────────
if "login_user" not in st.session_state:
    st.session_state.login_user = None

if st.session_state.authenticated and st.session_state.get("login_user") != "admin" and st.session_state.get("gebruiker") is None:
    with st.sidebar:
        st.header("👤 Kies je vestiging")
        temp = st.selectbox("Vestiging", ["Delft", "Den Haag"], key="temp_gebruiker")
        if st.button("Vestiging"):
            st.session_state.gebruiker = temp
            st.success(f"✅ Vestiging: {temp}")
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
        "gebruiker": st.session_state.get("gebruiker"),
        # Zet hier alvast een lege login_user, zodat de key altijd bestaat
        "login_user": st.session_state.get("login_user", None),
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

init_session_state()

## ─── SIDEBAR ─────────────────────────────────────
with st.sidebar:
    st.header("🔧 Instellingen")

    # 1) Haal op wie er ingelogd is
    login_user = st.session_state.get("login_user")
    st.markdown(f"**Ingelogd als:** Vestiging: {st.session_state.gebruiker}")
    if st.button("🔄 Wissel vestiging"):
        st.session_state.gebruiker = None
        st.rerun()

    # 2) Controle: bestaat er al data voor vandaag?
    try:
        df_today = run_query("""
            SELECT 1
            FROM apb_routes
            WHERE datum = current_date
            LIMIT 1
        """)
        has_today = not df_today.empty
    except Exception as e:
        st.error(f"❌ Fout bij controle op bestaande data: {e}")
        has_today = False

    # 3) Rol‐selectie: admin mag altijd Upload zien
    if login_user == "admin":
        rollen = ["Gebruiker", "Upload"]
    else:
        if has_today:
            rollen = ["Gebruiker"]
            st.info("✅ Data is up-to-date.")
        else:
            rollen = ["Gebruiker", "Upload"]

    # Als er slechts één rol in de lijst staat, hoef je geen selectbox te tonen
    if len(rollen) == 1:
        rol = rollen[0]
    else:
        rol = st.selectbox("👤 Kies je rol:", rollen)

    st.markdown(f"**Ingelogd als:** {login_user or '—'}")
    if login_user == "admin":
        st.caption("👑 Je bent ingelogd als admin en kunt altijd uploaden.")

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
        types = sorted(df_sidebar["content_type"].dropna().unique())
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
                route_counts = df_routes_full["route_omschrijving"].value_counts().to_dict()
                beschikbare_routes = sorted(route_counts.items())
                label_to_route = {f"{route} ({count})": route for route, count in beschikbare_routes}
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
            else:
                st.info("📬 Geen routes van vandaag of later beschikbaar. Upload eerst data.")
        except Exception as e:
            st.error(f"❌ Fout bij ophalen van routes: {e}")

    elif rol == "Upload":
        st.markdown("### 📤 Upload bestanden")
        file1 = st.file_uploader("🟢 Bestand van Abel", type=["xlsx"], key="upload_abel")
        file2 = st.file_uploader("🔵 Bestand van Pieterbas", type=["xlsx"], key="upload_pb")
        process = st.button("🗄️ Verwerk en laad data")

        if process and file1 and file2:
            try:
                st.cache_data.clear()
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
                df1["oproute"] = df1["container_name"].isin(df2["Omschrijving"]).map({True: "Ja", False: "Nee"})
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

                st.session_state.refresh_needed = True
                st.success("✅ Gegevens succesvol geüpload en cache vernieuwd.")
            except Exception as e:
                st.error(f"❌ Fout bij verwerken van bestanden: {e}")

# ─── DASHBOARD (voorheen tab1) ───────────────────────────────
st.header("📊 Dashboard")

# Haal dezelfde dataframe op als voorheen
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
    if st.button("⬅️", key="prev_page"):
        if st.session_state.page_bewerkbaar > 0:
            st.session_state.page_bewerkbaar -= 1
with col2:
    if st.button("➡️", key="next_page"):
        if st.session_state.page_bewerkbaar < total_pages - 1:
            st.session_state.page_bewerkbaar += 1
with col3:
    st.markdown(f"**Pagina {st.session_state.page_bewerkbaar + 1} van {total_pages}**")

start_idx = st.session_state.page_bewerkbaar * containers_per_page
end_idx = start_idx + containers_per_page
paged = bewerkbaar.iloc[start_idx:end_idx]

# AgGrid
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

# ─── KAARTWEERGAVE (voorheen tab2) ─────────────────────────
st.header("🗺️ Kaartweergave")

@st.cache_data(ttl=300)
def load_routes_for_map():
    df = run_query("""
        SELECT r.route_omschrijving, r.omschrijving AS container_name,
               c.container_location, c.content_type, c.fill_level, c.address, c.city
        FROM apb_routes r
        JOIN apb_containers c ON r.omschrijving = c.container_name
        WHERE c.container_location IS NOT NULL
    """)
    df[["r_lat", "r_lon"]] = (
        df["container_location"]
          .str.split(",", expand=True)
          .astype(float)
    )
    df["fill_level"] = pd.to_numeric(df["fill_level"], errors="coerce")
    return df.dropna(subset=["r_lat", "r_lon"])

@st.cache_data(ttl=300)
def load_all_containers():
    df = run_query("""
        SELECT container_name, container_location, location_code,
               content_type, fill_level, address, city
        FROM apb_containers
    """)
    df[["lat", "lon"]] = (
        df["container_location"]
          .str.split(",", expand=True)
          .astype(float)
    )
    df["fill_level"] = pd.to_numeric(df["fill_level"], errors="coerce")
    return df

# Data laden
df_routes = load_routes_for_map()
df_containers = load_all_containers()
sel_routes = st.session_state.geselecteerde_routes
sel_names  = st.session_state.extra_meegegeven_tijdelijk
df_hand    = df_containers[df_containers["container_name"].isin(sel_names)].copy()

# Bepaal voor handselectie de dichtstbijzijnde route
def find_nearest_route(r):
    if pd.isna(r["lat"]) or pd.isna(r["lon"]):
        return None
    radius = 0.15
    while radius <= 5:
        matches = [
            rp["route_omschrijving"]
            for _, rp in df_routes.iterrows()
            if rp["content_type"] == r["content_type"]
            and geodesic((r["lat"], r["lon"]), (rp["r_lat"], rp["r_lon"])).km <= radius
        ]
        if matches:
            return Counter(matches).most_common(1)[0][0]
        radius += 0.1
    return None

if not df_hand.empty:
    df_hand["dichtstbijzijnde_route"] = df_hand.apply(find_nearest_route, axis=1)

# Kleuren per route
kleuren = [
    [255, 0, 0], [0, 100, 255], [0, 255, 0], [255, 165, 0], [160, 32, 240],
    [0, 206, 209], [255, 105, 180], [255, 255, 0], [139, 69, 9], [0, 128, 128]
]
kleur_map = {
    route: kleuren[i % len(kleuren)] + [175]
    for i, route in enumerate(sel_routes)
}

# Helper-functies voor concatenatie
def concat_names(names):
    return " / ".join(names)

def concat_levels(levels):
    vals = [int(l) for l in levels if pd.notnull(l)]
    return " / ".join(f"{v}%" for v in vals) if vals else ""

# ─── 1) Groeperen van route-punten ─────────────────
grouped_routes = (
    df_routes
    .groupby(
        ["r_lat", "r_lon", "content_type",
         "route_omschrijving", "address", "city"],
        as_index=False
    )
    .agg({
        "container_name": concat_names,
        "fill_level":     concat_levels
    })
)
grouped_routes["tooltip_label"] = grouped_routes.apply(
    lambda row: f"""
        <b>🧺 {row['container_name']}</b><br>
        Type: {row['content_type']}<br>
        Vulgraad: {row['fill_level']}<br>
        Route: {row['route_omschrijving'] or "—"}<br>
        Locatie: {row['address']}, {row['city']}
    """,
    axis=1
)

# ─── 2) Groeperen van handmatig geselecteerde punten ────
if not df_hand.empty:
    grouped_hand = (
        df_hand
        .dropna(subset=["lat", "lon"])
        .groupby(
            ["lat", "lon", "content_type", "address", "city"],
            as_index=False
        )
        .agg({
            "container_name": concat_names,
            "fill_level":     concat_levels,
            "dichtstbijzijnde_route": lambda routes: " / ".join(
                dict.fromkeys(r for r in routes if r)
            )
        })
    )
    grouped_hand["tooltip_label"] = grouped_hand.apply(
        lambda row: f"""
            <b>🖤 {row['container_name']}</b><br>
            Type: {row['content_type']}<br>
            Vulgraad: {row['fill_level']}<br>
            Route: {row['dichtstbijzijnde_route'] or "—"}<br>
            Locatie: {row['address']}, {row['city']}
        """,
        axis=1
    )

# ─── 3) Definitie van PyDeck-lagen ─────────────────────
layers = []
# per route
for route in sel_routes:
    df_r = grouped_routes[grouped_routes["route_omschrijving"] == route]
    layers.append(pdk.Layer(
        "ScatterplotLayer",
        data=df_r,
        get_position='[r_lon, r_lat]',
        get_fill_color=kleur_map[route],
        stroked=True,
        get_line_color=[0, 0, 0],
        line_width_min_pixels=2,
        radiusMinPixels=4,
        radiusMaxPixels=6,
        pickable=True
    ))
# handmatige selectie
if not df_hand.empty:
    layers.append(pdk.Layer(
        "ScatterplotLayer",
        data=grouped_hand,
        get_position='[lon, lat]',
        get_fill_color=[0, 0, 0, 220],
        stroked=True,
        radiusMinPixels=5,
        radiusMaxPixels=10,
        pickable=True
    ))

# Tooltip-stijl
tooltip = {
    "html": "{tooltip_label}",
    "style": {"backgroundColor": "steelblue", "color": "white"}
}

# Midpoint bepalen
if not df_containers.empty:
    midpoint = [df_containers["lat"].mean(), df_containers["lon"].mean()]
else:
    midpoint = [52.0, 4.3]

# Kaart renderen
st.pydeck_chart(pdk.Deck(
    map_style="mapbox://styles/mapbox/streets-v12",
    initial_view_state=pdk.ViewState(
        latitude=midpoint[0], longitude=midpoint[1],
        zoom=11, pitch=0
    ),
    layers=layers,
    tooltip=tooltip
))

# Onder de kaart: handmatige selectie
if not df_hand.empty:
    st.markdown("### 📋 Handmatig geselecteerde containers")
    st.dataframe(
        df_hand[[
            "container_name", "address", "city", "location_code",
            "content_type", "fill_level", "dichtstbijzijnde_route"
        ]],
        use_container_width=True
    )
else:
    st.info("📋 Nog geen containers geselecteerd. Alleen routes worden getoond.")
