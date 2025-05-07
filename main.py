import streamlit as st
import pandas as pd
import json
from sqlalchemy import create_engine, text
from datetime import datetime
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import folium
import socket
from folium.plugins import HeatMap
from streamlit_folium import st_folium
from geopy.distance import geodesic
from streamlit_autorefresh import st_autorefresh


# ─── BASIC LOGIN ────────────────────────────────────────────────────────────────
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
# ────────────────────────────────────────────────────────────────────────────────

@st.cache_resource
def get_engine():
    cfg = st.secrets["postgres"]
    # dynamisch het IPv4-adres van je host ophalen
    infos = socket.getaddrinfo(cfg["host"], None)
    ipv4 = next((info[4][0] for info in infos if info[0] == socket.AF_INET), cfg["host"])

    # bouw de URL met sslmode
    db_url = (
        f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg['port']}/{cfg['dbname']}?sslmode=require"
    )

    # forceer verbinding via het IPv4-adres
    return create_engine(
        db_url,
        connect_args={
            "sslmode": "require",
            "hostaddr": ipv4
        }
    )


def run_query(query, params=None):
    with get_engine().connect() as conn:
        return pd.read_sql(text(query), conn, params=params)

def execute_query(query, params=None):
    with get_engine().begin() as conn:
        conn.execute(text(query), params or {})

st.set_page_config(page_title="Afvalcontainerbeheer", layout="wide")
st.title("♻️ Afvalcontainerbeheer Dashboard")

tab1, tab2, tab3 = st.tabs(["📊 Dashboard", "🗺️ Kaartweergave", "📋 Route-status"])

with tab1:
    # Initialiseer sessiestate voor filterbehoud en refresh
    if "selected_type" not in st.session_state:
        st.session_state.selected_type = None
    if "op_route" not in st.session_state:
        st.session_state.op_route = False
    if "refresh_needed" not in st.session_state:
        st.session_state.refresh_needed = False

    rol = st.selectbox("👤 Kies je rol:", ["Gebruiker", "Upload"], label_visibility="collapsed")

    if rol == "Upload":
        st.subheader("📤 Upload Excel-bestanden")
        file1 = st.file_uploader("Bestand van Abel", type=["xlsx"])
        file2 = st.file_uploader("Bestand van Pieterbas", type=["xlsx"])

        if file1 and file2:
            df1 = pd.read_excel(file1)
            df1.columns = df1.columns.str.strip().str.lower().str.replace(" ", "_")
            df1.rename(columns={"fill_level_(%)": "fill_level"}, inplace=True)
            df2 = pd.read_excel(file2)

            df1 = df1[(df1['operational_state'] == 'In use') &
                      (df1['status'] == 'In use') &
                      (df1['on_hold'] == 'No')].copy()

            df1["content_type"] = df1["content_type"].apply(lambda x: "Glas" if "glass" in str(x).lower() else x)
            df1['combinatietelling'] = df1.groupby(['location_code', 'content_type'])['content_type'].transform('count')
            df1['gemiddeldevulgraad'] = df1.groupby(['location_code', 'content_type'])['fill_level'].transform('mean')
            df1['oproute'] = df1['container_name'].isin(df2['Omschrijving'].values).map({True: 'Ja', False: 'Nee'})
            df1['extra_meegegeven'] = False

            engine = get_engine()
            df1.to_sql("apb_containers", engine, if_exists="replace", index=False)
            # Normaliseer kolomnamen vóór to_sql
            df2 = df2.rename(columns={
                "Route Omschrijving": "route_omschrijving",
                "Omschrijving": "omschrijving",
                "Datum": "datum"
            })

            df2[["route_omschrijving", "omschrijving", "datum"]].drop_duplicates().to_sql(
                "apb_routes", engine, if_exists="replace", index=False
            )

            st.success("✅ Gegevens succesvol opgeslagen in de database.")

    elif rol == "Gebruiker":
        gebruiker = st.selectbox("🔑 Kies je gebruiker:", ["Delft", "Den Haag"])

        # Laad data (ververs indien nodig)
        if st.session_state.refresh_needed:
            df = run_query("SELECT * FROM apb_containers")
            st.session_state.refresh_needed = False
        else:
            df = run_query("SELECT * FROM apb_containers")

        df["extra_meegegeven"] = df["extra_meegegeven"].astype(bool)
        df["fill_level"] = pd.to_numeric(df["fill_level"], errors="coerce")

        with st.expander("🔎 Filters", expanded=True):
            col1, col2 = st.columns(2)
            with col1:
                types = sorted(df["content_type"].dropna().unique())
                if st.session_state.selected_type not in types:
                    st.session_state.selected_type = types[0] if types else None
                selected_type = st.selectbox("Content type", types, index=types.index(st.session_state.selected_type))
                st.session_state.selected_type = selected_type
            with col2:
                op_route = st.toggle("📍 Alleen op route", value=st.session_state.op_route)
                st.session_state.op_route = op_route

        df_all = df.copy()

        # KPI's 1–3 uit hoofddata
        try:
            df_logboek = run_query("SELECT gebruiker FROM apb_logboek_afvalcontainers")
            log_counts = df_logboek["gebruiker"].value_counts()
            delft_count = log_counts.get("Delft", 0)
            denhaag_count = log_counts.get("Den Haag", 0)
        except:
            delft_count = denhaag_count = 0

        # Toon alle KPI's in 4 kolommen naast elkaar
        kpi1, kpi2, kpi3 = st.columns(3)

        kpi1.metric("📦 Totaal containers", len(df_all))
        kpi2.metric("📊 Vulgraad ≥ 80%", (df_all["fill_level"] >= 80).sum())
        kpi3.metric("🧍 Extra meegegeven (Delft / Den Haag)", f"{delft_count} / {denhaag_count}")

        # Filters toepassen
        df = df[df["content_type"] == st.session_state.selected_type]
        df = df[df["oproute"] == ("Ja" if st.session_state.op_route else "Nee")]


        zichtbaar = [
            "container_name", "address", "city", "location_code", "content_type",
            "fill_level", "combinatietelling", "gemiddeldevulgraad", "oproute", "extra_meegegeven"
        ]

        bewerkbaar = df[df["extra_meegegeven"] == False].copy()
        st.subheader("✏️ Bewerkbare containers")
        gb = GridOptionsBuilder.from_dataframe(bewerkbaar[zichtbaar])
        gb.configure_column("extra_meegegeven", editable=True)
        grid_response = AgGrid(
            bewerkbaar[zichtbaar], gridOptions=gb.build(), update_mode=GridUpdateMode.VALUE_CHANGED, height=500
        )
        updated_df = grid_response["data"].copy()
        updated_df["extra_meegegeven"] = updated_df["extra_meegegeven"].astype(bool)

        # Belangrijk: werk de sessiestate altijd bij met de laatste selectie
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
                    al_gelogd = (
                        (df_log["container_name"] == row["container_name"]) &
                        (df_log["datum"].dt.date == vandaag)
                    ).any()

                    if al_gelogd:
                        continue

                    naam = row["container_name"].strip()
                    update_query = f"""
                        UPDATE apb_containers
                        SET extra_meegegeven = TRUE
                        WHERE TRIM(container_name) = '{naam}'
                    """
                    execute_query(update_query)

                    execute_query("""
                        INSERT INTO apb_logboek_afvalcontainers (
                            container_name, address, city, location_code, content_type,
                            fill_level, datum, gebruiker
                        ) VALUES (:a, :b, :c, :d, :e, :f, :g, :h)
                    """, {
                        "a": row["container_name"], "b": row["address"], "c": row["city"],
                        "d": row["location_code"], "e": row["content_type"],
                        "f": row["fill_level"], "g": datetime.now(), "h": gebruiker
                    })

                    log_count += 1

                if log_count > 0:
                    st.success(f"✔️ {log_count} containers gelogd en bijgewerkt.")
                    st.session_state.refresh_needed = True
                    st.rerun()  # ⬅️ dit forceert een volledige heruitvoering
                else:
                    st.warning("⚠️ Geen nieuwe logs toegevoegd.")

        st.subheader("🔒 Reeds gemarkeerde containers")
        reeds = df[df["extra_meegegeven"] == True]
        st.dataframe(reeds[zichtbaar], use_container_width=True)

#---------------------kaart-----------------
with tab2:
    import itertools
    import folium
    from streamlit_folium import st_folium

    st.subheader("🗺️ Containerkaart")


    # ⬇️ Voeg deze toe vóór gebruik!
    def parse_location(loc):
        try:
            lat, lon = map(float, loc.split(","))
            return lat, lon
        except:
            return None, None


    # Data ophalen
    df_routes = run_query("SELECT route_omschrijving, omschrijving FROM apb_routes")
    df_containers = run_query("SELECT container_name, container_location, content_type, fill_level FROM apb_containers")

    # Keuze van routes (meerdere tegelijk mogelijk)
    beschikbare_routes = sorted(df_routes["route_omschrijving"].dropna().unique())
    geselecteerde_routes = st.multiselect("📍 Selecteer één of meerdere routes:", beschikbare_routes)

    # Handmatige selectie (van tab1)
    geselecteerde_namen = st.session_state.get("extra_meegegeven_tijdelijk", [])
    aantal_geselecteerd = len(geselecteerde_namen)

    # Begin met lege kaart
    m = folium.Map(location=[52.0, 4.3], zoom_start=11)

    # 🌈 ROUTES tekenen (één kleur per route)
    if geselecteerde_routes:
        kleuren = itertools.cycle([
            "red", "blue", "green", "purple", "orange", "darkred",
            "lightblue", "darkgreen", "cadetblue", "pink"
        ])
        kleur_map = {route: kleur for route, kleur in zip(geselecteerde_routes, kleuren)}

        df_routenamen = df_routes[df_routes["route_omschrijving"].isin(geselecteerde_routes)]

        df_routedata = df_routenamen.merge(
            df_containers,
            left_on="omschrijving",
            right_on="container_name",
            how="inner"
        )

        # Coördinaten splitsen
        def parse_location(loc):
            try:
                lat, lon = map(float, loc.split(","))
                return lat, lon
            except:
                return None, None

        df_routedata[["lat", "lon"]] = df_routedata["container_location"].apply(
            lambda x: pd.Series(parse_location(x) if pd.notna(x) else (None, None))
        )

        # Markers per routekleur
        for _, row in df_routedata.dropna(subset=["lat", "lon"]).iterrows():
            kleur = kleur_map.get(row["route_omschrijving"], "gray")
            folium.CircleMarker(
                location=(row["lat"], row["lon"]),
                radius=6,
                color=kleur,
                fill=True,
                fill_color=kleur,
                fill_opacity=0.8,
                tooltip=folium.Tooltip(
                    f"""
                    📦 <b>{row['container_name']}</b><br>
                    🧺 {row['content_type']}<br>
                    📊 Vulgraad: {row['fill_level']}%<br>
                    🚚 Route: {row['route_omschrijving']}
                    """,
                    sticky=True
                )
            ).add_to(m)

    # 🖤 Handmatig geselecteerde containers tekenen
    if geselecteerde_namen:
        df_handmatig = df_containers[df_containers["container_name"].isin(geselecteerde_namen)].copy()
        df_handmatig[["lat", "lon"]] = df_handmatig["container_location"].apply(
            lambda x: pd.Series(parse_location(x) if pd.notna(x) else (None, None))
        )

        for _, row in df_handmatig.dropna(subset=["lat", "lon"]).iterrows():
            folium.Marker(
                location=(row["lat"], row["lon"]),
                popup=f"🖤 {row['container_name']}",
                icon=folium.Icon(color="black", icon="plus")
            ).add_to(m)

    # Layout: kaart links, selectie rechts
    col_kaart, col_rechts = st.columns([3, 1])

    with col_kaart:
        st_folium(m, width=1000, height=600)

    with col_rechts:
        if geselecteerde_namen:
            df_handmatig = df_containers[df_containers["container_name"].isin(geselecteerde_namen)].copy()
            st.markdown("### 📋 Handmatig geselecteerde containers")
            st.dataframe(df_handmatig[[
                "container_name", "content_type", "fill_level", "container_location"
            ]], use_container_width=True)
        else:
            st.info("📋 Nog geen containers geselecteerd in tab 1.")

# -------------------- ROUTE STATUS --------------------
with tab3:
    df = run_query("SELECT * FROM public.apb_routes")
    routes = sorted(df["route_omschrijving"].dropna().unique())

    st.subheader("🚣️ Route status")
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
                execute_query("""
                    INSERT INTO public.apb_logboek_route (route, status, reden, datum)
                    VALUES (:a, :b, :c, :d)
                """, {
                    "a": route, "b": gekozen.replace(":", ""), "c": reden,
                    "d": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
                st.success("📝 Afwijking succesvol gelogd.")