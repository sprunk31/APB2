import streamlit as st
import pandas as pd
# import json # Niet expliciet gebruikt, kan weg als niet indirect nodig
from sqlalchemy import create_engine, text
from datetime import datetime
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
# import itertools # Niet expliciet gebruikt, kan weg
import folium  # Wordt niet direct gebruikt voor de kaart (Pydeck wel)
# from streamlit_folium import st_folium # Niet gebruikt, Pydeck wordt gebruikt
from geopy.distance import geodesic
from collections import Counter
import pydeck as pdk

# ─── CENTRALE REFRESH AFHANDELING (VOOR ENIGE UI OUTPUT) ───────────────────
if "refresh_needed" not in st.session_state:
    st.session_state.refresh_needed = False

if st.session_state.refresh_needed:
    st.cache_data.clear()  # Leeg alle @st.cache_data functies
    st.cache_resource.clear()  # Overweeg ook @st.cache_resource te legen als de engine opnieuw moet worden gemaakt (meestal niet nodig)
    st.session_state.refresh_needed = False
    # Geen st.rerun() hier, de normale script flow gaat verder en
    # alle data-ophalende functies zullen verse data laden.


# ─── SESSIESTATE INITIALISATIE ───────────────────
def init_session_state():
    defaults = {
        "authenticated": False,
        "gebruiker": None,
        "temp_gebruiker_select": "Delft",  # Default voor gebruikerkeuze selectbox
        "app_rol": "Gebruiker",  # Default rol
        "op_route": False,
        "selected_type": None,  # Zal later worden gevalideerd/ingesteld
        "geselecteerde_routes": [],  # Zal later worden gevalideerd/ingesteld
        "extra_meegegeven_tijdelijk": [],
        "routes_cache": None,  # Voor de kaart na upload, indien nog relevant
        # refresh_needed wordt al bovenaan afgehandeld/geïnitialiseerd
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


init_session_state()

# ─── LOGIN ───────────────────────────────────────
if not st.session_state.authenticated:
    st.markdown("## 🔐 Log in om toegang te krijgen")
    username = st.text_input("Gebruikersnaam", key="login_user")
    password = st.text_input("Wachtwoord", type="password", key="login_pass")
    if st.button("Inloggen", key="login_button"):  # Key toegevoegd voor duidelijkheid
        creds = st.secrets["credentials"]
        if username == creds["username"] and password == creds["password"]:
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("❌ Ongeldige gebruikersnaam of wachtwoord")
    st.stop()

# ─── GEBRUIKER KEUZE ─────────────────────────────
if st.session_state.authenticated and st.session_state.gebruiker is None:
    with st.sidebar:
        st.header("👤 Kies je gebruiker")
        st.selectbox("Gebruiker", ["Delft", "Den Haag"], key="temp_gebruiker_select")

        if st.button("Bevestig gebruiker", key="confirm_user_button"):
            st.session_state.gebruiker = st.session_state.temp_gebruiker_select
            st.success(f"✅ Ingeset als gebruiker: {st.session_state.gebruiker}")
            # Reset afhankelijke filters voor een schone staat voor de nieuwe gebruiker
            st.session_state.selected_type = None
            st.session_state.geselecteerde_routes = []
            st.session_state.op_route = False
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


def run_query(query, params=None):
    with get_engine().connect() as conn:
        return pd.read_sql(text(query), conn, params=params)


def execute_query(query, params=None):
    with get_engine().begin() as conn:
        conn.execute(text(query), params or {})


# ─── GECACHEDE QUERIES ──────────────────────────
@st.cache_data(ttl=300)
def get_df_sidebar_data():  # Hernoemd voor duidelijkheid
    df = run_query("SELECT * FROM apb_containers")
    if not df.empty:
        df["fill_level"] = pd.to_numeric(df["fill_level"], errors="coerce")
        df["extra_meegegeven"] = df["extra_meegegeven"].astype(bool)
    return df


@st.cache_data(ttl=300)
def get_df_routes_data():  # Hernoemd voor duidelijkheid
    return run_query("""
        SELECT r.route_omschrijving, r.omschrijving AS container_name,
               r.datum, c.container_location, c.content_type
        FROM apb_routes r
        JOIN apb_containers c ON r.omschrijving = c.container_name
        WHERE r.datum >= current_date AND c.container_location IS NOT NULL AND c.container_location <> ''
    """)  # Extra check op lege container_location


@st.cache_data(ttl=300)
def get_df_all_containers_data():  # Hernoemd voor duidelijkheid
    return run_query("""
        SELECT container_name, container_location, content_type, fill_level, address, city
        FROM apb_containers
        WHERE container_location IS NOT NULL AND container_location <> ''
    """)  # Extra check op lege container_location


# ─── PAGINA INSTELLINGEN ────────────────────────
st.set_page_config(page_title="Afvalcontainerbeheer", layout="wide")
st.title("♻️ Afvalcontainerbeheer Dashboard")

# ─── SIDEBAR ─────────────────────────────────────
# Globale dataframes die in de sidebar en mogelijk tabs worden gebruikt
# Deze worden eenmalig per rerun (na eventuele cache clear) geladen.
try:
    df_sidebar = get_df_sidebar_data()
except Exception as e:
    st.error(f"❌ Fout bij laden van algemene containerdata: {e}")
    df_sidebar = pd.DataFrame()

try:
    df_routes_full_for_filters = get_df_routes_data()
except Exception as e:
    st.error(f"❌ Fout bij laden van data voor route filters: {e}")
    df_routes_full_for_filters = pd.DataFrame()

with st.sidebar:
    st.header("🔧 Instellingen")
    st.selectbox("👤 Kies je rol:", ["Gebruiker", "Upload"], key="app_rol")
    st.markdown(f"**Ingelogd als:** {st.session_state.gebruiker}")

    if st.session_state.app_rol == "Gebruiker":
        st.markdown("### 🔎 Filters")

        # Filter voor content_type
        available_types = []
        if not df_sidebar.empty and "content_type" in df_sidebar.columns:
            available_types = sorted(df_sidebar["content_type"].dropna().unique())

        if not available_types:  # Als er geen types zijn
            st.session_state.selected_type = None
        # Als de huidige selectie None is OR niet (meer) in de beschikbare types voorkomt
        elif st.session_state.selected_type is None or st.session_state.selected_type not in available_types:
            st.session_state.selected_type = available_types[0]  # Val terug op de eerste beschikbare

        current_selected_type_index = 0
        if st.session_state.selected_type and available_types:  # Als er een valide selectie en types zijn
            try:
                current_selected_type_index = available_types.index(st.session_state.selected_type)
            except ValueError:  # Fallback voor het zeldzame geval dat index niet gevonden wordt
                st.session_state.selected_type = available_types[0] if available_types else None
                current_selected_type_index = 0

        st.selectbox(
            "Content type",
            options=available_types,
            index=current_selected_type_index,
            key="selected_type",
            disabled=not available_types
        )
        if not available_types:
            st.caption("Geen content types beschikbaar om te filteren.")

        # Filter voor op_route (Ja/Nee)
        st.toggle("Alleen containers op route?", key="op_route",
                  help="Filter containers in het dashboard op basis of ze 'op route' staan.")

        st.markdown("### 🚚 Routeselectie (voor kaart)")
        available_routes_for_multiselect = []
        if not df_routes_full_for_filters.empty and "route_omschrijving" in df_routes_full_for_filters.columns:
            available_routes_for_multiselect = sorted(
                df_routes_full_for_filters["route_omschrijving"].dropna().unique())

        if not available_routes_for_multiselect:  # Als er geen routes zijn
            st.session_state.geselecteerde_routes = []
        else:  # Valideer bestaande selectie tegen beschikbare routes
            st.session_state.geselecteerde_routes = [
                route for route in st.session_state.geselecteerde_routes if route in available_routes_for_multiselect
            ]

        st.multiselect(
            label="Selecteer routes voor kaart:",
            options=available_routes_for_multiselect,
            key="geselecteerde_routes",
            help="Selecteer één of meerdere routes om op de kaart te tonen.",
            placeholder="Klik om routes te selecteren",
            disabled=not available_routes_for_multiselect
        )
        if not available_routes_for_multiselect and df_routes_full_for_filters.empty:
            st.info("📬 Geen routes beschikbaar voor selectie. Upload eerst data.")
        elif not available_routes_for_multiselect and not df_routes_full_for_filters.empty:
            st.caption("Geen unieke route omschrijvingen gevonden in de huidige data.")

    elif st.session_state.app_rol == "Upload":
        st.markdown("### 📤 Upload bestanden")
        file1 = st.file_uploader("🟢 Bestand van Abel (containers)", type=["xlsx"], key="upload_abel")
        file2 = st.file_uploader("🔵 Bestand van Pieterbas (routes)", type=["xlsx"], key="upload_pb")

        if st.button("Start verwerking uploads", key="process_uploads_button", disabled=not (file1 and file2)):
            if file1 and file2:  # Dubbele check, hoewel button disabled is
                try:
                    # 📥 1. Lees en verwerk bestanden
                    df_containers_upload = pd.read_excel(file1)
                    df_containers_upload.columns = df_containers_upload.columns.str.strip().str.lower().str.replace(" ",
                                                                                                                    "_")
                    df_containers_upload.rename(columns={"fill_level_(%)": "fill_level"}, inplace=True)

                    df_routes_upload = pd.read_excel(file2)

                    # 🧹 2. Filter en verrijk containerdata
                    df_containers_upload = df_containers_upload[
                        (df_containers_upload['operational_state'] == 'In use') &
                        (df_containers_upload['status'] == 'In use') &
                        (df_containers_upload['on_hold'] == 'No')
                        ].copy()
                    df_containers_upload["content_type"] = df_containers_upload["content_type"].apply(
                        lambda x: "Glas" if "glass" in str(x).lower() else x
                    )
                    # Controleer of 'location_code' en 'content_type' bestaan voordat groupby wordt gebruikt
                    if "location_code" in df_containers_upload.columns and "content_type" in df_containers_upload.columns:
                        df_containers_upload["combinatietelling"] = df_containers_upload.groupby(
                            ["location_code", "content_type"]
                        )["content_type"].transform("count")
                        if "fill_level" in df_containers_upload.columns:
                            df_containers_upload["gemiddeldevulgraad"] = df_containers_upload.groupby(
                                ["location_code", "content_type"]
                            )["fill_level"].transform("mean")
                        else:
                            df_containers_upload["gemiddeldevulgraad"] = 0  # of pd.NA
                    else:
                        df_containers_upload["combinatietelling"] = 1  # of pd.NA
                        df_containers_upload["gemiddeldevulgraad"] = 0  # of pd.NA

                    df_containers_upload["oproute"] = df_containers_upload["container_name"].isin(
                        df_routes_upload["Omschrijving"].values).map(
                        {True: "Ja", False: "Nee"}
                    )
                    df_containers_upload["extra_meegegeven"] = False

                    # Definieer vereiste kolommen en selecteer ze, voeg ontbrekende toe met defaults
                    required_cols = [
                        "container_name", "address", "city", "location_code", "content_type",
                        "fill_level", "container_location", "combinatietelling",
                        "gemiddeldevulgraad", "oproute", "extra_meegegeven"
                    ]
                    for col in required_cols:
                        if col not in df_containers_upload.columns:
                            df_containers_upload[col] = pd.NA  # Of een andere geschikte default

                    df_containers_upload = df_containers_upload[required_cols]

                    # 🚀 3. Tabel legen en data snel opnieuw invoegen
                    engine = get_engine()
                    with engine.begin() as conn:
                        conn.execute(text(
                            "TRUNCATE TABLE apb_containers RESTART IDENTITY CASCADE"))  # CASCADE voor afhankelijkheden
                    df_containers_upload.to_sql("apb_containers", engine, if_exists="append", index=False)

                    # 📦 4. Verwerk routes
                    df_routes_upload = df_routes_upload.rename(columns={
                        "Route Omschrijving": "route_omschrijving",
                        "Omschrijving": "omschrijving",
                        "Datum": "datum"
                    })
                    df_routes_upload = df_routes_upload[
                        ["route_omschrijving", "omschrijving", "datum"]].drop_duplicates()
                    with engine.begin() as conn:
                        conn.execute(text("TRUNCATE TABLE apb_routes RESTART IDENTITY CASCADE"))  # CASCADE
                    df_routes_upload.to_sql("apb_routes", engine, if_exists="append", index=False)

                    # 🗺️ 5. Route-cache bijwerken (optioneel, als de kaart direct state.routes_cache gebruikt)
                    # De kaart laadt nu zijn eigen data, dus dit is mogelijk niet strikt nodig.
                    # Als het wel nodig is, laad de data opnieuw uit de DB:
                    # df_routes_for_cache = get_df_routes_data() # Gebruik de gecachete functie
                    # ... verdere verwerking voor cache ...
                    # st.session_state["routes_cache"] = df_routes_for_cache_processed

                    # 🧮 6. Log aantal volle containers
                    if "fill_level" in df_containers_upload.columns:
                        aantal_volle = int(
                            (pd.to_numeric(df_containers_upload["fill_level"], errors='coerce').fillna(0) >= 80).sum())
                        vandaag = datetime.now().date()
                        with engine.begin() as conn:
                            conn.execute(text("""
                                INSERT INTO apb_logboek_totaal (datum, aantal_volle_bakken)
                                VALUES (:datum, :aantal)
                                ON CONFLICT (datum)
                                DO UPDATE SET aantal_volle_bakken = EXCLUDED.aantal_volle_bakken
                            """), {"datum": vandaag, "aantal": aantal_volle})

                    # ✅ 7. Afronden
                    st.success("✅ Gegevens succesvol geüpload en verwerkt.")
                    st.session_state.refresh_needed = True
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Fout bij verwerken van bestanden: {e}")
                    st.exception(e)  # Toon volledige traceback voor debuggen

# ─── TABS ─────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["📊 Dashboard", "🗺️ Kaartweergave", "📋 Route-status"])

# ─── TAB 1: DASHBOARD ────────────────────────────
with tab1:
    st.header("📊 Dashboard Overzicht")
    # Gebruik df_sidebar die bovenaan is geladen en al up-to-date zou moeten zijn
    if df_sidebar.empty:
        st.warning("Geen containerdata beschikbaar voor het dashboard. Upload eventueel nieuwe data.")
        # Maak een lege dataframe om fouten in de rest van de tab te voorkomen
        df_dashboard = pd.DataFrame(columns=[
            "container_name", "address", "city", "location_code", "content_type",
            "fill_level", "combinatietelling", "gemiddeldevulgraad", "oproute", "extra_meegegeven"
        ])
    else:
        df_dashboard = df_sidebar.copy()
        # Zorg dat types correct zijn voor bewerkingen; dit gebeurt ook in get_df_sidebar_data
        df_dashboard["fill_level"] = pd.to_numeric(df_dashboard["fill_level"], errors="coerce")
        df_dashboard["extra_meegegeven"] = df_dashboard["extra_meegegeven"].astype(bool)

    # KPI's
    try:
        df_logboek_kpi = run_query("SELECT gebruiker FROM apb_logboek_afvalcontainers WHERE DATE(datum) = CURRENT_DATE")
        counts_kpi = df_logboek_kpi["gebruiker"].value_counts().to_dict()
        delft_count_kpi = counts_kpi.get("Delft", 0)
        denhaag_count_kpi = counts_kpi.get("Den Haag", 0)
    except Exception as e:
        # st.error(f"Fout bij laden KPI data: {e}") # Optioneel tonen
        delft_count_kpi = denhaag_count_kpi = 0

    k1, k2, k3 = st.columns(3)
    k1.metric("📦 Totaal containers", len(df_dashboard))
    k2.metric("📊 Vulgraad ≥ 80%", (df_dashboard["fill_level"].fillna(0) >= 80).sum())
    k3.metric("🧍 Extra meegegeven (vandaag)", f"{delft_count_kpi} / {denhaag_count_kpi}")

    # Filters toepassen
    if st.session_state.selected_type:  # Alleen filteren als een type is geselecteerd
        df_dashboard = df_dashboard[df_dashboard["content_type"] == st.session_state.selected_type]

    # Filter op basis van de 'op_route' toggle in de sidebar
    df_dashboard = df_dashboard[df_dashboard["oproute"] == ("Ja" if st.session_state.op_route else "Nee")]

    zichtbare_kolommen = [
        "container_name", "address", "city", "location_code", "content_type",
        "fill_level", "combinatietelling", "gemiddeldevulgraad", "oproute", "extra_meegegeven"
    ]
    # Zorg dat alle zichtbare kolommen bestaan in df_dashboard
    for col in zichtbare_kolommen:
        if col not in df_dashboard.columns:
            df_dashboard[col] = pd.NA  # Of een passende default

    df_dashboard_display = df_dashboard[zichtbare_kolommen].copy()

    # Bewerkbare containers
    bewerkbaar_df = df_dashboard_display[~df_dashboard_display["extra_meegegeven"]].copy()
    # Zorg dat fill_level en gemiddeldevulgraad numeriek zijn voor vergelijking
    bewerkbaar_df["fill_level"] = pd.to_numeric(bewerkbaar_df["fill_level"], errors="coerce").fillna(0)
    bewerkbaar_df["gemiddeldevulgraad"] = pd.to_numeric(bewerkbaar_df["gemiddeldevulgraad"], errors="coerce").fillna(0)

    bewerkbaar_df = bewerkbaar_df[
        (bewerkbaar_df["gemiddeldevulgraad"] > 45) |
        (bewerkbaar_df["fill_level"] > 80)
        ].sort_values("gemiddeldevulgraad", ascending=False)

    st.subheader("✏️ Bewerkbare containers (selecteer om extra mee te geven)")
    if not bewerkbaar_df.empty:
        gb = GridOptionsBuilder.from_dataframe(bewerkbaar_df)
        gb.configure_default_column(filter=True, resizable=True)
        gb.configure_column("extra_meegegeven", editable=True, cellEditor='agCheckboxCellEditor')
        gb.configure_selection(selection_mode="multiple", use_checkbox=True)  # Optioneel: selectie via checkbox
        grid_options = gb.build()

        ag_grid_response = AgGrid(
            bewerkbaar_df,
            gridOptions=grid_options,
            update_mode=GridUpdateMode.MODEL_CHANGED,  # Of VALUE_CHANGED als dat beter werkt
            height=400,
            fit_columns_on_grid_load=True,
            allow_unsafe_jscode=True  # Alleen als nodig voor custom JS
        )
        updated_df_from_grid = pd.DataFrame(ag_grid_response["data"])
        if not updated_df_from_grid.empty:
            # Omdat de grid mogelijk alleen gewijzigde rijen teruggeeft of de boolean niet goed parst
            st.session_state.extra_meegegeven_tijdelijk = updated_df_from_grid[
                updated_df_from_grid["extra_meegegeven"] == True  # Expliciete check op True
                ]["container_name"].tolist()
        else:
            st.session_state.extra_meegegeven_tijdelijk = []

    else:
        st.info("Geen containers die momenteel voldoen aan de criteria voor bewerking.")
        st.session_state.extra_meegegeven_tijdelijk = []

    if st.button("✅ Geselecteerde containers loggen als extra meegegeven", key="log_extra_button"):
        # We gebruiken de originele dataframe `bewerkbaar_df` en filteren op basis van `extra_meegegeven_tijdelijk`
        # om de volledige rij-informatie te krijgen, aangezien AgGrid soms alleen de gewijzigde cellen teruggeeft.
        # Dit vereist dat container_name uniek is en als index kan dienen.

        containers_to_log_names = st.session_state.extra_meegegeven_tijdelijk
        if containers_to_log_names:
            # Haal de volledige rijen op van de containers die zijn gemarkeerd.
            # We nemen de data direct uit bewerkbaar_df voordat AgGrid het mogelijk heeft aangepast
            # of gebruik de `updated_df_from_grid` als die betrouwbaar de volledige rijen bevat.

            # We werken met `updated_df_from_grid`
            gewijzigde_containers_df = updated_df_from_grid[
                updated_df_from_grid["container_name"].isin(containers_to_log_names) & (
                            updated_df_from_grid["extra_meegegeven"] == True)]

            if not gewijzigde_containers_df.empty:
                try:
                    df_log_check = run_query(
                        "SELECT container_name, DATE(datum) as log_datum FROM apb_logboek_afvalcontainers")
                except:
                    df_log_check = pd.DataFrame(columns=["container_name", "log_datum"])

                vandaag_date = datetime.now().date()
                count_logged = 0
                for _, row in gewijzigde_containers_df.iterrows():
                    is_already_logged_today = False
                    if not df_log_check.empty:
                        is_already_logged_today = (
                                (df_log_check["container_name"] == row["container_name"]) &
                                (pd.to_datetime(df_log_check["log_datum"]).dt.date == vandaag_date)
                        # Vergelijk date objecten
                        ).any()

                    if is_already_logged_today:
                        continue  # Sla over als al gelogd vandaag

                    nm = str(row["container_name"]).strip()  # Zorg dat het een string is
                    execute_query(
                        "UPDATE apb_containers SET extra_meegegeven = TRUE WHERE TRIM(container_name) = :naam",
                        {"naam": nm}
                    )
                    execute_query("""
                        INSERT INTO apb_logboek_afvalcontainers
                        (container_name, address, city, location_code, content_type, fill_level, datum, gebruiker)
                        VALUES (:a, :b, :c, :d, :e, :f, :g, :h)
                    """, {
                        "a": row["container_name"], "b": row["address"], "c": row["city"],
                        "d": row["location_code"], "e": row["content_type"], "f": row["fill_level"],
                        "g": datetime.now(), "h": st.session_state.gebruiker
                    })
                    count_logged += 1

                if count_logged > 0:
                    st.success(f"✔️ {count_logged} containers succesvol gelogd en bijgewerkt.")
                    st.session_state.refresh_needed = True
                    st.session_state.extra_meegegeven_tijdelijk = []  # Reset tijdelijke selectie
                    st.rerun()
                else:
                    st.warning("⚠️ Geen nieuwe containers om te loggen (mogelijk al gelogd vandaag).")
            else:
                st.info("Geen containers geselecteerd in de tabel om te loggen.")
        else:
            st.info("Geen containers geselecteerd in de tabel om te loggen.")

    st.subheader("🔒 Reeds als extra meegegeven gemarkeerde containers")
    reeds_gemarkeerd_df = df_dashboard_display[df_dashboard_display["extra_meegegeven"]].copy()
    if not reeds_gemarkeerd_df.empty:
        st.dataframe(reeds_gemarkeerd_df, use_container_width=True, hide_index=True)
    else:
        st.info("Nog geen containers gemarkeerd als extra meegegeven voor de huidige selectie.")

# ─── TAB 2: KAART ─────────────────────────────────
with tab2:
    st.header("🗺️ Containerkaart")

    # Data laadfuncties specifiek voor de kaart (deze zijn al gecached)
    try:
        df_routes_map = get_df_routes_data()  # Gebruik de algemene functie
        if not df_routes_map.empty and "container_location" in df_routes_map.columns:
            # Definieer _parse lokaal of globaal als het vaker nodig is
            def _parse_coords(loc_str):
                try:
                    return tuple(map(float, loc_str.split(",")))
                except:
                    return (None, None)  # Robuuster voor foute data


            # Pas _parse_coords toe en maak lat/lon kolommen
            coords = df_routes_map["container_location"].apply(
                lambda x: pd.Series(_parse_coords(x) if pd.notna(x) else (None, None)))
            if not coords.empty:  # Voorkom error als coords leeg is
                df_routes_map[['r_lat', 'r_lon']] = coords

        else:  # Zorg dat kolommen bestaan, zelfs als leeg
            df_routes_map['r_lat'] = pd.Series(dtype='float')
            df_routes_map['r_lon'] = pd.Series(dtype='float')

    except Exception as e:
        st.error(f"Fout bij laden routedata voor kaart: {e}")
        df_routes_map = pd.DataFrame(
            columns=['route_omschrijving', 'container_name', 'container_location', 'content_type', 'fill_level',
                     'address', 'city', 'r_lat', 'r_lon'])

    try:
        df_all_containers_map = get_df_all_containers_data()  # Gebruik de algemene functie
        if not df_all_containers_map.empty and "container_location" in df_all_containers_map.columns:
            def _parse_coords(loc_str):  # Herdefinieer of maak globaal
                try:
                    return tuple(map(float, loc_str.split(",")))
                except:
                    return (None, None)


            coords_all = df_all_containers_map["container_location"].apply(
                lambda x: pd.Series(_parse_coords(x) if pd.notna(x) else (None, None)))
            if not coords_all.empty:
                df_all_containers_map[['lat', 'lon']] = coords_all
        else:  # Zorg dat kolommen bestaan, zelfs als leeg
            df_all_containers_map['lat'] = pd.Series(dtype='float')
            df_all_containers_map['lon'] = pd.Series(dtype='float')

    except Exception as e:
        st.error(f"Fout bij laden containerdata voor kaart: {e}")
        df_all_containers_map = pd.DataFrame(
            columns=['container_name', 'container_location', 'content_type', 'fill_level', 'address', 'city', 'lat',
                     'lon'])

    selected_route_names_map = st.session_state.geselecteerde_routes  # Uit sidebar
    handmatig_selected_names_map = st.session_state.extra_meegegeven_tijdelijk  # Uit Tab 1 AgGrid

    df_handmatig_map = df_all_containers_map[
        df_all_containers_map["container_name"].isin(handmatig_selected_names_map)].copy()
    df_handmatig_map.dropna(subset=['lat', 'lon'], inplace=True)  # Verwijder rijen zonder valide coördinaten


    # Functie voor dichtstbijzijnde route (blijft hetzelfde)
    def find_nearest_route(container_row, routes_df):
        if pd.isna(container_row["lat"]) or pd.isna(container_row["lon"]) or routes_df.empty:
            return None

        container_coords = (container_row["lat"], container_row["lon"])
        min_dist = float('inf')
        nearest_route_name = None
        radius_km = 0.15  # Start radius

        # Filter routes_df op hetzelfde content_type voor efficiëntie
        relevant_routes = routes_df[
            (routes_df["content_type"] == container_row["content_type"]) &
            routes_df['r_lat'].notna() & routes_df['r_lon'].notna()  # Alleen routes met valide coördinaten
            ]
        if relevant_routes.empty:
            return None  # Geen relevante routes om mee te vergelijken

        # Zoek binnen een groeiende radius voor prestaties
        # Dit deel kan nog steeds traag zijn bij veel punten. Overweeg georuimtelijke indexering als dit een bottleneck is.
        # Voor nu houden we de bestaande logica aan, maar met een limiet.

        # Eenvoudigere benadering: vind de dichtstbijzijnde binnen de relevante routes
        # Deze loop is niet ideaal voor performance bij veel punten.
        # De Counter logica was om de meest voorkomende route te vinden als er meerdere matches binnen de radius waren.
        # Als we simpelweg de *dichtstbijzijnde* willen:

        distances = []
        for _, route_point in relevant_routes.iterrows():
            route_coords = (route_point["r_lat"], route_point["r_lon"])
            dist = geodesic(container_coords, route_coords).km
            distances.append({'name': route_point["route_omschrijving"], 'dist': dist})

        if not distances:
            return None

        # Vind de dichtstbijzijnde route_omschrijving
        # Als meerdere punten van dezelfde route dichtbij zijn, kan dit nog verfijnd worden.
        # Nu pakken we de route van het absoluut dichtstbijzijnde routepunt.

        closest = min(distances, key=lambda x: x['dist'])
        if closest['dist'] <= 5:  # Alleen als binnen 5km
            return closest['name']
        return None


    if not df_handmatig_map.empty:
        # Zorg ervoor dat df_routes_map de benodigde kolommen 'r_lat', 'r_lon', 'content_type', 'route_omschrijving' heeft.
        df_handmatig_map["dichtstbijzijnde_route"] = df_handmatig_map.apply(find_nearest_route, axis=1,
                                                                            routes_df=df_routes_map)
    else:
        df_handmatig_map["dichtstbijzijnde_route"] = None

    kleuren_map_list = [
        [255, 0, 0], [0, 100, 255], [0, 255, 0], [255, 165, 0], [160, 32, 240],
        [0, 206, 209], [255, 105, 180], [255, 255, 0], [139, 69, 19], [0, 128, 128]
    ]
    route_kleur_map = {route: kleuren_map_list[i % len(kleuren_map_list)] + [175] for i, route in
                       enumerate(selected_route_names_map)}

    pydeck_layers = []

    # Routes geselecteerd in sidebar
    for route_naam in selected_route_names_map:
        df_r_display = df_routes_map[df_routes_map["route_omschrijving"] == route_naam].copy()
        df_r_display.dropna(subset=['r_lat', 'r_lon'], inplace=True)  # Essentieel voor Pydeck
        if not df_r_display.empty:
            df_r_display["tooltip_label"] = df_r_display.apply(
                lambda
                    row: f"<b>🧺 {row['container_name']}</b><br>Type: {row['content_type']}<br>Vulgraad: {row.get('fill_level', 'N/B')}%<br>Route: {row['route_omschrijving'] or '—'}<br>Locatie: {row.get('address', 'N/B')}, {row.get('city', 'N/B')}",
                axis=1
            )
            pydeck_layers.append(pdk.Layer(
                "ScatterplotLayer",
                data=df_r_display,
                get_position='[r_lon, r_lat]',
                get_fill_color=route_kleur_map[route_naam],
                get_radius=60,  # Radius in meters
                # radiusMinPixels=4, radiusMaxPixels=6, # Alternatief
                pickable=True,
                get_line_color=[0, 0, 0, 80],  # Donkere rand
                line_width_min_pixels=1
            ))

    # Handmatig geselecteerde containers (uit Tab 1)
    if not df_handmatig_map.empty:
        df_handmatig_map["tooltip_label"] = df_handmatig_map.apply(
            lambda
                row: f"<b>🖤 {row['container_name']} (Extra)</b><br>Type: {row['content_type']}<br>Vulgraad: {row.get('fill_level', 'N/B')}%<br>Mog. route: {row.get('dichtstbijzijnde_route', '—')}<br>Locatie: {row.get('address', 'N/B')}, {row.get('city', 'N/B')}",
            axis=1
        )
        pydeck_layers.append(pdk.Layer(
            "ScatterplotLayer",
            data=df_handmatig_map,  # Al gefilterd op dropna lat/lon
            get_position='[lon, lat]',
            get_fill_color='[0, 0, 0, 220]',  # Zwart
            get_radius=80,  # Iets groter
            pickable=True
        ))

    pydeck_tooltip = {
        "html": "{tooltip_label}",
        "style": {"backgroundColor": "rgba(0,0,0,0.8)", "color": "white", "border": "1px solid white", "padding": "5px"}
    }

    # Bepaal middelpunt voor de kaart
    kaart_midpoint = [52.01, 4.36]  # Default (Delft)
    all_points_for_mid = []
    if not df_routes_map.empty and 'r_lat' in df_routes_map and 'r_lon' in df_routes_map:
        all_points_for_mid.append(df_routes_map[['r_lat', 'r_lon']].rename(columns={'r_lat': 'lat', 'r_lon': 'lon'}))
    if not df_handmatig_map.empty and 'lat' in df_handmatig_map and 'lon' in df_handmatig_map:  # df_handmatig_map heeft al 'lat'/'lon'
        all_points_for_mid.append(df_handmatig_map[['lat', 'lon']])

    if all_points_for_mid:
        combined_points_df = pd.concat(all_points_for_mid).dropna()
        if not combined_points_df.empty:
            kaart_midpoint = [combined_points_df["lat"].mean(), combined_points_df["lon"].mean()]

    if pydeck_layers:
        st.pydeck_chart(pdk.Deck(
            map_style="mapbox://styles/mapbox/streets-v12",  # of 'light-v10', 'dark-v10'
            initial_view_state=pdk.ViewState(
                latitude=kaart_midpoint[0], longitude=kaart_midpoint[1],
                zoom=11, pitch=30  # Lichte pitch voor 3D effect
            ),
            layers=pydeck_layers, tooltip=pydeck_tooltip
        ))
    else:
        st.info("Geen containers geselecteerd of beschikbaar om op de kaart te tonen.")

    if not df_handmatig_map.empty:
        st.markdown("### 📋 Details handmatig geselecteerde containers (extra mee te geven)")
        st.dataframe(df_handmatig_map[[
            "container_name", "address", "city", "content_type",
            "fill_level", "dichtstbijzijnde_route"
        ]], use_container_width=True, hide_index=True)

# ─── TAB 3: ROUTE STATUS ─────────────────────────
with tab3:
    st.header("🚣️ Route status")
    try:
        df_routes_tab3 = get_df_routes_data()  # Haal alle actuele routes op
        available_routes_tab3 = sorted(
            df_routes_tab3["route_omschrijving"].dropna().unique()) if not df_routes_tab3.empty else []
    except Exception as e:
        st.error(f"Fout bij laden routes voor status update: {e}")
        available_routes_tab3 = []

    selected_route_tab3 = st.selectbox(
        "Kies een route",
        options=available_routes_tab3,
        key="route_status_select",
        disabled=not available_routes_tab3
    )

    status_opties_tab3 = ["Actueel", "Gedeeltelijk niet gereden door:", "Volledig niet gereden door:"]
    gekozen_status_tab3 = st.selectbox("Status", status_opties_tab3, key="route_status_gekozen")

    reden_tab3 = ""
    if "niet gereden" in gekozen_status_tab3:
        reden_tab3 = st.text_input("Reden (verplicht indien niet actueel)", key="route_status_reden")

    if st.button("✅ Bevestig status", key="confirm_route_status_button", disabled=not selected_route_tab3):
        if not selected_route_tab3:  # Zou niet moeten gebeuren als button niet disabled is
            st.warning("Selecteer eerst een route.")
        elif "niet gereden" in gekozen_status_tab3 and not reden_tab3:
            st.warning("⚠️ Reden is verplicht als de route niet (volledig) actueel is.")
        else:
            vandaag_str = datetime.now().strftime("%Y-%m-%d")
            if gekozen_status_tab3 == "Actueel":
                # Verwijder eventuele eerdere logs voor deze route op deze dag
                # Dit is een simpele implementatie; een 'soft delete' of update kan beter zijn.
                try:
                    # Eerst checken of er iets te verwijderen is kan efficiënter zijn.
                    # Voor nu, directe delete.
                    execute_query(
                        "DELETE FROM public.apb_logboek_route WHERE route = :route AND DATE(datum) = :datum_vandaag",
                        {"route": selected_route_tab3, "datum_vandaag": vandaag_str}
                    )
                    st.success(
                        f"Status voor route '{selected_route_tab3}' is ingesteld op 'Actueel' voor vandaag. Eventuele eerdere afwijkingen voor vandaag zijn verwijderd.")
                except Exception as e:
                    st.error(f"Fout bij bijwerken logboek voor actuele status: {e}")
            else:
                try:
                    # Bij een afwijking, altijd een nieuwe entry of een update als er al een is.
                    # Huidige logica voegt toe, wat meerdere entries per dag per route kan geven.
                    # Een ON CONFLICT DO UPDATE zou beter zijn als route+datum uniek moet zijn.
                    # Voor nu, houden we de insert-logica aan.
                    execute_query(
                        """INSERT INTO apb_logboek_route (route, status, reden, datum)
                           VALUES (:a, :b, :c, :d)""",
                        {
                            "a": selected_route_tab3,
                            "b": gekozen_status_tab3.replace(":", ""),  # Verwijder dubbele punt
                            "c": reden_tab3,
                            "d": datetime.now()  # Volledige timestamp
                        }
                    )
                    st.success(f"📝 Afwijking voor route '{selected_route_tab3}' succesvol gelogd.")
                except Exception as e:
                    st.error(f"Fout bij loggen van afwijking: {e}")

            st.session_state.refresh_needed = True  # Refresh data in andere tabs (bijv. KPIs)
            st.rerun()  # Rerun om de UI te verversen

    if not available_routes_tab3:
        st.info("Geen routes beschikbaar om status voor bij te werken. Upload eventueel routedata.")

    st.subheader("📜 Huidige afwijkingen vandaag")
    try:
        df_logboek_route_vandaag = run_query(
            "SELECT route, status, reden, to_char(datum, 'HH24:MI') as tijdstip FROM apb_logboek_route WHERE DATE(datum) = CURRENT_DATE ORDER BY datum DESC")
        if not df_logboek_route_vandaag.empty:
            st.dataframe(df_logboek_route_vandaag, use_container_width=True, hide_index=True)
        else:
            st.info("Geen afwijkingen gelogd voor vandaag.")
    except Exception as e:
        st.error(f"Fout bij laden logboek route afwijkingen: {e}")
