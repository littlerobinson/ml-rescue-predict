import streamlit as st
import geopandas as gpd
import folium
from streamlit_folium import st_folium
from datetime import datetime, timedelta
import pandas as pd
import io
import requests

# Définir la page en plein écran
st.set_page_config(layout="wide")

# Titre
st.title("Prédiction des accidents pour la journée du 8 mars 2025")

# Fonction pour télécharger le dataset public depuis S3
@st.cache_data
def charger_dataset():
    url = "https://streamlit-open.s3.eu-west-3.amazonaws.com/output_with_predictions_mapping.csv"
    response = requests.get(url)
    df = pd.read_csv(io.StringIO(response.text))
    return df

# Charger le dataset
df_accidents = charger_dataset()

# Filtrer uniquement pour le 8 mars 2025
date_selectionnee = "2025-03-08"
df_accidents_filtre = df_accidents[(df_accidents['year'] == 2025) &
                                   (df_accidents['month'] == 3) &
                                   (df_accidents['day'] == 8)]

# Charger la carte des communes de Seine-et-Marne (77)
@st.cache_data
def charger_carte():
    url_geojson = "https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/departements/77-seine-et-marne/communes-77-seine-et-marne.geojson"
    gdf = gpd.read_file(url_geojson)
    return gdf

gdf = charger_carte()

# Associer les données des accidents aux communes
gdf["accident"] = gdf["code"].astype(str).map(lambda x: 1 if x in df_accidents_filtre["com"].astype(str).tolist() else 0)

# Création de la carte avec un meilleur ajustement
bounds = gdf.total_bounds  # [minx, miny, maxx, maxy]
m = folium.Map(location=[(bounds[1] + bounds[3]) / 2, (bounds[0] + bounds[2]) / 2], zoom_start=9)

# Ajouter les communes avec couleur en fonction des accidents
for _, row in gdf.iterrows():
    couleur = "#8B0000" if row["accident"] == 1 else "#008000"  # Rouge foncé et vert foncé
    folium.GeoJson(
        row.geometry,
        tooltip=row['nom'],
        style_function=lambda feature, couleur=couleur: {
            'fillColor': couleur,
            'color': 'black',
            'weight': 1,
            'fillOpacity': 0.8
        }
    ).add_to(m)

# Ajuster la carte pour afficher tout le département
m.fit_bounds([[bounds[1], bounds[0]], [bounds[3], bounds[2]]])

# Mise en page avec colonnes
col1, col2 = st.columns([3, 1])

with col1:
    st.subheader("Carte des accidents")
    st_folium(m, width=1000, height=800)

with col2:
    st.subheader("🌦️ Météo du 8 mars")
    meteo = df_accidents_filtre.iloc[0]
    st.markdown(f"**🌡 Température :** {meteo['temp']}°C")
    st.markdown(f"**🥶 Ressenti :** {meteo['feels_like']}°C")
    st.markdown(f"**⬇️ Temp. min :** {meteo['temp_min']}°C")
    st.markdown(f"**⬆️ Temp. max :** {meteo['temp_max']}°C")
    st.markdown(f"**🧭 Pression :** {meteo['pressure']} hPa")
    st.markdown(f"**💧 Humidité :** {meteo['humidity']}%")
    st.markdown(f"**💨 Vent :** {meteo['wind_speed']} m/s")
    
    # Vacances scolaires
    st.subheader("🏫 Vacances scolaires")
    zone_a = "✅ Oui" if meteo['vacances_Zone_A'] == 1 else "❌ Non"
    zone_b = "✅ Oui" if meteo['vacances_Zone_B'] == 1 else "❌ Non"
    zone_c = "✅ Oui" if meteo['vacances_Zone_C'] == 1 else "❌ Non"
    st.markdown(f"**Zone A :** {zone_a}")
    st.markdown(f"**Zone B :** {zone_b}")
    st.markdown(f"**Zone C :** {zone_c}")
    
    # Jour férié
    st.subheader("📅 Jour férié")
    jour_ferie = "✅ Oui" if meteo['jour_ferie'] == 1 else "❌ Non"
    st.markdown(f"**Jour férié :** {jour_ferie}")
    
    # Légende
    st.subheader("🗺️ Légende")
    st.markdown("🟥 **Rouge foncé** : Au moins 1 accident prédit")
    st.markdown("🟩 **Vert foncé** : Pas d'accident prédit")
