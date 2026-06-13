# Databricks notebook source
import requests
import json
from datetime import datetime

# COMMAND ----------

BASE_URL = dbutils.secrets.get("weather", "open_meteo_base_url")

# COMMAND ----------

cities = {
    "fes": {"lat": 34.0331, "lon": -5.0003},
    "rabat": {"lat": 34.0209, "lon": -6.8416},
    "casablanca": {"lat": 33.5731, "lon": -7.5898},
    "marrakech": {"lat": 31.6295, "lon": -7.9811},
    "tanger": {"lat": 35.7595, "lon": -5.8339},
    "agadir": {"lat": 30.4278, "lon": -9.5981},
    "meknes": {"lat": 33.8935, "lon": -5.5473},
    "oujda": {"lat": 34.6814, "lon": -1.9086},
    "tetouan": {"lat": 35.5785, "lon": -5.3684},
    "temara": {"lat": 33.9287, "lon": -6.9063},
    "sale": {"lat": 34.0531, "lon": -6.7985},
    "kenitra": {"lat": 34.2610, "lon": -6.5802},
    "nador": {"lat": 35.1681, "lon": -2.9335},
    "laayoune": {"lat": 27.1536, "lon": -13.2033},
    "dakhla": {"lat": 23.6848, "lon": -15.9570},
    "safi": {"lat": 32.2994, "lon": -9.2372},
    "el_jadida": {"lat": 33.2316, "lon": -8.5007},
    "khouribga": {"lat": 32.8811, "lon": -6.9063},
    "beni_mellal": {"lat": 32.3373, "lon": -6.3498},
    "taza": {"lat": 34.2090, "lon": -4.0083}
}


# COMMAND ----------

HOURLY = "temperature_2m,relative_humidity_2m,wind_speed_10m"
forecast_days= 15
past_days= 15

# COMMAND ----------

OUTPUT_PATH = "/Volumes/weather_lakehouse/raw/weather_api_raw"

# COMMAND ----------

timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(f"start_time : {timestamp}")

for city, coords in cities.items():

    lat = coords["lat"]
    lon = coords["lon"]

    url = (
        f"{BASE_URL}"
        f"?latitude={lat}"
        f"&longitude={lon}"
        f"&hourly={HOURLY}"
        f"&forecast_days={forecast_days}"
        f"&past_days={past_days}"
        f"&timezone=Africa/Casablanca"
    )

    response = requests.get(url)
    data = response.json()

    file_name = f"weather_{city}.json"
    full_path = f"{OUTPUT_PATH}/{file_name}"

    dbutils.fs.put(full_path, json.dumps(data), overwrite=True)

    print(f"✔️ Fichier écrit pour {city}: {full_path}")
    
print(f"end_time : {timestamp}") 