# src/extract/air_quality.py
import requests

def fetch_air_quality_for_locations(locations):
    """Fetches hourly air quality data for multiple locations."""
    lats = ",".join(str(loc['latitude']) for loc in locations)
    lons = ",".join(str(loc['longitude']) for loc in locations)

    url = "https://air-quality-api.open-meteo.com/v1/air-quality"
    params = {
        "latitude": lats,
        "longitude": lons,
        "hourly": "pm2_5,pm10,nitrogen_dioxide,ozone",
        "timezone": "Europe/Tallinn",
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    if not isinstance(data, list):
        data = [data]

    return data
