# src/extract/air_quality.py
"""Extract air-quality forecast from Open-Meteo Air Quality API."""
import requests

AIR_QUALITY_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

def fetch_air_quality_for_locations(locations: list[dict], forecast_days: int = 7) -> list:
    """
    Fetches hourly air-quality forecast for multiple locations.
    """
    lats = ",".join(str(loc["latitude"]) for loc in locations)
    lons = ",".join(str(loc["longitude"]) for loc in locations)

    params = {
        "latitude": lats,
        "longitude": lons,
        "hourly": "pm2_5,pm10,nitrogen_dioxide,ozone",
        "forecast_days": forecast_days,
        "timezone": "Europe/Tallinn",
    }

    response = requests.get(AIR_QUALITY_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    return data if isinstance(data, list) else [data]
