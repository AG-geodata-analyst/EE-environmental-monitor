# src/extract/weather.py
"""Extract weather forecast data from Open-Meteo Forecast API."""
import requests

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

def fetch_weather_for_locations(locations: list[dict], forecast_days: int = 7) -> list:
    """
    Fetches a multi-day weather forecast for a list of locations.

    Parameters
    ----------
    locations : list of dicts with 'latitude' and 'longitude' keys
    forecast_days : number of days to forecast (1-16, default 7)

    Returns
    -------
    list of per-location response dicts (Open-Meteo returns one entry per coordinate)
    """
    lats = ",".join(str(loc["latitude"]) for loc in locations)
    lons = ",".join(str(loc["longitude"]) for loc in locations)

    params = {
        "latitude": lats,
        "longitude": lons,
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max",
        "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
        "forecast_days": forecast_days,
        "timezone": "Europe/Tallinn",
    }

    response = requests.get(FORECAST_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    return data if isinstance(data, list) else [data]
