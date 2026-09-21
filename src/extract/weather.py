# src/extract/weather.py
import requests
import pandas as pd
import json

def fetch_weather_for_locations(locations, start_date, end_date):
    """Fetches daily weather data for a list of locations from Open-Meteo."""
    lats = ",".join(str(loc['latitude']) for loc in locations)
    lons = ",".join(str(loc['longitude']) for loc in locations)

    # Use the historical API to get data for a specific date range
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lats,
        "longitude": lons,
        "start_date": start_date,
        "end_date": end_date,
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max",
        "timezone": "Europe/Tallinn",
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    # The API returns a list of structures for multiple locations
    if not isinstance(data, list):
        data = [data]

    return data
