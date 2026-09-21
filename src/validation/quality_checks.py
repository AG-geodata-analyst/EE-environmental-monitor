# src/validation/quality_checks.py
"""
Data validation for the Estonia Environmental Monitor pipeline.

Uses Pydantic models to enforce schema and range constraints on data
returned by the Open-Meteo APIs, and flattens nested API responses into
flat lists of records that can be loaded into PostgreSQL.
"""

from typing import Optional

from pydantic import BaseModel, Field, ValidationError


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class DailyWeather(BaseModel):
    """A single day of weather observations for one city."""
    city_id: str
    latitude: float
    longitude: float
    date: str                       # ISO date, e.g. "2024-09-21"
    temperature_max: float
    temperature_min: float
    precipitation_sum: float = Field(ge=0)
    wind_speed_max: float = Field(ge=0)


class HourlyAirQuality(BaseModel):
    """A single hourly air-quality observation for one city."""
    city_id: str
    latitude: float
    longitude: float
    time: str                       # ISO datetime, e.g. "2024-09-21T06:00"
    pm2_5: Optional[float] = Field(None, ge=0)
    pm10: Optional[float] = Field(None, ge=0)
    nitrogen_dioxide: Optional[float] = Field(None, ge=0)
    ozone: Optional[float] = Field(None, ge=0)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _city_for_index(locations: list[dict], index: int) -> str:
    """Map a response index back to a city name. Returns 'unknown-N' as fallback."""
    if 0 <= index < len(locations):
        return locations[index]["city"]
    return f"unknown-{index}"


# ---------------------------------------------------------------------------
# Public validation functions
# ---------------------------------------------------------------------------

def validate_weather_data(data: list, locations: list[dict]) -> list[dict]:
    """
    Validate the weather API response and return a flat list of daily records.

    The Open-Meteo historical API returns one object per requested coordinate,
    in the same order as the coordinates were passed in.
    """
    validated: list[dict] = []

    for idx, location_response in enumerate(data):
        city_id = _city_for_index(locations, idx)
        lat = location_response.get("latitude")
        lon = location_response.get("longitude")
        daily = location_response.get("daily", {})

        dates       = daily.get("time", [])
        tmax_list   = daily.get("temperature_2m_max", [])
        tmin_list   = daily.get("temperature_2m_min", [])
        precip_list = daily.get("precipitation_sum", [])
        wind_list   = daily.get("wind_speed_10m_max", [])

        # Every series must have the same length.
        n = len(dates)
        if not all(len(series) == n for series in (tmax_list, tmin_list, precip_list, wind_list)):
            print(f"[validation] Length mismatch for {city_id}, skipping")
            continue

        for i in range(n):
            record = {
                "city_id": city_id,
                "latitude": lat,
                "longitude": lon,
                "date": dates[i],
                "temperature_max": tmax_list[i],
                "temperature_min": tmin_list[i],
                "precipitation_sum": precip_list[i],
                "wind_speed_max": wind_list[i],
            }

            # Skip rows with any missing values.
            if any(v is None for v in record.values()):
                continue

            try:
                validated.append(DailyWeather(**record).model_dump())
            except ValidationError as e:
                print(f"[validation] Dropped bad weather record for {city_id} on {dates[i]}: {e}")

    return validated


def validate_air_quality_data(data: list, locations: list[dict]) -> list[dict]:
    """
    Validate the air-quality API response and return a flat list of hourly records.
    """
    validated: list[dict] = []

    for idx, location_response in enumerate(data):
        city_id = _city_for_index(locations, idx)
        lat = location_response.get("latitude")
        lon = location_response.get("longitude")
        hourly = location_response.get("hourly", {})

        times = hourly.get("time", [])
        pm25  = hourly.get("pm2_5", [])
        pm10  = hourly.get("pm10", [])
        no2   = hourly.get("nitrogen_dioxide", [])
        o3    = hourly.get("ozone", [])

        n = len(times)
        if not all(len(series) == n for series in (pm25, pm10, no2, o3)):
            print(f"[validation] Length mismatch for {city_id}, skipping")
            continue

        for i in range(n):
            record = {
                "city_id": city_id,
                "latitude": lat,
                "longitude": lon,
                "time": times[i],
                "pm2_5": pm25[i],
                "pm10": pm10[i],
                "nitrogen_dioxide": no2[i],
                "ozone": o3[i],
            }

            # AQ values are Optional — a missing pollutant is not a failure.
            try:
                validated.append(HourlyAirQuality(**record).model_dump())
            except ValidationError as e:
                print(f"[validation] Dropped bad AQ record for {city_id} at {times[i]}: {e}")

    return validated
