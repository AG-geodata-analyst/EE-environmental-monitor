# dags/estonia_environmental_monitor.py
import sys
from pathlib import Path

# Make the project root importable so 'from src...' works from any cwd
project_root = str(Path(__file__).resolve().parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import csv
import json
from datetime import datetime, timedelta

import pendulum
from airflow.sdk import dag, task

from src.extract.weather import fetch_weather_for_locations
from src.extract.air_quality import fetch_air_quality_for_locations
from src.validation.quality_checks import (
    validate_weather_data,
    validate_air_quality_data,
)


@dag(
    dag_id="estonia_environmental_monitor",
    start_date=pendulum.datetime(2024, 1, 1, tz="Europe/Tallinn"),
    schedule="0 6 * * *",
    catchup=False,
    tags=["environmental", "estonia", "portfolio"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
)
def estonia_environmental_monitor():

    @task
    def load_locations() -> list[dict]:
        with open("data/locations.csv", mode="r") as infile:
            reader = csv.DictReader(infile)
            return [
                {**row,
                 "latitude": float(row["latitude"]),
                 "longitude": float(row["longitude"])}
                for row in reader
            ]

    @task
    def get_weather(locations: list[dict]) -> list:
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        return fetch_weather_for_locations(locations, start_date, end_date)

    @task
    def get_air_quality(locations: list[dict]) -> list:
        return fetch_air_quality_for_locations(locations)

    @task
    def validate_and_save(weather_data: list, aq_data: list, locations: list[dict]) -> dict:
        valid_weather = validate_weather_data(weather_data, locations)
        valid_aq      = validate_air_quality_data(aq_data, locations)

        with open("data/validated_weather.json", "w") as f:
            json.dump(valid_weather, f, indent=2)
        with open("data/validated_air_quality.json", "w") as f:
            json.dump(valid_aq, f, indent=2)

        summary = {
            "weather_records": len(valid_weather),
            "aq_records":      len(valid_aq),
        }
        print(f"[pipeline] Validation summary: {summary}")
        return summary

    # Wire dependencies
    locations  = load_locations()
    weather    = get_weather(locations)
    aq         = get_air_quality(locations)
    validate_and_save(weather, aq, locations)


estonia_environmental_monitor()
