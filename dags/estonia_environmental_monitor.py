"""
Estonia Environmental Monitor — Airflow DAG

Fetches a 7-day weather and air-quality forecast for six Estonian cities
from the Open-Meteo Forecast APIs, validates it with Pydantic, loads it
into PostgreSQL with UPSERT semantics, runs quality checks, and writes
a public JSON file for the GitHub Pages dashboard.
"""
import sys
from pathlib import Path

# Make the project root importable so `from src...` works from any cwd
project_root = str(Path(__file__).resolve().parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import os
import csv
import json
from datetime import datetime, timedelta

import pendulum
import pandas as pd
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from src.extract.weather import fetch_weather_for_locations
from src.extract.air_quality import fetch_air_quality_for_locations
from src.validation.quality_checks import (
    validate_weather_data,
    validate_air_quality_data,
)


@dag(
    dag_id="estonia_environmental_monitor",
    start_date=pendulum.datetime(2024, 1, 1, tz="Europe/Tallinn"),
    schedule="0 6 * * *",  # 06:00 Europe/Tallinn daily
    catchup=False,
    tags=["environmental", "estonia", "portfolio"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
)
def estonia_environmental_monitor():
    """A daily 7-day environmental forecast pipeline for 6 Estonian cities."""

    # ------------------------------------------------------------------
    # Task 1 — Load locations
    # ------------------------------------------------------------------
    @task
    def load_locations() -> list[dict]:
        with open("data/locations.csv", mode="r") as infile:
            reader = csv.DictReader(infile)
            return [
                {
                    **row,
                    "latitude": float(row["latitude"]),
                    "longitude": float(row["longitude"]),
                }
                for row in reader
            ]

    # ------------------------------------------------------------------
    # Task 2 — Extract weather forecast (today + 7 days)
    # ------------------------------------------------------------------
    @task
    def get_weather(locations: list[dict], **kwargs) -> list:
        logical_date = kwargs["logical_date"]
        print(f"[extract] Fetching 7-day weather forecast. logical_date={logical_date}")
        return fetch_weather_for_locations(locations, forecast_days=7)

    # ------------------------------------------------------------------
    # Task 3 — Extract air-quality forecast (today + 7 days)
    # ------------------------------------------------------------------
    @task
    def get_air_quality(locations: list[dict], **kwargs) -> list:
        logical_date = kwargs["logical_date"]
        print(f"[extract] Fetching 7-day AQ forecast. logical_date={logical_date}")
        return fetch_air_quality_for_locations(locations, forecast_days=7)

    # ------------------------------------------------------------------
    # Task 4 — Validate + persist to disk
    # ------------------------------------------------------------------
    @task
    def validate_and_save(
        weather_data: list, aq_data: list, locations: list[dict]
    ) -> dict:
        valid_weather = validate_weather_data(weather_data, locations)
        valid_aq = validate_air_quality_data(aq_data, locations)

        weather_path = "data/validated_weather.json"
        aq_path = "data/validated_air_quality.json"

        with open(weather_path, "w") as f:
            json.dump(valid_weather, f, indent=2)
        with open(aq_path, "w") as f:
            json.dump(valid_aq, f, indent=2)

        summary = {
            "weather_path": weather_path,
            "aq_path": aq_path,
            "weather_records": len(valid_weather),
            "aq_records": len(valid_aq),
        }
        print(f"[pipeline] Validation summary: {summary}")
        return summary

    # ------------------------------------------------------------------
    # Task 5 — Transform + load into PostgreSQL (UPSERT)
    # ------------------------------------------------------------------
    @task
    def transform_and_load(summary: dict) -> int:
        # Read validated files from disk (avoid large XComs)
        with open(summary["weather_path"]) as f:
            weather = pd.DataFrame(json.load(f))
        with open(summary["aq_path"]) as f:
            aq = pd.DataFrame(json.load(f))

        # Aggregate hourly AQ -> daily means per city
        aq["date"] = pd.to_datetime(aq["time"]).dt.date.astype(str)
        aq_daily = (
            aq.groupby(["city_id", "date"], as_index=False)
            [["pm2_5", "pm10", "nitrogen_dioxide", "ozone"]]
            .mean()
        )

        # Merge weather + AQ on (city_id, date)
        merged = weather.merge(aq_daily, on=["city_id", "date"], how="left")

        rows = merged[
            [
                "city_id",
                "latitude",
                "longitude",
                "date",
                "temperature_max",
                "temperature_min",
                "precipitation_sum",
                "wind_speed_max",
                "pm2_5",
                "pm10",
                "nitrogen_dioxide",
                "ozone",
            ]
        ].values.tolist()

        upsert_sql = """
            INSERT INTO environmental_observations
                (city_id, latitude, longitude, observation_date,
                 temperature_max, temperature_min, precipitation_sum, wind_speed_max,
                 pm2_5, pm10, nitrogen_dioxide, ozone)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (city_id, observation_date) DO UPDATE SET
                temperature_max     = EXCLUDED.temperature_max,
                temperature_min     = EXCLUDED.temperature_min,
                precipitation_sum   = EXCLUDED.precipitation_sum,
                wind_speed_max      = EXCLUDED.wind_speed_max,
                pm2_5               = EXCLUDED.pm2_5,
                pm10                = EXCLUDED.pm10,
                nitrogen_dioxide    = EXCLUDED.nitrogen_dioxide,
                ozone               = EXCLUDED.ozone,
                ingestion_timestamp = NOW();
        """

        pg = PostgresHook(postgres_conn_id="postgres_env_monitor")
        with pg.get_conn() as conn:
            with conn.cursor() as cur:
                cur.executemany(upsert_sql, rows)
            conn.commit()

        print(f"[pipeline] Upserted {len(rows)} rows into environmental_observations")
        return len(rows)

    # ------------------------------------------------------------------
    # Task 6 — Quality check (fails the pipeline on bad data)
    # ------------------------------------------------------------------
    @task
    def quality_check(rows_loaded: int) -> None:
        pg = PostgresHook(postgres_conn_id="postgres_env_monitor")

        checks = {
            "at_least_one_row_loaded": rows_loaded > 0,
            "six_cities_recently_ingested": (
                pg.get_first(
                    "SELECT COUNT(DISTINCT city_id) FROM environmental_observations "
                    "WHERE ingestion_timestamp > NOW() - INTERVAL '5 minutes';"
                )[0]
                == 6
            ),
            "no_null_temperatures": (
                pg.get_first(
                    "SELECT COUNT(*) FROM environmental_observations "
                    "WHERE temperature_max IS NULL OR temperature_min IS NULL;"
                )[0]
                == 0
            ),
        }

        failed = [name for name, ok in checks.items() if not ok]
        if failed:
            raise ValueError(f"Data quality checks failed: {failed}")

        print(f"[quality_check] All {len(checks)} checks passed")

    # ------------------------------------------------------------------
    # Task 7 — Publish public JSON for the website
    # ------------------------------------------------------------------
    @task
    def publish_data() -> None:
        pg = PostgresHook(postgres_conn_id="postgres_env_monitor")

        rows = pg.get_records(
            """
            SELECT city_id, latitude, longitude, observation_date,
                   temperature_max, temperature_min, precipitation_sum,
                   wind_speed_max, pm2_5, pm10, nitrogen_dioxide, ozone
            FROM environmental_observations
            WHERE observation_date >= CURRENT_DATE
            ORDER BY observation_date, city_id;
            """
        )

        os.makedirs("docs/data", exist_ok=True)

        payload = {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "forecast_days": 7,
            "cities": [
                dict(
                    zip(
                        [
                            "city_id",
                            "latitude",
                            "longitude",
                            "date",
                            "temperature_max",
                            "temperature_min",
                            "precipitation_sum",
                            "wind_speed_max",
                            "pm2_5",
                            "pm10",
                            "nitrogen_dioxide",
                            "ozone",
                        ],
                        [str(v) if hasattr(v, "isoformat") else v for v in row],
                    )
                )
                for row in rows
            ],
        }

        with open("docs/data/latest.json", "w") as f:
            json.dump(payload, f, indent=2, default=str)

        print(f"[publish_data] Wrote {len(rows)} rows to docs/data/latest.json")

    # ------------------------------------------------------------------
    # Wire the graph
    # ------------------------------------------------------------------
    locations = load_locations()
    weather = get_weather(locations)
    aq = get_air_quality(locations)
    summary = validate_and_save(weather, aq, locations)

    rows = transform_and_load(summary)
    quality_check(rows) >> publish_data()


estonia_environmental_monitor()
