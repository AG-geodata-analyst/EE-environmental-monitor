# Estonia Environmental Monitor

An automated data pipeline that collects weather and air-quality data
for six Estonian cities, validates it, and (soon) publishes a daily
dashboard on GitHub Pages.

## Status

- [x] Milestone 1: Local Airflow DAG extracts and validates data
- [ ] Milestone 2: Load into PostgreSQL, generate web data
- [ ] Milestone 3: Automate daily with GitHub Actions, publish to GitHub Pages

## Stack

- Apache Airflow 3.1
- Python (requests, pandas, pydantic)
- Open-Meteo APIs (weather + air quality)

## Running locally

```bash
# Activate your Airflow environment
cd ~/airflow && source airflow_venv/bin/activate

# Run the DAG
cd ~/projects/estonia-environmental-monitor
airflow dags test estonia_environmental_monitor 2024-09-22
