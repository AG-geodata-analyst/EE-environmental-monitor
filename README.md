# Estonia Environmental Forecast Monitor

> An automated data pipeline that fetches a 7-day weather and air-quality forecast for six Estonian cities, validates it, loads it into PostgreSQL, runs data-quality checks, and publishes a live dashboard on GitHub Pages.

**🔗 Live dashboard:** https://AG-geodata-analyst.github.io/EE-environmental-monitor/

---

## 📋 Overview

This project demonstrates an **end-to-end, production-style data pipeline** built with Apache Airflow 3.1. It runs automatically every day at 06:00 UTC via GitHub Actions, produces a public dataset, and serves it as an interactive dashboard.

The goal is to showcase core Data Engineering / Data Analyst skills on a realistic, domain-relevant problem: monitoring environmental conditions across Estonia.

---

## 🏗️ Architecture

```
Open-Meteo APIs (forecast: weather + air quality)
        │
        ▼
Apache Airflow DAG: estonia_environmental_monitor
        │
        ├── load_locations
        ├── get_weather
        ├── get_air_quality
        ├── validate_and_save       (Pydantic)
        ├── transform_and_load      (merge + PostgreSQL UPSERT)
        ├── quality_check           (fail on bad data)
        └── publish_data            (writes docs/data/latest.json)
        │
        ▼
GitHub Actions (commits JSON back to repo)
        │
        ▼
GitHub Pages (Leaflet map + Chart.js dashboard)
```

---

## ✨ Features

- **Automated daily ETL** orchestrated by Apache Airflow 3.1
- **Multi-city ingestion**: 6 Estonian cities in a single API call
- **7-day forecast** of temperature, precipitation, wind speed, and 4 pollutants (PM2.5, PM10, NO₂, O₃)
- **Schema validation** with Pydantic (types + range constraints)
- **Idempotent loading** into PostgreSQL using `UPSERT` (`ON CONFLICT DO UPDATE`) — safe to re-run
- **Data quality gates**: pipeline fails if expectations are not met (6 cities present, no null temperatures, at least 1 row loaded)
- **Daily automation** via GitHub Actions (cron schedule + manual trigger)
- **Live public dashboard** on GitHub Pages with interactive map and time-series chart

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 3.1 (TaskFlow API) |
| Language | Python 3.12 |
| Data sources | Open-Meteo Forecast API, Open-Meteo Air Quality API |
| Validation | Pydantic |
| Data processing | pandas |
| Database | PostgreSQL 16 |
| Automation | GitHub Actions |
| Hosting | GitHub Pages |
| Frontend | HTML, CSS, JavaScript, Leaflet, Chart.js |

---

## 📂 Project Structure

```
EE-environmental-monitor/
├── dags/
│   └── estonia_environmental_monitor.py   # Airflow DAG (7 tasks)
├── src/
│   ├── extract/
│   │   ├── weather.py                     # Open-Meteo forecast fetcher
│   │   └── air_quality.py                 # Open-Meteo AQ fetcher
│   └── validation/
│       └── quality_checks.py              # Pydantic models + validators
├── sql/
│   └── schema.sql                         # environmental_observations table
├── data/
│   └── locations.csv                      # 6 cities with lat/lon
├── docs/                                  # GitHub Pages site
│   ├── index.html
│   ├── css/style.css
│   ├── js/app.js
│   └── data/latest.json                   # auto-updated by the pipeline
├── .github/workflows/
│   └── daily_run.yml                      # GitHub Actions workflow
└── README.md
```

---

## 🚀 Running Locally

### Prerequisites

- WSL2 (Ubuntu) on Windows
- PostgreSQL 16 running locally
- An Airflow 3.1 environment

### 1. Clone and install

```bash
git clone https://github.com/AG-geodata-analyst/EE-environmental-monitor.git
cd EE-environmental-monitor

cd ~/airflow && source airflow_venv/bin/activate
cd ~/projects/EE-environmental-monitor

pip install requests pandas pydantic psycopg2-binary apache-airflow-providers-postgres
```

### 2. Prepare PostgreSQL

```bash
sudo -u postgres psql
```

```sql
CREATE USER env_monitor WITH PASSWORD 'env_monitor_pw';
CREATE DATABASE env_monitor OWNER env_monitor;
GRANT ALL PRIVILEGES ON DATABASE env_monitor TO env_monitor;
\q
```

Apply the schema:

```bash
psql -h localhost -U env_monitor -d env_monitor -f sql/schema.sql
```

### 3. Register the Airflow connection

In the Airflow UI (**Admin → Connections → +**):

| Field | Value |
|---|---|
| Connection ID | `postgres_env_monitor` |
| Connection Type | `Postgres` |
| Host | `localhost` |
| Schema | `env_monitor` |
| Login | `env_monitor` |
| Password | `env_monitor_pw` |
| Port | `5432` |

### 4. Run the DAG

```bash
airflow dags test estonia_environmental_monitor $(date +%Y-%m-%d)
```

Expected output:

```
[pipeline] Validation summary: {'weather_records': 42, 'aq_records': 1008, ...}
[pipeline] Upserted 42 rows into environmental_observations
[quality_check] All 3 checks passed
[publish_data] Wrote 42 rows to docs/data/latest.json
```

### 5. Preview the dashboard locally

```bash
cd docs
python3 -m http.server 8000
```

Open **http://localhost:8000** in your browser (⚠️ not `file://...` — browsers block `fetch()` on `file://` URLs).

---

## 🗄️ Database Schema

```sql
CREATE TABLE environmental_observations (
    id                   SERIAL PRIMARY KEY,
    city_id              TEXT NOT NULL,
    latitude             DOUBLE PRECISION NOT NULL,
    longitude            DOUBLE PRECISION NOT NULL,
    observation_date     DATE NOT NULL,
    temperature_max      DOUBLE PRECISION,
    temperature_min      DOUBLE PRECISION,
    precipitation_sum    DOUBLE PRECISION,
    wind_speed_max       DOUBLE PRECISION,
    pm2_5                DOUBLE PRECISION,
    pm10                 DOUBLE PRECISION,
    nitrogen_dioxide     DOUBLE PRECISION,
    ozone                DOUBLE PRECISION,
    ingestion_timestamp  TIMESTAMPTZ DEFAULT NOW(),
    source               TEXT DEFAULT 'open-meteo',
    UNIQUE (city_id, observation_date)
);
```

The `UNIQUE (city_id, observation_date)` constraint enables idempotent **UPSERT** — running the DAG multiple times for the same date updates rows rather than duplicating them.

---

## 🧪 Example Queries

**Cities with the highest forecasted PM2.5 for tomorrow:**

```sql
SELECT city_id, observation_date, pm2_5, temperature_max
FROM environmental_observations
WHERE observation_date = CURRENT_DATE + INTERVAL '1 day'
ORDER BY pm2_5 DESC;
```

**7-day temperature range per city:**

```sql
SELECT city_id,
       MIN(temperature_min) AS min_temp,
       MAX(temperature_max) AS max_temp
FROM environmental_observations
WHERE observation_date >= CURRENT_DATE
GROUP BY city_id
ORDER BY city_id;
```

**Latest ingestion run:**

```sql
SELECT MAX(ingestion_timestamp), COUNT(*)
FROM environmental_observations;
```

---

## 🧠 Design Decisions

| Decision | Rationale |
|---|---|
| **Forecast API instead of Historical** | Provides today + 7-day forward-looking data — genuinely useful for a public dashboard |
| **UPSERT instead of INSERT** | Makes the pipeline idempotent — re-runs are safe and don't duplicate data |
| **XCom used only for paths** | DataFrames go through disk, not XCom (Airflow best practice — XCom is for small metadata only) |
| **Aggregate hourly AQ to daily** | Weather API returns daily, AQ API returns hourly — aggregation makes them joinable on `(city_id, date)` |
| **`logical_date` instead of wall clock** | Ensures reproducibility and correct backfill behavior |
| **Quality checks raise exceptions** | Bad data stops the pipeline — the website is never served with stale or missing data |
| **Static site on GitHub Pages** | Free, fast, zero infrastructure, integrates natively with the GitHub Actions workflow |

---

## 📅 Automation

The pipeline runs **daily at 06:00 UTC** via GitHub Actions. The workflow:

1. Spins up a temporary PostgreSQL 16 service container
2. Installs Apache Airflow 3.1 with the correct constraints file
3. Applies the schema and registers the connection
4. Runs the DAG for the current date
5. Commits the updated `docs/data/latest.json` back to the repository
6. GitHub Pages automatically redeploys the site

You can also trigger it manually from the **Actions** tab.

---

## 🗺️ Monitored Cities

| City | Latitude | Longitude |
|---|---|---|
| Tallinn | 59.4370 | 24.7536 |
| Tartu | 58.3776 | 26.7290 |
| Pärnu | 58.3859 | 24.4971 |
| Narva | 59.3772 | 28.1904 |
| Kuressaare | 58.2481 | 22.5039 |
| Võru | 57.8337 | 27.0231 |

---

## 🛣️ Roadmap

- [x] **Milestone 1** — Local Airflow DAG extracts and validates data
- [x] **Milestone 2** — Load into PostgreSQL, quality checks, generate web data
- [x] **Milestone 3** — Automate daily with GitHub Actions, publish to GitHub Pages
- [ ] **v1.2** — Add PostGIS geometry column and spatial indexes
- [ ] **v1.3** — Expand to all 15 Estonian counties
- [ ] **v1.4** — Add satellite NDVI (Sentinel-2 via Earth Engine)
- [ ] **v1.5** — Migrate transformations to dbt

---

## 📜 License

This project is licensed under the MIT License — you are free to use, modify, and distribute it with attribution.

---

## 🙋 About

**Anderson Isaac Guamán Viveros**
Background in GIScience, Earth Observation, and Environmental Modelling.
Interested in geospatial data engineering, environmental analytics, and automated data pipelines.

*Data source: Open-Meteo · Built with Apache Airflow*
