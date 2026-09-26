CREATE TABLE IF NOT EXISTS environmental_observations (
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

CREATE INDEX IF NOT EXISTS idx_env_obs_date ON environmental_observations (observation_date);
CREATE INDEX IF NOT EXISTS idx_env_obs_city ON environmental_observations (city_id);
