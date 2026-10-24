# Weather ETL Pipeline with Apache Airflow

An automated data engineering pipeline that extracts current weather data from the Open-Meteo API, transforms the response, and loads structured records into PostgreSQL using Apache Airflow.

## Overview

This project implements an end-to-end ETL (Extract, Transform, Load) workflow using Apache Airflow 3.x, the Astronomer CLI (Astro), Python, and PostgreSQL.

The pipeline retrieves weather observations for London, UK, validates and transforms the API response, and stores the results in a relational database. Airflow manages task dependencies, scheduling, execution, and monitoring.

## Architecture

```text
Open-Meteo API
      |
      v
Extract Weather Data
   (HTTP Hook)
      |
      v
Transform Weather Data
 (Validation & Mapping)
      |
      v
Load Weather Data
 (PostgreSQL Hook)
      |
      v
PostgreSQL Database
```

## Technology Stack

- **Python:** Data extraction, transformation, and validation.
- **Apache Airflow 3.x:** Workflow orchestration and scheduling.
- **Airflow TaskFlow API:** Decorator-based DAG and task definitions.
- **Astronomer CLI (Astro):** Local Airflow development environment.
- **Docker:** Containerized execution.
- **Open-Meteo API:** External weather data source.
- **PostgreSQL:** Persistent storage for weather observations.

## Key Features

- **Automated scheduling:** Configured for daily pipeline execution.
- **REST API integration:** Fetches current weather data through Airflow's HTTP Hook.
- **Data transformation:** Extracts and structures temperature, wind speed, wind direction, and weather codes.
- **Data validation:** Checks for required weather fields before loading.
- **Database integration:** Creates the destination table when necessary and inserts transformed records.
- **Duplicate prevention:** Uses a unique constraint and conflict handling to prevent duplicate observations for the same location and observation time.
- **Resource management:** Handles database transactions and closes database resources.
- **Workflow monitoring:** Tracks task execution and provides logs through the Airflow UI.

## ETL Workflow

### 1. Extract

The extraction task calls the Open-Meteo forecast endpoint using Airflow's `HttpHook`. It requests current weather conditions for London's coordinates and returns the JSON response.

### 2. Transform

The transformation task validates the required fields and maps the API response into a structured Python dictionary containing the location, temperature, wind speed, wind direction, weather code, and observation time.

### 3. Load

The loading task uses Airflow's `PostgresHook` to connect to PostgreSQL. It creates the destination table if needed and inserts the transformed record using parameterized SQL.

A unique constraint on location and observation time helps make repeated loads of the same observation safe.

## Data Schema

The `weather_data` table contains:

| Column | Description |
|---|---|
| `id` | Auto-generated primary key |
| `latitude` | Latitude of the observation location |
| `longitude` | Longitude of the observation location |
| `temperature` | Current temperature in °C |
| `windspeed` | Wind speed in km/h |
| `winddirection` | Wind direction in degrees |
| `weathercode` | Numerical weather condition code |
| `observation_time` | Time associated with the weather observation |
| `loaded_at` | Database insertion timestamp |

## Project Structure

```text
weather-etl-airflow/
├── dags/
│   └── etlweather.py
├── tests/
├── include/
├── plugins/
├── Dockerfile
├── requirements.txt
├── packages.txt
├── airflow_settings.yaml
├── .dockerignore
├── .gitignore
└── README.md
```

The exact files may vary depending on your Astro project configuration.

## Prerequisites

- Python version compatible with your Airflow release
- Docker Desktop
- Astronomer CLI
- Git
- An Open-Meteo API connection configured in Airflow
- A reachable PostgreSQL database

## Getting Started

### 1. Clone the repository

```bash
git https://github.com/AjitYadaav/Weather-ETL-ApacheAirFlow.git
cd Weather-ETL-Airflow
```

### 2. Configure dependencies

Ensure your `requirements.txt` includes the HTTP and PostgreSQL providers compatible with your Airflow version:

```text
apache-airflow-providers-http
apache-airflow-providers-postgres
```

Keep the Airflow version and constraints aligned with your Astro project configuration.

### 3. Start the Airflow environment

Make sure Docker Desktop is running, then execute:

```bash
astro dev start
```

Wait for the Airflow services to become healthy.

### 4. Configure Airflow connections

Open the Airflow UI at [http://localhost:8080](http://localhost:8080).

Create the following connections if they do not already exist.

**Open-Meteo HTTP connection**

| Setting | Value |
|---|---|
| Connection ID | `open_meteo_api` |
| Connection Type | HTTP |
| Host | `https://api.open-meteo.com` |

**PostgreSQL connection**

| Setting | Value |
|---|---|
| Connection ID | `postgres_default` |
| Connection Type | Postgres |
| Host | `postgres` |
| Database | `postgres` |
| Username | `postgres` |
| Password | Your configured database password |
| Port | `5432` |

The PostgreSQL hostname must resolve from the Airflow container. If PostgreSQL runs in a separate Docker environment, configure Docker networking and the hostname accordingly.

Use environment-specific credentials and never commit real passwords or secrets to GitHub.

### 5. Run the pipeline

1. Open the Airflow UI.
2. Locate `weather_etl_pipeline`.
3. Enable the DAG if necessary.
4. Trigger a manual run.
5. Check that the extraction, transformation, and loading tasks succeed.

### 6. Verify the data

Connect to PostgreSQL and execute:

```sql
SELECT *
FROM weather_data
ORDER BY observation_time DESC
LIMIT 10;
```

This query displays the most recent stored observations.

## Configuration

The default location is London, UK.

Update the following constants in `dags/etlweather.py` to use another location:

```python
LATITUDE = 51.5074
LONGITUDE = -0.1278
```

The pipeline is scheduled with `@daily`, and `catchup=False` prevents automatic creation of historical scheduled runs.

## Testing and Troubleshooting

Run the project's tests using:

```bash
astro dev pytest
```

If the DAG does not appear or a task fails:

- Check the DAG processor logs for import errors.
- Verify that the required Airflow provider packages are installed.
- Test the HTTP and PostgreSQL connections.
- Inspect task logs in the Airflow UI.
- Confirm that the database is reachable from the Airflow container.

## Future Improvements

- Add unit tests for extraction, transformation, and loading.
- Implement retry policies and failure notifications.
- Add structured logging and stronger data quality checks.
- Support multiple locations and historical weather data.
- Build a dashboard to analyze weather trends.
- Introduce automated deployment and CI/CD checks.

## Learning Outcomes

This project provides practical experience with ETL pipeline design, Apache Airflow orchestration, REST API integration, Python data transformation, PostgreSQL operations, Docker-based development, and workflow monitoring.
