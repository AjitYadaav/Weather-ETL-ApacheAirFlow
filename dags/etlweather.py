import pendulum
from airflow.sdk import dag, task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Configuration

LATITUDE = 51.5074
LONGITUDE = -0.1278

API_CONN_ID = "open_meteo_api"
POSTGRES_CONN_ID = "postgres_default"


# DAG Definition

@dag(
    dag_id="weather_etl_pipeline",
    description="Extract weather data from Open-Meteo and load it into PostgreSQL",
    start_date=pendulum.datetime(2026, 1, 24, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["weather", "etl", "postgres"],
)
def weather_etl_pipeline():

    # Task 1: Extract
    @task
    def extract_weather_data():
        """Fetch current weather data from the Open-Meteo API."""

        http_hook = HttpHook(
            method="GET",
            http_conn_id=API_CONN_ID,
        )

        endpoint = (
            f"/v1/forecast"
            f"?latitude={LATITUDE}"
            f"&longitude={LONGITUDE}"
            f"&current=temperature_2m,wind_speed_10m,"
            f"wind_direction_10m,weather_code"
            f"&timezone=UTC"
        )

        response = http_hook.run(endpoint=endpoint)
        response.raise_for_status()

        weather_data = response.json()

        if "current" not in weather_data:
            raise ValueError("API response does not contain current weather data.")

        return weather_data

    # Task 2: Transform
    @task
    def transform_weather_data(weather_data: dict):
        """Select and structure the required weather fields."""

        current_weather = weather_data["current"]

        required_fields = [
            "temperature_2m",
            "wind_speed_10m",
            "wind_direction_10m",
            "weather_code",
            "time",
        ]

        missing_fields = [
            field for field in required_fields
            if current_weather.get(field) is None
        ]

        if missing_fields:
            raise ValueError(
                f"Missing required weather fields: {missing_fields}"
            )

        transformed_data = {
            "latitude": weather_data["latitude"],
            "longitude": weather_data["longitude"],
            "temperature": current_weather["temperature_2m"],
            "windspeed": current_weather["wind_speed_10m"],
            "winddirection": current_weather["wind_direction_10m"],
            "weathercode": current_weather["weather_code"],
            "observation_time": current_weather["time"],
        }

        return transformed_data

    # Task 3: Load
    @task
    def load_weather_data(transformed_data: dict):
        """Load transformed weather data into PostgreSQL."""

        pg_hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        conn = pg_hook.get_conn()
        cursor = None

        try:
            cursor = conn.cursor()

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS weather_data (
                    id BIGSERIAL PRIMARY KEY,
                    latitude DOUBLE PRECISION NOT NULL,
                    longitude DOUBLE PRECISION NOT NULL,
                    temperature DOUBLE PRECISION NOT NULL,
                    windspeed DOUBLE PRECISION NOT NULL,
                    winddirection DOUBLE PRECISION NOT NULL,
                    weathercode INTEGER NOT NULL,
                    observation_time TIMESTAMP NOT NULL,
                    loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (latitude, longitude, observation_time)
                );
                """
            )

            cursor.execute(
                """
                INSERT INTO weather_data (
                    latitude,
                    longitude,
                    temperature,
                    windspeed,
                    winddirection,
                    weathercode,
                    observation_time
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (
                    latitude, longitude, observation_time
                ) DO NOTHING;
                """,
                (
                    transformed_data["latitude"],
                    transformed_data["longitude"],
                    transformed_data["temperature"],
                    transformed_data["windspeed"],
                    transformed_data["winddirection"],
                    transformed_data["weathercode"],
                    transformed_data["observation_time"],
                ),
            )

            conn.commit()
            print("Weather data loaded successfully.")

        except Exception:
            conn.rollback()
            raise

        finally:
            if cursor is not None:
                cursor.close()
            conn.close()

    # Define Task Dependencies
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)


# Register the DAG with Airflow
weather_etl_pipeline()
