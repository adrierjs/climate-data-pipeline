import logging
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def run_postgres_query(query: str):
    def _execute_query():
        hook = PostgresHook(postgres_conn_id="postgres_climate_rds")
        logging.info(f"Executing query: {query}")
        hook.run(query)
    return _execute_query


with DAG(
    "gold_data_climate_layer",
    default_args={
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Creation of the gold layer",
    schedule=timedelta(days=1),
    start_date=datetime(2025, 4, 1),
    catchup=False,
    tags=["gold", "data_climate"],
) as dag:

    avg_temperature_daily = PythonOperator(
        task_id="avg_temperature_daily",
        python_callable=run_postgres_query("""
            CREATE TABLE IF NOT EXISTS gold_events_climatics.avg_temperature_daily AS
            SELECT 
                latitude,
                longitude,
                datetime_brasil AS date,
                ROUND(AVG(temperature)::NUMERIC, 2) AS avg_temperature,
                ROUND(AVG(temperature_apparent)::NUMERIC, 2) AS avg_apparent_temperature
            FROM silver_events_climatics.evemts_climatics_torrow
            GROUP BY latitude, longitude, datetime_brasil
            ORDER BY date DESC;
        """),
    )

    precipitation_daily = PythonOperator(
        task_id="precipitation_daily",
        python_callable=run_postgres_query("""
            CREATE TABLE IF NOT EXISTS gold_events_climatics.precipitation_daily AS
            SELECT 
                latitude,
                longitude,
                datetime_brasil AS date,
                ROUND(AVG(precipitation_probability)::NUMERIC, 2) AS avg_precipitation_probability
            FROM silver_events_climatics.evemts_climatics_torrow
            GROUP BY latitude, longitude, datetime_brasil
            ORDER BY date DESC;
        """),
    )

    humidity_wind_daily = PythonOperator(
        task_id="humidity_wind_daily",
        python_callable=run_postgres_query("""
            CREATE TABLE IF NOT EXISTS gold_events_climatics.humidity_wind_daily AS
            SELECT 
                latitude,
                longitude,
                datetime_brasil AS date,
                ROUND(AVG(humidity)::NUMERIC, 2) AS avg_humidity,
                ROUND(AVG(wind_speed)::NUMERIC, 2) AS avg_wind_speed
            FROM silver_events_climatics.evemts_climatics_torrow
            GROUP BY latitude, longitude, datetime_brasil
            ORDER BY date DESC;
        """),
    )

    cloud_cover_daily = PythonOperator(
        task_id="cloud_cover_daily",
        python_callable=run_postgres_query("""
            CREATE TABLE IF NOT EXISTS gold_events_climatics.cloud_cover_daily AS
            SELECT 
                latitude,
                longitude,
                datetime_brasil AS date,
                ROUND(AVG(cloud_cover)::NUMERIC, 2) AS avg_cloud_cover,
                ROUND(MAX(cloud_base)::NUMERIC, 2) AS max_cloud_base,
                ROUND(MIN(cloud_ceiling)::NUMERIC, 2) AS min_cloud_ceiling
            FROM silver_events_climatics.evemts_climatics_torrow
            GROUP BY latitude, longitude, datetime_brasil
            ORDER BY date DESC;
        """),
    )

    weather_condition_summary = PythonOperator(
        task_id="weather_condition_summary",
        python_callable=run_postgres_query("""
            CREATE TABLE IF NOT EXISTS gold_events_climatics.weather_condition_summary AS
            SELECT 
                latitude,
                longitude,
                weather_code,
                COUNT(*) AS weather_code_count
            FROM silver_events_climatics.evemts_climatics_torrow
            GROUP BY latitude, longitude, weather_code
            ORDER BY weather_code_count DESC;
        """),
    )

    max_min_temperature_daily = PythonOperator(
        task_id="max_min_temperature_daily",
        python_callable=run_postgres_query("""
            CREATE TABLE IF NOT EXISTS gold_events_climatics.max_min_temperature_daily AS
            SELECT 
                latitude,
                longitude,
                datetime_brasil AS date,
                ROUND(MAX(temperature)::NUMERIC, 2) AS max_temperature,
                ROUND(MIN(temperature)::NUMERIC, 2) AS min_temperature
            FROM silver_events_climatics.evemts_climatics_torrow
            GROUP BY latitude, longitude, datetime_brasil
            ORDER BY date DESC;
        """),
    )

    snow_rain_condition_daily = PythonOperator(
        task_id="snow_rain_condition_daily",
        python_callable=run_postgres_query("""
            CREATE TABLE IF NOT EXISTS gold_events_climatics.snow_rain_condition_daily AS
            SELECT 
                latitude,
                longitude,
                datetime_brasil AS date,
                COUNT(CASE WHEN rain_intensity > 0 THEN 1 END) AS rain_events,
                COUNT(CASE WHEN snow_intensity > 0 THEN 1 END) AS snow_events
            FROM silver_events_climatics.evemts_climatics_torrow
            GROUP BY latitude, longitude, datetime_brasil
            ORDER BY date DESC;
        """),
    )

    [
        avg_temperature_daily,
        precipitation_daily,
        humidity_wind_daily,
        cloud_cover_daily,
        weather_condition_summary,
        max_min_temperature_daily,
        snow_rain_condition_daily
    ]
