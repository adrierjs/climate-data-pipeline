import logging
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dagrun_operator import TriggerDagRunOperator


def get_aws_credentials():
    aws_credentials = BaseHook.get_connection("aws_default_conn_id")
    logging.info("Geting aws credentials...")
    return aws_credentials

def get_postgres_jdbc_info():
    """
    Pega a URL e as propriedades JDBC usando PostgresHook.
    """
    hook = PostgresHook(postgres_conn_id="postgres_climate_rds")
    conn = hook.get_connection(hook.postgres_conn_id)

    host = conn.host
    port = conn.port or 5432
    schema = conn.schema or "postgres"
    user = conn.login
    password = conn.password

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{schema}"
    properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver"
    }

    return jdbc_url, properties

def get_spark_session(access_key,secret_key):
    from pyspark.sql import SparkSession
    
    logging.info("Stating SparkSession...")
    spark = SparkSession.builder \
            .appName("ReadS3Data") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.access.key", access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
            .config("spark.jars", "/opt/spark/jars/postgresql-42.7.2.jar") \
            .getOrCreate()
            
    logging.info("SparkSession started successfully.")
    
    return spark

def read_and_tranform_data(spark, jdbc_url, properties):
    from pyspark.sql.functions import col, current_date, current_timestamp, from_utc_timestamp, to_date
    from pyspark.sql.types import DoubleType, StringType, TimestampType, DateType, IntegerType

    logging.info("Reading JSON files from S3...")

    df = spark.read.json("s3a://weather-events-raw/*/*/*")
    logging.info(f"{df.count()} records loaded from S3.")

    logging.info("Selecting and transforming columns...")
    df = df.select(
        col("location.name").cast(DoubleType()).alias("location_name"),
        col("location.lat").cast(DoubleType()).alias("latitude"),
        col("location.lon").cast(DoubleType()).alias("longitude"),
        col("data.time").cast(StringType()).alias("datetime_utc"),
        to_date(from_utc_timestamp(col("data.time"), "America/Sao_Paulo")).alias("datetime_brasil"),
        col("data.values.cloudBase").cast(DoubleType()).alias("cloud_base"),
        col("data.values.cloudCeiling").cast(DoubleType()).alias("cloud_ceiling"),
        col("data.values.cloudCover").cast(DoubleType()).alias("cloud_cover"),
        col("data.values.dewPoint").cast(DoubleType()).alias("dew_point"),
        col("data.values.freezingRainIntensity").cast(DoubleType()).alias("freezing_rain_intensity"),
        col("data.values.humidity").cast(DoubleType()).alias("humidity"),
        col("data.values.precipitationProbability").cast(DoubleType()).alias("precipitation_probability"),
        col("data.values.pressureSeaLevel").cast(DoubleType()).alias("pressure_sea_level"),
        col("data.values.pressureSurfaceLevel").cast(DoubleType()).alias("pressure_surface_level"),
        col("data.values.rainIntensity").cast(DoubleType()).alias("rain_intensity"),
        col("data.values.sleetIntensity").cast(DoubleType()).alias("sleet_intensity"),
        col("data.values.snowIntensity").cast(DoubleType()).alias("snow_intensity"),
        col("data.values.temperature").cast(DoubleType()).alias("temperature"),
        col("data.values.temperatureApparent").cast(DoubleType()).alias("temperature_apparent"),
        col("data.values.uvHealthConcern").cast(IntegerType()).alias("uv_health_concern"),
        col("data.values.uvIndex").cast(DoubleType()).alias("uv_index"),
        col("data.values.visibility").cast(DoubleType()).alias("visibility"),
        col("data.values.weatherCode").cast(IntegerType()).alias("weather_code"),
        col("data.values.windDirection").cast(DoubleType()).alias("wind_direction"),
        col("data.values.windGust").cast(DoubleType()).alias("wind_gust"),
        col("data.values.windSpeed").cast(DoubleType()).alias("wind_speed")
    )

    logging.info("Adding ingestion columns...")
    df = df.withColumn("ingestion_at", current_date().cast(DateType()))
    df = df.withColumn("ingestion_at_timestamp", current_timestamp().cast(TimestampType()))
    
    logging.info("Saving data to PostgreSQL")
    
    df.write.jdbc(url=jdbc_url, table="silver_events_climatics.events_climatics_torrow", mode="append", properties=properties)

    logging.info("Displaying first transformed records:")
    
    return df
    

def read_s3_weather_data():
    try:
        aws_conn = get_aws_credentials()
        spark = get_spark_session(access_key=aws_conn.login, secret_key=aws_conn.password)
        jdbc_url, properties = get_postgres_jdbc_info()
        read_and_tranform_data(spark, jdbc_url, properties)
        
    except Exception as e:
        logging.error("Error during S3 data processing.", exc_info=True)
        raise e

    finally:
        logging.info("Shutting down SparkSession.")
        spark.stop()

def write_to_postgres():
    pass
    
    

# Escreve os dados tratados no PostgreSQL
    
    logging.info("Writing to PostgreSQL...")
    return None

with DAG(
    "silver_data_climate_layer",
    default_args={
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Creation of the silver layer",
    schedule=timedelta(hours=1),
    start_date=datetime(2025, 4, 1),
    catchup=False,
    tags=["reader_s3", "data_climate", "silver"],
) as dag:

    read_s3_weather_data_task = PythonOperator(
        task_id="read_s3_weather_data_task",
        python_callable=read_s3_weather_data
    )
    
    data_create_gold_climate_layer = TriggerDagRunOperator(
        task_id='data_create_gold_climate_layer',
        trigger_dag_id='gold_data_climate_layer',
        conf={"mensagem": "Chamando a child_dag"},
        wait_for_completion=True, 
    )
    
    read_s3_weather_data_task >> data_create_gold_climate_layer
