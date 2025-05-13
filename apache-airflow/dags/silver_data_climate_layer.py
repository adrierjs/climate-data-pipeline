import logging
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook


def get_aws_credentials():
    aws_credentials = BaseHook.get_connection("aws_default_conn_id")
    logging.info("Geting aws credentials...")
    return aws_credentials

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
            .getOrCreate()
        
    logging.info("SparkSession started successfully.")
    
    return spark

def read_and_tranform_data(spark):
    from pyspark.sql.functions import col, current_date, current_timestamp, from_utc_timestamp, to_date
    
    logging.info("Reading JSON files from S3...")

    df = spark.read.json("s3a://weather-events-raw/*/*/*")
    logging.info(f"{df.count()} records loaded from S3.")

    logging.info("Selecting and transforming columns...")
    df = df.select(
        col("location.lat").alias("latitude"),
        col("location.lon").alias("longitude"),
        col("data.time").alias("datetime_utc"),
        to_date(from_utc_timestamp(col("data.time"), "America/Sao_Paulo").alias("datetime_brasil")),
        col("data.values.cloudBase").alias("cloud_base"),
        col("data.values.cloudCeiling").alias("cloud_ceiling"),
        col("data.values.cloudCover").alias("cloud_cover"),
        col("data.values.dewPoint").alias("dew_point"),
        col("data.values.freezingRainIntensity").alias("freezing_rain_intensity"),
        col("data.values.humidity").alias("humidity"),
        col("data.values.precipitationProbability").alias("precipitation_probability"),
        col("data.values.pressureSeaLevel").alias("pressure_sea_level"),
        col("data.values.pressureSurfaceLevel").alias("pressure_surface_level"),
        col("data.values.rainIntensity").alias("rain_intensity"),
        col("data.values.sleetIntensity").alias("sleet_intensity"),
        col("data.values.snowIntensity").alias("snow_intensity"),
        col("data.values.temperature").alias("temperature"),
        col("data.values.temperatureApparent").alias("temperature_apparent"),
        col("data.values.uvHealthConcern").alias("uv_health_concern"),
        col("data.values.uvIndex").alias("uv_index"),
        col("data.values.visibility").alias("visibility"),
        col("data.values.weatherCode").alias("weather_code"),
        col("data.values.windDirection").alias("wind_direction"),
        col("data.values.windGust").alias("wind_gust"),
        col("data.values.windSpeed").alias("wind_speed")
    )

    logging.info("Adding ingestion columns...")
    df = df.withColumn("ingestion_at", current_date())
    df = df.withColumn("ingestion_at_timestamp", current_timestamp())

    logging.info("Displaying first transformed records:")
    
    return df
    

def read_s3_weather_data():
    try:
        aws_conn = get_aws_credentials()
        spark = get_spark_session(access_key=aws_conn.login, secret_key=aws_conn.password)
        read_and_tranform_data(spark)
        
    except Exception as e:
        logging.error("Error during S3 data processing.", exc_info=True)
        raise e

    finally:
        logging.info("Shutting down SparkSession.")
        spark.stop()

def write_to_postgres():
    
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
    
    write_to_postgres_task = PythonOperator(
        task_id="write_to_postgres_task",
        python_callable=write_to_postgres
    )

    read_s3_weather_data_task >> write_to_postgres_task
