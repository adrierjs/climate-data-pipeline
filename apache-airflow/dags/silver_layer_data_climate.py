from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator


def read_s3_weather_data():
    import logging
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, current_date, current_timestamp, from_utc_timestamp, to_date

    logger = logging.getLogger("airflow.task")

    try:
        logger.info("Inicializando SparkSession...")
        spark = SparkSession.builder \
            .appName("ReadS3Data") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.access.key", "AKIAWBOBRGZWZ6H7WYMS") \
            .config("spark.hadoop.fs.s3a.secret.key", "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") \
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
            .getOrCreate()
        logger.info("SparkSession iniciada com sucesso.")

        logger.info("Lendo arquivos JSON do S3...")
        df = spark.read.json("s3a://weather-events-raw/*/*/*")
        logger.info(f"{df.count()} registros carregados do S3.")

        logger.info("Selecionando e transformando colunas...")
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

        logger.info("Adicionando colunas de ingestão...")
        df = df.withColumn("ingestion_at", current_date())
        df = df.withColumn("ingestion_at_timestamp", current_timestamp())

        logger.info("Exibindo primeiros registros transformados:")
        df.show(truncate=False)

    except Exception as e:
        logger.error("Erro durante o processamento dos dados do S3.", exc_info=True)
        raise e

    finally:
        logger.info("Encerrando SparkSession.")
        spark.stop()

with DAG(
    "silver_layer_data_climate",
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
        python_callable=read_s3_weather_data,
    )

    read_s3_weather_data_task
