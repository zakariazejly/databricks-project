import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Schéma de la colonne hourly (STRING → STRUCT)
hourly_schema = StructType([
    StructField("time", ArrayType(StringType()), True),
    StructField("temperature_2m", ArrayType(DoubleType()), True),
    StructField("relative_humidity_2m", ArrayType(DoubleType()), True),
    StructField("wind_speed_10m", ArrayType(DoubleType()), True)
])

@dlt.view(
    name="weather_silver_view",
    comment="Silver view with parsed, flattened and aligned weather data per city."
)
def weather_silver_view():

    df_weather = spark.readStream.table("bronze.weather_bronze")
    df_weather = df_weather.withColumn( "city",regexp_extract(col("source_file"), r"weather_api_raw/weather_(.*)\.json", 1))
    df_weather = df_weather.withColumn("hourly_parsed",from_json(col("hourly"), hourly_schema))
    df_weather = df_weather.withColumn("hour",explode(col("hourly_parsed.time")))
    df_weather = df_weather.withColumn("temperature",expr("hourly_parsed.temperature_2m[array_position(hourly_parsed.time, hour)]"))
    df_weather = df_weather.withColumn("humidity",expr("hourly_parsed.relative_humidity_2m[array_position(hourly_parsed.time, hour)]"))
    df_weather = df_weather.withColumn("wind_speed",expr("hourly_parsed.wind_speed_10m[array_position(hourly_parsed.time, hour)]"))
    df_weather = df_weather.withColumn("processDate", current_timestamp())

    return df_weather


# Table Silver (streaming)
dlt.create_streaming_table(
    name="silver.weather_silver",
    comment="Silver table with flattened and aligned weather data per city."
)

# Auto CDC Flow (SCD Type 1)
dlt.create_auto_cdc_flow(
    target="silver.weather_silver",
    source="weather_silver_view",
    keys=["city", "hour"], 
    sequence_by=col("processDate"),
    stored_as_scd_type=1
)
