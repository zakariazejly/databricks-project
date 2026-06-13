import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.view(
    name="weather_gold_view",
    comment="Gold table with daily aggregated weather metrics per city."
)
def weather_gold_view():
    df_weather = spark.readStream.table("silver.weather_silver")
    df_weather = df_weather.withColumn("hour_ts", to_timestamp(col("hour")))
    df_weather = df_weather.withWatermark("hour_ts", "1 day")
    df_weather = df_weather.groupBy(
        "city",
        window("hour_ts", "1 day")
    ).agg(
        avg("temperature").alias("avg_temperature"),
        max("temperature").alias("max_temperature"),
        min("temperature").alias("min_temperature"),

        avg("humidity").alias("avg_humidity"),
        min("humidity").alias("min_humidity"),
        max("humidity").alias("max_humidity"),

        avg("wind_speed").alias("avg_wind_speed"),
        max("wind_speed").alias("max_wind_speed")
    )
    df_weather = df_weather.select(
        col("city"),
        col("window.start").cast("date").alias("date"),
        col("avg_temperature"),
        col("max_temperature"),
        col("min_temperature"),
        col("avg_humidity"),
        col("min_humidity"),
        col("max_humidity"),
        col("avg_wind_speed"),
        col("max_wind_speed")
    ).withColumn("processDate", current_timestamp())

    return df_weather

dlt.create_streaming_table(
    name="gold.weather_daily_metrics",
    comment="Daily aggregated weather metrics per city."
)

dlt.create_auto_cdc_flow(
    target="gold.weather_daily_metrics",
    source="weather_gold_view",
    keys=["city", "date"], 
    sequence_by=col("processDate"),
    stored_as_scd_type=2,
    except_column_list=['processDate']
)
