import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.view(
    name="weather_today_view",
    comment="Gold view with only today's weather data per city."
)
def weather_today_view():
    df_weather = spark.readStream.table("silver.weather_silver")

    df_weather = df_weather.withColumn("hour_ts", to_timestamp(col("hour")))
    df_weather = df_weather.withColumn("hour_date", to_date(col("hour_ts")))

    df_weather = df_weather.filter(col("hour_date") == current_date())

    df_weather = df_weather.select(
        "city",
        "hour_ts",
        "temperature",
        "humidity",
        "wind_speed",
        "hour_date"
    ).withColumn("processDate", current_timestamp())

    return df_weather


dlt.create_streaming_table(
    name="gold.weather_today",
    comment="Weather data for the current day only."
)

dlt.create_auto_cdc_flow(
    target="gold.weather_today",
    source="weather_today_view",
    keys=["city", "hour_ts"],
    sequence_by=col("processDate"),
    stored_as_scd_type=2,
    except_column_list=["processDate"]
)
